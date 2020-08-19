"""Describes the Celery Tasks definition of Sentinel products."""

from datetime import datetime
from pathlib import Path
from urllib3.exceptions import NewConnectionError, MaxRetryError
from zipfile import ZipFile
import logging
import os
import re
import time

from botocore.exceptions import EndpointConnectionError
from requests.exceptions import ConnectionError, HTTPError
from sqlalchemy.exc import InvalidRequestError

from bdc_db.models import db

from ...celery import celery_app
from ...celery.cache import lock_handler
from ...config import Config
from ...db import db_aws
from ..base_task import RadcorTask
from ..utils import extract_and_get_internal_name, refresh_assets_view, is_valid_compressed, upload_file
from .clients import sentinel_clients
from .download import download_sentinel_images, download_sentinel_from_aws
from .publish import publish
from .correction import correction_sen2cor255, correction_sen2cor280
# from .onda import download_from_onda


lock = lock_handler.lock('sentinel_download_lock_4')


class SentinelTask(RadcorTask):
    """Define abstraction of Sentinel 1 - S1A and S1B products."""

    def get_user(self):
        """Try to get an iddle user to download images.

        Since we are downloading images from Copernicus, you can only have
        two concurrent download per account. In this way, we should handle the
        access to the stack of SciHub accounts defined in `secrets.json`
        in order to avoid download interrupt.

        Returns:
            AtomicUser An atomic user
        """
        user = None

        while lock.locked():
            logging.info('Resource locked....')
            time.sleep(1)

        lock.acquire(blocking=True)
        while user is None:
            user = sentinel_clients.use()

            if user is None:
                logging.info('Waiting for available user to download...')
                time.sleep(5)

        lock.release()

        return user

    def get_tile_id(self, scene_id, **kwargs):
        """Retrieve tile from sceneid."""
        fragments = scene_id.split('_')
        return fragments[-2][1:]

    def get_tile_date(self, scene_id, **kwargs):
        """Retrieve tile date from sceneid."""
        fragments = scene_id.split('_')
        return datetime.strptime(fragments[2][:8], '%Y%m%d')

    def download(self, scene):
        """Perform download sentinel images from copernicus.

        Args:
            scene (dict) - Scene containing activity

        Returns:
            dict Scene with sentinel file path
        """
        scene['collection_id'] = 'S1_IW_GRD'

        activity_history = self.create_execution(scene)

        with db.session.no_autoflush:
            activity_args = scene.get('args', dict())

            collection_item = self.get_collection_item(activity_history.activity)
            fragments = scene['sceneid'].split('_')
            year_month = fragments[2][:4] + '-' + fragments[2][4:6]
            product_dir = os.path.join(activity_args.get('file'), year_month)
            link = activity_args['link']
            scene_id = scene['sceneid']

            zip_file_name = os.path.join(product_dir, '{}.zip'.format(scene_id))
            collection_item.compressed_file = zip_file_name.replace(Config.DATA_DIR, '')

            try:
                valid = True

                if os.path.exists(zip_file_name):
                    logging.debug('zip file exists')
                    valid = is_valid_compressed(zip_file_name)

                if not os.path.exists(zip_file_name) or not valid:
                    try:
                        with self.get_user() as user:
                            logging.info('Starting Download {} - {}...'.format(scene_id, user.username))
                            download_sentinel_images(link, zip_file_name, user)

                    except (ConnectionError, HTTPError) as e:
                        raise e
                        # try:
                        #     logging.warning('Trying to download "{}" from ONDA...'.format(scene_id))
                        #
                        #     download_from_onda(scene_id, os.path.dirname(zip_file_name))
                        # except:
                        #     try:
                        #         logging.warning('Trying to download {} from CREODIAS...'.format(scene_id))
                        #         download_sentinel_from_creodias(scene_id, zip_file_name)
                        #     except:
                        #         raise e

                internal_folder_name = extract_and_get_internal_name(zip_file_name)
                extracted_file_path = os.path.join(product_dir, internal_folder_name)

                logging.debug('Download done!')
                activity_args['file'] = extracted_file_path

            except (HTTPError, MaxRetryError, NewConnectionError, ConnectionError) as e:
                if os.path.exists(zip_file_name):
                    os.remove(zip_file_name)

                logging.error('Sentinel "{}" is offline or no internet connection - {}. '
                              'Retrying in {}'.format(scene_id, str(e), Config.TASK_RETRY_DELAY), exc_info=True)
                raise e

            except BaseException as e:
                logging.error('An error occurred during task execution {}'.format(activity_history.activity_id),
                              exc_info=True)

                raise e

        collection_item.save()

        activity_args.pop('link')
        scene['args'] = activity_args
        scene['activity_type'] = 'correctionS1'

        return scene

    # TODO: inserir rotinas de correção
    def correction(self, scene):
        """Apply atmospheric correction on collection.

        Args:
            scene - Serialized Activity
        """
        logging.debug('Starting Correction Sentinel-1...')

        version = 'sen1'
        scene['collection_id'] = 'S1_IW_GRD_COR'
        scene['activity_type'] = 'correctionS1'

        self.create_execution(scene)

        try:
            params = dict(
                app=scene['activity_type'],
                sceneid=scene['sceneid'],
                file=scene['args']['file']
            )

            if version == 'sen1':
                correction_result = correction_sen2cor280(params)
            else:
                correction_result = correction_sen2cor255(params)
            if correction_result is not None:
                scene['args']['file'] = correction_result

        except BaseException as e:
            logging.error('An error occurred during task execution - {}'.format(scene.get('sceneid')))
            raise e

        scene['activity_type'] = 'publishS1'

        return scene

    def publish(self, scene):
        """Publish and persist collection on database.

        Args:
            scene - Serialized Activity
        """
        scene['activity_type'] = 'publishS1'

        activity_history = self.create_execution(scene)

        logging.info('Starting publish Sentinel-1 {} - Activity {}'.format(scene.get('collection_id'),
                                                                           activity_history.activity.id))

        try:
            assets = publish(self.get_collection_item(activity_history.activity), activity_history.activity)
        except InvalidRequestError as e:
            # TODO: Is it occurs on local instance?
            logging.error("Transaction Error on activity - {}".format(activity_history.activity_id), exc_info=True)
            db_aws.session.rollback()
            raise e
        except BaseException as e:
            logging.error('An error occurred during task execution - {}'.format(activity_history.activity_id), exc_info=True)
            raise e

        scene['activity_type'] = 'uploadS1'
        scene['args']['assets'] = assets

        if scene.get('collection_id') == 'S1_IW_GRD_COR':
            refresh_assets_view()

        logging.debug('Done Publish Sentinel.')

        return scene

    # TODO: modificar com endereços do Bucket responsável pela armazenagem do S1
    def upload(self, scene):
        """Upload collection to AWS.

        Make sure to set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and
        `AWS_REGION_NAME` defined in `bdc_collection_builder.config.Config`.

        Args:
            scene - Serialized Activity
        """
        self.create_execution(scene)

        assets = scene['args']['assets']

        for entry in assets.values():
            file_without_prefix = entry['asset'].replace('{}/'.format(Config.AWS_BUCKET_NAME), '')
            logging.warning('Uploading {} to BUCKET {} - {}'.format(entry['file'], Config.AWS_BUCKET_NAME, file_without_prefix))
            upload_file(entry['file'], Config.AWS_BUCKET_NAME, file_without_prefix)


# TODO: Sometimes, copernicus reject the connection even using only 2 concurrent connection
# We should set "autoretry_for" and retry_kwargs={'max_retries': 3} to retry
# task execution since it seems to be bug related to the api
@celery_app.task(base=SentinelTask,
                 queue='download',
                 max_retries=72,
                 autoretry_for=(HTTPError, MaxRetryError, NewConnectionError, ConnectionError),
                 default_retry_delay=Config.TASK_RETRY_DELAY)
def download_sentinel(scene):
    """Represent a celery task definition for handling Sentinel Download files.

    This celery tasks listen only for queues 'download'.

    Args:
        scene (dict): Radcor Activity

    Returns:
        Returns processed activity
    """
    return download_sentinel.download(scene)


@celery_app.task(base=SentinelTask,
                 queue='publish',
                 max_retries=3,
                 autoretry_for=(InvalidRequestError,),
                 default_retry_delay=Config.TASK_RETRY_DELAY)
def publish_sentinel(scene):
    """Represent a celery task definition for handling Sentinel Publish TIFF files generation.

    This celery tasks listen only for queues 'publish'.

    Args:
        scene (dict): Radcor Activity with "publishS1" app context

    Returns:
        Returns processed activity
    """
    return publish_sentinel.publish(scene)


@celery_app.task(base=SentinelTask,
                 queue='upload',
                 max_retries=3,
                 auto_retry=(EndpointConnectionError, NewConnectionError),
                 default_retry_delay=Config.TASK_RETRY_DELAY)
def upload_sentinel(scene):
    """Represent a celery task definition for handling Sentinel Upload TIFF to AWS.

    This celery tasks listen only for queues 'uploadS1'.

    Args:
        scene (dict): Radcor Activity with "uploadS2" app context
    """
    upload_sentinel.upload(scene)
