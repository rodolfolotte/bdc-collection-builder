"""Describes the Celery Tasks definition of Sentinel products."""

# Python Native
from datetime import datetime
from pathlib import Path
from urllib3.exceptions import NewConnectionError, MaxRetryError
from zipfile import ZipFile
import logging
import os
import re
import time

# 3rdparty
from botocore.exceptions import EndpointConnectionError
from requests.exceptions import ConnectionError, HTTPError
from sqlalchemy.exc import InvalidRequestError

# BDC DB
from bdc_db.models import db

# Builder
from ...celery import celery_app
from ...celery.cache import lock_handler
from ...config import Config
from ...db import db_aws
from ..base_task import RadcorTask
from ..utils import extract_and_get_internal_name, refresh_assets_view, is_valid_compressed, upload_file
from .clients import sentinel_clients
from .download import download_sentinel_images, download_sentinel_from_creodias
from .harmonization import sentinel_harmonize
from .publish import publish
from .correction import correction_laSRC
from .onda import download_from_onda
from .utils import factory


lock = lock_handler.lock('sentinel_download_lock_4')


class SentinelTask(RadcorTask):
    """Define abstraction of Sentinel 2 - L1C and L2A products."""

    def get_user(self):
        """Try to get an iddle user to download images.

        Since we are downloading images from Copernicus, you can only have
        two concurrent download per account. In this way, we should handle the
        access to the stack of SciHub accounts defined in `secrets_s2.json`
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

        # Retrieve composite date of Collection Item
        return datetime.strptime(fragments[2][:8], '%Y%m%d')

    def download(self, scene, **kwargs):
        """Perform download sentinel images from copernicus.

        Args:
            scene (dict) - Scene containing activity

        Returns:
            dict Scene with sentinel file path
        """
        # Get Sentinel collection handler
        sentinel_scene = factory.get_from_sceneid(scene['sceneid'], level=1)

        # Create/update activity
        activity_history = self.create_execution(scene)

        with db.session.no_autoflush:
            activity_args = scene.get('args', dict())

            collection_item = self.get_collection_item(activity_history.activity)

            fragments = scene['sceneid'].split('_')
            year_month = fragments[2][:4] + '-' + fragments[2][4:6]

            # Output product dir
            product_dir = sentinel_scene.compressed_file().parent

            link = activity_args['link']
            scene_id = scene['sceneid']

            zip_file_name = sentinel_scene.compressed_file()

            collection_item.compressed_file = str(zip_file_name.relative_to(Config.DATA_DIR))
            cloud = activity_args.get('cloud')

            if cloud:
                collection_item.cloud_cover = cloud

            try:
                valid = True

                if zip_file_name.exists():
                    logging.debug('zip file exists')
                    valid = is_valid_compressed(str(zip_file_name))

                if not zip_file_name.exists() or not valid:
                    try:
                        # Acquire User to download
                        with self.get_user() as user:
                            logging.info('Starting Download {} - {}...'.format(scene_id, user.username))
                            # Download from Copernicus
                            download_sentinel_images(link, str(zip_file_name), user)
                    except (ConnectionError, HTTPError) as e:
                        try:
                            logging.warning('Trying to download "{}" from ONDA...'.format(scene_id))

                            download_from_onda(scene_id, os.path.dirname(str(zip_file_name)))
                        except:
                            try:
                                logging.warning('Trying download {} from CREODIAS...'.format(scene_id))
                                download_sentinel_from_creodias(scene_id, str(zip_file_name))
                            except:
                                # Ignore errors from external provider
                                raise e

                internal_folder_name = extract_and_get_internal_name(str(zip_file_name))
                extracted_file_path = os.path.join(str(product_dir), internal_folder_name)

                logging.debug('Done download.')
                activity_args['file'] = extracted_file_path

            except (HTTPError, MaxRetryError, NewConnectionError, ConnectionError) as e:
                if zip_file_name.exists():
                    os.remove(zip_file_name)

                # Retry when sentinel is offline
                logging.error('Sentinel "{}" is offline or No internet connection - {}. Retrying in {}'.format(
                    scene_id, str(e), Config.TASK_RETRY_DELAY), exc_info=True)

                raise e

            except BaseException as e:
                logging.error('An error occurred during task execution {}'.format(activity_history.activity_id),
                              exc_info=True)

                raise e

        # Persist a collection item on database
        collection_item.save()

        activity_args.pop('link')
        scene['args'] = activity_args

        # Create new activity 'correctionS2' to continue task chain
        scene['activity_type'] = 'correctionS2'

        return scene

    def correction(self, scene):
        """Apply atmospheric correction on collection.

        Args:
            scene - Serialized Activity
        """
        logging.debug('Starting Correction Sentinel...')

        # Get Resolver for Landsat scene level 2
        sentinel_scene = factory.get_from_sceneid(scene['sceneid'], level=2)

        # Set Collection to the Sentinel Surface Reflectance
        scene['collection_id'] = sentinel_scene.id
        scene['activity_type'] = 'correctionS2'

        # Create/update activity
        self.create_execution(scene)

        try:
            output_dir = sentinel_scene.path()

            # TODO: Add the sen2cor again as optional processor
            correction_result = correction_laSRC(scene['args']['file'], str(output_dir))
        except BaseException as e:
            logging.error('An error occurred during task execution - {}'.format(scene.get('sceneid')))
            raise e

        scene['args']['level'] = 2
        scene['args']['file'] = correction_result
        scene['activity_type'] = 'publishS2'

        return scene

    def publish(self, scene):
        """Publish and persist collection on database.

        Args:
            scene - Serialized Activity
        """
        scene['activity_type'] = 'publishS2'

        sentinel_scene = factory.get_from_sceneid(scene['sceneid'], level=scene['args'].get('level') or 1)

        # Create/update activity
        activity_history = self.create_execution(scene)

        logging.info('Starting publish Sentinel {} - Activity {}'.format(
            scene.get('collection_id'),
            activity_history.activity.id
        ))

        try:
            item = self.get_collection_item(activity_history.activity)
            assets = publish(item, activity_history.activity)
        except InvalidRequestError as e:
            # Error related with Transacion on AWS
            # TODO: Is it occurs on local instance?
            logging.error("Transaction Error on activity - {}".format(activity_history.activity_id), exc_info=True)

            db_aws.session.rollback()

            raise e
        except BaseException as e:
            logging.error('An error occurred during task execution - {}'.format(activity_history.activity_id), exc_info=True)
            raise e

        # Create new activity 'uploadS2' to continue task chain
        scene['activity_type'] = 'uploadS2'
        scene['args']['assets'] = assets

        if sentinel_scene.level > 1:
            refresh_assets_view()

        logging.debug('Done Publish Sentinel.')

        return scene

    def upload(self, scene):
        """Upload collection to AWS.

        Make sure to set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and
        `AWS_REGION_NAME` defined in `bdc_collection_builder.config.Config`.

        Args:
            scene - Serialized Activity
        """
        # Create/update activity
        self.create_execution(scene)

        assets = scene['args']['assets']

        for entry in assets.values():
            file_without_prefix = entry['asset'].replace('{}/'.format(Config.AWS_BUCKET_NAME), '')
            logging.warning('Uploading {} to BUCKET {} - {}'.format(entry['file'], Config.AWS_BUCKET_NAME, file_without_prefix))
            upload_file(entry['file'], Config.AWS_BUCKET_NAME, file_without_prefix)

    def harmonize(self, scene):
        """Apply Harmonization on Sentinel-2 collection.

        Args:
            scene - Serialized Activity
        """
        # Set Collection to the Sentinel NBAR (Nadir BRDF Adjusted Reflectance)
        scene['collection_id'] = 'S2NBAR'
        scene['activity_type'] = 'harmonizeS2'

        # Create/Update activity
        activity_history = self.create_execution(scene)

        logging.debug('Starting Harmonization Sentinel...')

        activity_history.activity.activity_type = 'harmonizeS2'
        activity_history.start = datetime.utcnow()
        activity_history.save()

        try:
            SAFEL2A = scene['args']['file']

            # Define new filenames for products
            parts = os.path.basename(SAFEL2A).split('_')

            # Get year month from .SAFE folder
            ymd_part = parts[2]
            y = ymd_part[:4]
            m = ymd_part[4:6]
            d = ymd_part[6:8]
            yyyymm = '{}-{}'.format(y, m)
            mgrs = parts[5]

            dir_published_L2 = str(Path(Config.DATA_DIR) / 'Repository/Archive/{}/{}/{}/'.format('S2_MSI_L2_SR_LASRC', yyyymm, os.path.basename(SAFEL2A)))

            L1_dir = str(Path(Config.DATA_DIR) / 'Repository/Archive/{}/{}/'.format('S2_MSI', yyyymm))
            zip_pattern = '.*_MSIL1C_{}{}{}.*_{}_.*.zip$'.format(y,m,d,mgrs)
            zip_file_name = [os.path.join(L1_dir,d) for d in os.listdir(L1_dir) if re.match('{}'.format(zip_pattern), d)][0]

            internal_folder_name = extract_and_get_internal_name(zip_file_name)
            SAFEL1C = os.path.join(L1_dir, internal_folder_name)

            target_dir = str(Path(Config.DATA_DIR) / 'Repository/Archive/{}/{}/{}'.format('S2_MSI', yyyymm, os.path.basename(SAFEL2A)[:-5]))
            os.makedirs(target_dir, exist_ok=True)

            harmonized_dir = sentinel_harmonize(SAFEL1C, dir_published_L2, target_dir)

        except BaseException as e:
            logging.error('Error at Harmonize Sentinel {}'.format(e))

            raise e

        scene['args']['file'] = harmonized_dir
        scene['args']['level'] = 3
        scene['activity_type'] = 'publishS2'

        return scene


# TODO: Sometimes, copernicus reject the connection even using only 2 concurrent connection
# We should set "autoretry_for" and retry_kwargs={'max_retries': 3} to retry
# task execution since it seems to be bug related to the api
@celery_app.task(base=SentinelTask,
                 queue='download',
                 max_retries=72,
                 autoretry_for=(HTTPError, MaxRetryError, NewConnectionError, ConnectionError),
                 default_retry_delay=Config.TASK_RETRY_DELAY)
def download_sentinel(scene, **kwargs):
    """Represent a celery task definition for handling Sentinel Download files.

    This celery tasks listen only for queues 'download'.

    Args:
        scene (dict): Radcor Activity

    Returns:
        Returns processed activity
    """
    return download_sentinel.download(scene, **kwargs)


@celery_app.task(base=SentinelTask, queue='atm-correction', max_retries=3, default_retry_delay=Config.TASK_RETRY_DELAY)
def atm_correction(scene, **kwargs):
    """Represent a celery task definition for handling Sentinel Atmospheric correction - sen2cor.

    This celery tasks listen only for queues 'atm-correction'.

    It only calls sen2cor for L1C products. It skips for
    sentinel L2A.

    Args:
        scene (dict): Radcor Activity with "correctionS2" app context

    Returns:
        Returns processed activity
    """
    return atm_correction.correction(scene, **kwargs)


@celery_app.task(base=SentinelTask,
                 queue='publish',
                 max_retries=3,
                 autoretry_for=(InvalidRequestError,),
                 default_retry_delay=Config.TASK_RETRY_DELAY)
def publish_sentinel(scene):
    """Represent a celery task definition for handling Sentinel Publish TIFF files generation.

    This celery tasks listen only for queues 'publish'.

    Args:
        scene (dict): Radcor Activity with "publishS2" app context

    Returns:
        Returns processed activity
    """
    return publish_sentinel.publish(scene)


@celery_app.task(base=SentinelTask,
                 queue='upload',
                 max_retries=3,
                 auto_retry=(EndpointConnectionError, NewConnectionError,),
                 default_retry_delay=Config.TASK_RETRY_DELAY)
def upload_sentinel(scene):
    """Represent a celery task definition for handling Sentinel Upload TIFF to AWS.

    This celery tasks listen only for queues 'uploadS2'.

    Args:
        scene (dict): Radcor Activity with "uploadS2" app context
    """
    upload_sentinel.upload(scene)


@celery_app.task(base=SentinelTask, queue='harmonization')
def harmonization_sentinel(scene):
    """Represent a celery task definition for harmonizing Sentinel2.
    This celery tasks listen only for queues 'harmonizeS2'.
    Args:
        scene (dict): Radcor Activity with "harmonizeS2" app context
    """
    return harmonization_sentinel.harmonize(scene)
