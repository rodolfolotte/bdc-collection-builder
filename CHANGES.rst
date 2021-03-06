..
    This file is part of Brazil Data Cube Collection Builder.
    Copyright (C) 2019-2020 INPE.

    Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


=======
Changes
=======

Version 0.2.0
-------------

Released 2020-04-29

- First experimental version.
- Metadata ingestion of Image Collections.
- Data download from open data catalogs:
  - `Copernicus <https://scihub.copernicus.eu/>`_.
  - `CREODIAS <https://creodias.eu/>`_.
  - `EarthExplorer <https://earthexplorer.usgs.gov/>`_.
- Processors for image collections based on:
  - `Sen2Cor <https://step.esa.int/main/third-party-plugins-2/sen2cor/>`_: processor for the generation of Sentinel-2 Level 2A product.
  - `LaSRC <https://github.com/USGS-EROS/espa-surface-reflectance>`_: processor for the generation of Landsat 8 surface reflectance data products.
- Multi-container applications based on ``Docker`` and ``docker-compose``.
- Documentation system based on ``Sphinx``.
- Documentation integrated to ``Read the Docs``.
- Package support through ``Setuptools``.
- Installation and deploy instructions.
- Schema versioning through ``Flask-Migrate``.
- Source code versioning based on `Semantic Versioning 2.0.0 <https://semver.org/>`_.
- License: `MIT <https://github.com/brazil-data-cube/bdc-collection-builder/blob/v0.2.0/LICENSE>`_.
