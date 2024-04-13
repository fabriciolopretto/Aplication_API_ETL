.. Aplicacion_ETL documentation master file, created by
   sphinx-quickstart on Sat Apr 13 10:42:45 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Bienvenidos a la documentación de la Aplicación de Aviation Weather ETL!
========================================================================
Realiza la extracción de datos horarios de Temperatura de la estación
meteorológica del aeródromo de Ezeiza, para luego refinarlos y persistirlos
en un *Data Lake* en formato parquet y en un *Data Werehouse* en Aiven con
un motor de base de datos Postgres.


.. toctree::
   :maxdepth: 4
   :caption: Contents:

   extraction
   main
   prosecution
   storage


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
