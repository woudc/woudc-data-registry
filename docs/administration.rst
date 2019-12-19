.. _administration:

Administration
==============

In addition to the project code itself, WOUDC Data Registry depends on several
databases which must be set up before the project can run.

The core backend, referred to as the Data Registry, is an archive of file
metadata and links using a relational database, currently PostgreSQL. The
Data Registry is the trusted and long-term source of file metadata, history
and versioning, and file location.

The secondary backend, known as the Search Index, is derived from the Data
Registry and is currently kept in ElasticSearch. The Search Index is
denormalized and will contain additional metrics data for quick access.

The whole file contents, as opposed to metadata only, are stored in the WOUDC
`Web Accessible Folder <https://woudc.org/archive/Archive-NewFormat/>`_
(WAF) and are linked via URLs in both the Data Registry and Search Index.

The Search Index is intended for use by web applications and for searchability,
while the Data Registry is intended for internal operations and to prop and
compare against the Search Index in case of error.

-----------------
Creating Backends
-----------------

make ENV=/path/to/environment/config.env createdb
    Create the Data Registry database instance

woudc_data_registry admin setup
    Create Data Registry tables and schema

woudc_data_registry admin search create-indexes
    Create Search Index mappings

-----------------
Deleting Backends
-----------------

woudc_data_registry admin teardown
    Delete all Data Registry tables and schema

woudc_data_registry admin search delete-indexes
    Delete all Search Index indexes and mappings

make ENV=/path/to/environment/config.env dropdb
    Delete the Data Registry database instance

-------------------
Populating Backends
-------------------

When necessary, the entire history of data records can be recreated by
processing all files in the WAF (backfilling). Not all metadata
can be extracted from these files, and so there are alternate methods to
recreate metadata from scratch.

If the Data Registry is empty, metadata is created using the old WOUDC
processing archive. The WOUDC Data Registry code comes with three files
in the data/init directory:
 * ships.csv
 * countries.json
 * fetch-content.sh

The .csv and .json files contain metadata which is not stored in the archive.
fetch-content.sh is a program that generates more csv metadata files using
the old WOUDC archive.

fetch-content.sh
    Creates several CSV files containing core metadata for the WOUDC Data
    Registry, using old WOUDC archives. Places the official WMO country
    list and the CSV files in a specified output directory.

    | -h (host)     Hostname of old WOUDC archive database
    |               (or filepath for SQLite)
    | -p (port)     Port number of old WOUDC archive database
    | -d (database) Schema name of old WOUDC archive database
    | -u (user)     Database account username
    | -o (output)   Output directory for generated files

After running fetch-content.sh, the files in data/init are ready to be
inserted to the Data Registry.

woudc_data_registry admin init -d <initialization> <flags>
    Searches the directory path <initialization> for .csv and .json files
    of metadata, and loads them into the Data Registry tables. If the
    --init-search-index flag is provided, loads them to the Search Index
    as well.

If the Data Registry is filled but not the Search Engine, the latter can be
populated by using the sync command (see :doc:`publishing`).
