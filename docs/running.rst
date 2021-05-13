.. _running:

Running
=======

---------------
Model Interface
---------------

The model interface allows for direct access to the Data Registry's contents
from the command line for certain types of metadata records.

Operations include viewing records, adding or deleting records, and updating
existing records.

Main use cases for the interface are to add missing metadata records or new
records during a processing run, or to fix small numbers of out-of-date
or damaged records between processing runs.

woudc_data_registry project list
    List all overarching projects in the Data Registry.

woudc_data_registry dataset list
    List all datasets received by the Data Registry.

woudc_data_registry station list
    List all stations that participate in the WOUDC project.

woudc_data_registry contributor list
    List all contributors to the WOUDC project.

woudc_data_registry instrument list
    List all instruments that have recorded data for WOUDC submissions.

woudc_data_registry deployment list
    List all contributor deployments that have submitted to the WOUDC project.

woudc_data_registry station add <options>
    Add a station record to the Data Registry, with metadata as specified by
    options:

    | -id IDENTIFIER      WOUDC station identifier.
    | -gi GAW_ID          GAW station identifier.
    | -n NAME             Name of the station.
    | -t TYPE             Type of the station or ship.
    | -g GEOMETRY         Latitude, longitude, and elevation of the station.
    | -c COUNTRY          Country where the station is located.
    | -w WMO_REGION_ID    WMO identifier for station's continent/region.
    | -sd START_DATE      Date when the station became active.
    | -ed END_DATE        Date when the station became inactive.

woudc_data_registry contributor add <options>
    Add a WOUDC contributing agency to the Data Registry, with metadata
    as specified by options:

    | -n (name) Full name of the contributor
    | -a (acronym) Abbreviated name of the contributor
    | -p (project) Overarching project that the contributor is a member of
    | -g (geometry) Latitude and longitude for the contributor's headquarters
    | -c (country) Contributor's country of origin or country of operation
    | -w (wmo_region) WMO identifier for contributor's home continent/region
    | -u (url) HTTP URL to the contributor's home webpage
    | -e (email) Address for central contact to the contributing agency
    | -f (ftp_username) Username for the contributor in the WOUDC FTP

woudc_data_registry instrument add <options>
    Add an instrument to the Data Registry with metadata as specified by
    options:

    | -n (name) Name of the producing company or source of the instrument
    | -m (model) Instrument model name
    | -s (serial) Instrument serial number
    | -d (dataset) Type of data the instrument is able to record
    | -st (station) Station from where the instrument is operated
    | -g (geometry) Latitude and longitude where the instrument is located

woudc_data_registry deployment add <options>
    Add a contributor deployment to the Data Registry with metadata as
    specified by options:

    | -c (contributor) ID of the contributor in the Data Registry
    | -s (station) Station WOUDC ID where the contributor is operating
    | -sd (start_date) Date when the deployment started
    | -ed (end_date) Date when the deployment ended

woudc_data_registry station update -id <identifier> <options>
    Modify an existing station record in the Data Registry with the ID
    <identifier> as specified by options:

    | -gi (gaw_id) GAW station identifier
    | -n (name) Name of the station
    | -t (type) Type of the station or ship
    | -g (geometry) Latitude, longitude, and elevation of the station
    | -c (country) Country where the station is located
    | -w (wmo_region) WMO identifier for station's continent/region
    | -sd (start_date) Date when the station became active
    | -ed (end_date) Date when the station became inactive

woudc_data_registry contributor update -id <identifier> <options>
    Modify an existing contributor record in the Data Registry with the ID
    <identifier>, and possibly change it to a new ID, as specified by options:

    | -n (name) Full name of the contributor
    | -a (acronym) Abbreviated name of the contributor
    | -p (project) Overarching project that the contributor is a member of
    | -g (geometry) Latitude and longitude for the contributor's headquarters
    | -c (country) Contributor's country of origin or country of operation
    | -w (wmo_region) WMO identifier for contributor's home continent/region
    | -u (url) HTTP URL to the contributor's home webpage
    | -e (email) Address for central contact to the contributing agency
    | -f (ftp_username) Username for the contributor in the WOUDC FTP

woudc_data_registry instrument update -id <identifier> <options>
    Modify an existing instrument record in the Data Registry with the ID
    <identifier>, and possibly change it to a new ID, as specified by options:

    | -n (name) Name of the producing company or source of the instrument
    | -m (model) Instrument model name
    | -s (serial) Instrument serial number
    | -d (dataset) Type of data the instrument is able to record
    | -st (station) Station from where the instrument is operated
    | -g (geometry) Latitude and longitude where the instrument is located

woudc_data_registry deployment update -id <identifier> <options>
    Modify an existing contributor deployment record in the Data Registry
    with the ID <identifier>, and possibly change it to a new ID, as
    specified by options:

    | -c (contributor) ID of the contributor in the Data Registry
    | -s (station) Station WOUDC ID where the contributor is operating
    | -sd (start_date) Date when the deployment started
    | -ed (end_date) Date when the deployment ended

woudc_data_registry station|contributor|instrument|deployment show <id>
    Display all information in the Data Registry about the record which has
    the identifier <id> under the specified metadata type (station,
    contributor, instrument, or deployment)

woudc_data_registry station|contributor|instrument|deployment delete <id>
    Delete the record with identifier <id> from the Data Registry, under the
    specified metadata type (station, contributor, instrument, or deployment).

---------
Ingestion
---------

The primary workflow involving the WOUDC Data Registry is ingestion, or bulk
processing, of input files. Ingest commands sequentially parse, validate,
repair, break down and upload contents of these files to the Data Registry
as well as the Search Index. A copy of the incoming file is sent to the WAF.

woudc_data_registry data ingest <input_source> <flags>
    Ingest the incoming data at <input_source>, which is either a path to a
    single input file or to a directory structure containing them. Output
    log files and reports are placed in <working_dir>. <flags> are
    as follows:

    | -y (yes) Automatically accept all permission checks
    | -l (lax) Only validate core metadata tables, and not dataset-specific
               metadata or data tables. Useful when data is presented in
               old formats or is formatted improperly and cannot be
               repaired but must be ingested anyways, such as
               during backfilling.

------------
Verification
------------

A secondary workflow in the WOUDC project is input file verification or
error-checking. As a mock ingestion, the same logging output is released as in
ingestion (including to the console) but no changes are made to the
Data Registry or Search Engine backends.

This workflow finds whether files are properly formatted, which can inform
contributors whether their file generation processes and their metadata are
correct. WOUDC Data Registry developers may also use the verification command
to test ingestion routines on dummy input files without inserting dummy data
into the backends.

woudc_data_registry data verify <input_source> <flags>
    Verify the incoming data at <input_source>, which is either a path to a
    single input file or to a directory structure containing them.
    <flags> are as follows:

    | -y (yes) Automatically accept all permission checks
    | -l (lax) Only validate core metadata tables, and not dataset-specific
               metadata or data tables. Useful when only core tables and
               metadata are important or when dataset-specific tables are
               known to contain errors but nothing can be done about them,
               such as during backfilling.

-------------------
UV Index generation
-------------------

An hourly UV Index can be generated using data and metadata from WOUDC Extended
CSV data. In particular, files from the Broadband and Spectral datasets are used
in this process. The UV Index can be generated from a single process to build the
entire index.

woudc-data-registry product uv-index generate /path/to/archive/root 
    Delete all records from the uv_index_hourly table and use all Spectral 
    and Broadband files to generate uv_index_hourly records. 
