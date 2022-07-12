.. _configuration:

Configuration
=============

The WOUDC Data Registry is configured mostly using environment variables.
Operators will be provided with environment files which set these variables
appropriately. Executing one of these environment files is required before
running the WOUDC Data Registry.

Some of the environment variables are paths to more specific configuration
files. All these paths are interpreted on the machine where the Data Registry
is running, and most of the files are included with the WOUDC Data Registry
project in the **/data** and **/data/migrate** folders.

Any configuration options can be changed directly in the environment after
running an environment file.

WDR_LOGGING_LOGLEVEL
    Minimum severity of log messages written to the log file.

WDR_LOGGING_LOGFILE
    Full path to log file location.

WDR_DB_DEBUG
    Whether to include log messages from the database in logging output.

WDR_DB_TYPE
    DBMS used to host the Data Registry (e.g. sqlite, postgresql).

WDR_DB_HOST
    HTTP URL to Data Registry DB host, or filepath to Sqlite DB file if applicable.

WDR_DB_PORT
    Port number for Data Registry DB host, if applicable.

WDR_DB_NAME
    Name of Data Registry schema within the DB.

WDR_DB_USERNAME
    Username for Data Registry DB user account.

WDR_DB_PASSWORD
    Password for Data Registry DB user account.

WDR_SEARCH_TYPE
    DBMS used to host the Search Index (e.g. elasticsearch)

WDR_SEARCH_URL
    HTTP URL to Search Index host.

WDR_WAF_BASEDIR
    Path to WAF files system location on the host machine.

WDR_WAF_BASEURL
    HTTP URL to WAF location on the web.

WDR_ERROR_CONFIG
    Path to error definition file on the host machine.

    The file defines error types and messages and their severity. All entries
    listed as type Error in this file cause the WOUDC Data Registry to stop
    processing an input. Entries of type Warning may be recovered from,
    and the Data Registry may be able to process a file regardless of any
    Warnings it receives.

    Warnings and Errors are logged in the Operator Report as part of the
    WOUDC Data Registry's core workflow.

WDR_ALIAS_CONFIG
    Path to alias configuration file on the host machine.

    The file defines alternate spellings for certain fields in input files.
    If encountered, any of these alternate spellings are substituted for one
    standard spelling, unless this substitution is marked as type Error in
    the error definitions file.

To display all configurations from woudc-data-registry, run `woudc-data-registry admin config`
