.. _operator-workflows:

Operator Workflows
==================

Every Monday the WOUDC Data Registry is updated with new files submitted over
the past week from various contributors. Files are either successfully ingested
immediately, have recoverable errors that are fixed manually, or have errors
that are irrecoverable and fail to process. Contributors are alerted to their
failing files and given feedback on their errors, and may attempt to resubmit
the files later.

---------
Gathering
---------

TODO

----------
Processing
----------

In the processing stage, incoming files go through a series of validation
checks and are assessed for formatting and metadata errors. Files that pass
are chunked and stored in the Data Registry, the DEV Search Engine, and the
WAF and become publicly visible.

The operator's role is to help assess and respond to errors that the
WOUDC Data Registry program can't deal with, such as correcting formatting or
correcting bad metadata, until as many of the incoming files can be persisted
as possible.

Setup:
    * Start a new screen session using `screen -S processing-<currentdate>`
    * Switch to (or create) the WOUDC Data Registry master virtual environment
    * Create a working directory at `/apps/data/incoming-<currentdate>-run`
    * Run the master environment file to set environment variables
      and configurations

Processing:
    * Attempt to process all incoming files from the week:
    * `woudc_data_registry data ingest /apps/data/incoming/<currentdate> -w /apps/data/incoming-<currentdate>-run`
    * Watch for the occasional prompt as a new instrument, station name, or
      contributor deployment is found.
    * Some files will fail to be processed because of a recoverable or
      irrecoverable error. Recoverable errors can be fixed between runs
      and reprocessed, but files with irrecoverable errors need not be
      processed again until the contributor resubmits them later.
    * To reprocess a selection of failed files:
    * Copy the selection of files to a new directory, named `failing_files`,
      within the working directory and run:
    * `woudc_data_registry data ingest /apps/data/incoming-<currentdate>-run/failing_files`
    * And then continue to reprocess failures from these runs using the
      second command.
    * Once a file has been processed successfully, do not move it to a new
      location!

-------------
Notifications
-------------

    * Create a file named `/apps/data/incoming-<currentdate>-run/failed-files-<currentdate>`.
      This file can be created and edited while processing is going on.
    * The file contains one block for each contributor that submitted data
      in the previous week, and begins with the contributor's acronym in all
      uppercase, a space, and a semicolon-separated email list between
      parentheses. Following the header is a summary block
      `as shown here <https://gccode.ssc-spc.gc.ca/woudc/woudc-bps/blob/master/etc/failed_files_email_template.txt#L13>`_.
    * Fill in the file with pass/fix/fail counts for each contributor.
      Also, in each contributor block, document error messages and the
      files they affected in Summary of Fixes and Summary of Failures blocks.
    * Contributor email can be fetched using a SQL query to the Data Registry
      like this one:
    * `SELECT acronym, email FROM contributors JOIN deployments USING (contributor_id) WHERE station_id = '...'`
    * Where ... is replaced with the #PLATFORM.ID from any of the input files
      from that contributor.
    * #PLATFORM.ID is used in the query because most files get it right, while
      some files have the wrong agency name. If the #PLATFORM.ID seems wrong,
      feel free to query the contributor using a different field.
      than other fields.
    * Another operator will use this email report as the basis for a feedback
      email message to each contributor.

----------
Publishing
----------

TODO
