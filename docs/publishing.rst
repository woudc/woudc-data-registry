.. _publishing:

Publishing
==========

Currently, processing a file updates the Search Index with its metadata
as long as the file passes validation. This way the Search Index and
Data Registry should stay synchronized as long as no manual changes are made.

In case the Data Registry and Search Index become desynchronized, there is a
command to resync them. This command is destructive and cannot be undone,
so use with caution and only when the Data Registry's content is trusted.

woudc_data_registry admin search sync
    Synchronize the Data Registry and Search Index exactly, using the Data
    Registry as a template. First inserts or update records in the Index to
    match the row with the same ID in the Registry, and then deletes excess
    documents in the Index with no ID match in the Registry.

In the future, whether to add elements to the Search Index while processing
may turn into a configuration option.
