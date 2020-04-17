.. _development:

Development
===========

-------------------------
Dataset Table Definitions
-------------------------

Tables in Extended CSV files must conform to a certain format, and certain
tables are required for each dataset. The specifications for table formatting
are defined in table definition files, two of which are provided with the
WOUDC Data Registry codebase.

**data/tables.yml** is a table definition file for production use when processing
incoming files. **data/migrate/tables-backfilling.yml** is an alternative table
definition file for backfilling the registry with historical files. The second
file is looser on required tables and fields to allow flexibility with older
WOUDC formats.

~~~~~~~~~~~~~~~~~~~~~~~
Table Definition Format
~~~~~~~~~~~~~~~~~~~~~~~

Table definition files are made up of organized sections of table definitions.
Each table definition is a dictionary-type element (in a JSON or YAML file or
equivalent) which defines the shape and expected fields of a table.

A table definition is formatted like this (example in YAML)::

  table_name:
      rows: <range of rows>
      occurrences: <range of occurrences>
      ~required_fields:
          - list
          - of
          - required
          - fields
      ~optional_fields:
          - list
          - of
          - optional
          - fields

Optional keys are prefixed with the **~** character.

In a table definition, a range of integers **a** < **b** is specified by a string
with one of the following forms::

  b      The discrete number b.
  a-b    The range of integers between a and b (inclusive).
  b+     The range of integers with no upper bound starting at b.

The **rows** key defines the allowable height range for the table, i.e. the
number of rows the table must have.

The **occurrences** key defines the allowable range of number of times the
table appears in the file. If 0 is included in the range then the table
as a whole is optional and may be left out of a file.

The (optional) **required_fields** key defines a list of field names that must
appear in the table. If any of these fields is missing from the file, the
table will be considered invalid. All tables must have at least one required
field.

The (optional) **optional_fields** key defines a list of field names that may
appear in the table but are not required.


~~~~~~~~~~~~~~~~~~~~~~~~~
Dataset Definition Format
~~~~~~~~~~~~~~~~~~~~~~~~~

The set of table definitions varies based on which type of data the file
contains. Table definition files express this by organizing table definitions
into sections.

Some tables are common to all Extended CSV files. The **Common** key at the top of
a table definition files contains the definitions for all these shared tables.

Besides the common tables, the table definitions that apply to an Extended CSV
file depend on certain fields within the file.

The table definitions apply depend on the file's dataset (the type of data
contained, controlled by the **#CONTENT.Category** value), its level (of QA,
controlled by the **#CONTENT.Level** value), and its form (controlled by its
**#CONTENT.Form** value). In some cases there are multiple allowable table
definitions for the same dataset, level, and form, in which case each option
is assigned an integer version number starting from 1.

A dataset definition is formatted like this (example in YAML)::

  dataset_name:
    "level":
      "form":
        table definition 1
        table definition 2
        ...
        data_table: <name of data table>
      ..
    ..

OR::

  dataset_name:
    "level":
      "form":
        "version":
          table definition 1
          table definition 2
          ...
          data_table: <name of data table>
        ..
      ..
    ..

The numeric level, form, and version keys all must be surrounded by double
quotes to force them to be strings. There may be multiple level keys in a
dataset, multiple forms within a level, and multiple versions within a form.

In the innermost block is a series of table names mapped to table definitions.
The one additional key, data_table, maps to the name of the table containing
observational data. This must correspond to a required table amongst the
table definitions in that block.


~~~~~~~~~~~~~~~~~~~~~
Asserting Correctness
~~~~~~~~~~~~~~~~~~~~~

The WOUDC Data Registry process validates all table definition files against a
schema before using them. The schema is written using JSON schema language and
is stored at **data/table-schema.json**.

WOUDC developers adding to a table definitions file can use JSON schema
validation tools to check that their additions are in the right format.
