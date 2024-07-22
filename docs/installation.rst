.. _installation:

Installation
============

The WOUDC Data Registry is available on GitHub.

------------
Requirements
------------

* `Python <https://python.org>`_ 3 and above
* `virtualenv <https://virtualenv.pypa.io>`_
* `ElasticSearch <https://www.elastic.co/products/elasticsearch>`_
  5.5.0 and above

Other requirements are as listed in requirements.txt, and are installed
during project installation.

------------
Instructions
------------

* Create and activate a virtual environment::
     | virtualenv woudc-data-registry
     | cd woudc-data-registry
     | source bin/activate

* Install the project::
     | git clone https://github.com/woudc/woudc-data-registry.git
     | cd woudc-data-registry
     |
     | pip install -r requirements.txt     # Core dependencies
     | pip install -r requirements-pg.txt  # For PostgreSQL backends
     |
     | python3 setup.py build
     | python3 setup.py install

* Set up the project::
     | . /path/to/environment/config.env  # Set environment variables
     |
     | # Continue by creating databases as per instructions in
         :doc:`administration`

