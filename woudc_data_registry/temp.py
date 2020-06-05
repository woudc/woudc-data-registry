import logging 
from urllib.parse import urlparse

import click 
from elasticsearch import Elasticsearch, helpers 
from elasticsearch.exceptions import (ConnectionError, NotFoundError, 
                                      RequestError) 
from woudc_data_registry import config, registry

from woudc_data_registry.models import (Contributor, DataRecord, Dataset, Deployment, Instrument, Project, Station, StationName)

from search import SearchIndex

from sqlalchemy.ext.declarative import declarative_base

from models import Country

#from models import country_models

s = SearchIndex()
contributor_dict = dict({'contributor_id' : '95' , 'country_id' : '001' , 'name' : 'dan' , 'acronym' : 'dwf' , 'project_id' : '002' , 'wmo_region_id' : 'III', 'url' : 'http://localhost:9201//_search/', 
    'email' : 'danwai@gmail.com' , 'ftp_username' : 'dwai573', 'active' : 1 , 'start_date' : '2020-06-03' , 'end_date' : '2020-06-04', 'last_validated_datetime' : '2020-03-02', 'x' : '0.3' , 'y' :'0.4'})
c = Contributor(contributor_dict)
print(registry.Registry())
#models.contributor_models.append(c)
#SearchIndex().index(c,contributor_docs)
print(c.contributor_id)
#SearchIndex.delete(s)
#SearchIndex.create(s)

