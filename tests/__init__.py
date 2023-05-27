import os

from elasticsearch_dsl import connections

es_host = os.environ.get('ES_HOST', None)

if not es_host:
    raise EnvironmentError('Please provide ES_HOST environment variable to '
                           'start unittesting.')

connections.create_connection(hosts=[es_host])