# routines for uploading gsheets hours worked spreadsheet data into ES and aggregating projects and discs record
# from scanning info into user records to get averages.



import httplib2

import json
import pdb
import os
import sys
from apiclient import discovery
from oauth2client.file import Storage
from dateutil import parser
from elasticsearch import Elasticsearch, helpers, serializer, compat, exceptions
import logging
import ConfigParser

# read from same directory as this
base_name = os.path.dirname(sys.argv[0])
if '' == base_name:
    config_path = "."
else:
    config_path = base_name
Config = ConfigParser.SafeConfigParser()

config_file_name = config_path + "/config.txt"
if 0 == len(Config.read(config_file_name)):
    sys.stderr.write("Could not find config file: '%s'\n" % config_file_name)
    sys.exit(-1)
    
log_levels = {"CRITICAL":logging.CRITICAL,"ERROR":logging.ERROR,"WARNING":logging.WARNING,"INFO":logging.INFO,"DEBUG":logging.DEBUG,"NOTSET":logging.NOTSET}

logger = logging.getLogger(__name__)
logger.setLevel(log_levels[Config.get('logging', 'level')])

# create console handler and set level to debug
ch = logging.StreamHandler()

# create console handler and set level to debug
logging_file = Config.get('logging', 'file')
if 'stdout' == logging_file:
    ch = logging.StreamHandler()
else:
    ch = logging.FileHandler(logging_file)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
if 0 == len(logger.handlers):
    logger.addHandler(ch)

# see https://github.com/elastic/elasticsearch-py/issues/374
class JSONSerializerPython2(serializer.JSONSerializer):
    """Override elasticsearch library serializer to ensure it encodes utf characters during json dump.
    See original at: https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L42
    A description of how ensure_ascii encodes unicode characters to ensure they can be sent across the wire
    as ascii can be found here: https://docs.python.org/2/library/json.html#basic-usage
    """
    
    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, compat.string_types):
            return data
        try:
            return json.dumps(data, default=self.default, ensure_ascii=True)
        except (ValueError, TypeError) as e:
            raise exceptions.SerializationError(data, e)



def get_es():
    return Elasticsearch([Config.get('es', 'host')], 
                         port=int(Config.get('es', 'port')), use_ssl=('True' == Config.get('es','use_ssl')),
                         url_prefix = Config.get('es', 'url_prefix'), serializer=JSONSerializerPython2())




def get_hours():
    #credentials = Storage('/Users/dan/.credentials/sheets.googleapis.com-python-quickstart.json').get()
    credentials = Storage(Config.get('gapi', 'credential_file')).get()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)
    #
    id = Config.get('timesheet', 'id')
    range_name = Config.get('timesheet', 'range_name') 
    result = service.spreadsheets().values().get(
        spreadsheetId=id, range=range_name).execute()
    values = result.get('values', [])
    #
    items = {}
    records = {}
    if not values:
        print('No data found.')
    else:
        for row in values[2:]:
            operator = row[1].strip()
            if 0 != len(operator):
                for index in range(len(row)- 2):
                    date = values[0][index+2].strip()
                    hours = row[index + 2].strip()
                    if (0 != len(date)) and (0 != len(hours)):
                        date = parser.parse(date)
                        items[(str(date)[0:10], operator)] = float(hours)
    return items

# using scrolling, map over a query
def map_over_data(query, es, size=10000, source=True):
    index = Config.get('es', 'index')
    query = {  "query": {    "bool": {      "must": [        {          "query_string": {            "analyze_wildcard": True,            "query": query}}]}}}
    logger.debug("Querying ES for: '%s'" % query)
    page = es.search(index=index, body=query,scroll='2m',size=size, _source=source)
    reported_size =  page['hits']['total']
    count = 0
    while True:
        if 0 == len(page['hits']['hits']):
            break
        for res in page['hits']['hits']:
            yield res['_id'], res['_type'], res['_source']
            count += 1
        if count == reported_size:
            break
        page = es.scroll(scroll_id = page['_scroll_id'], scroll = '2m')

# retrieve_hours_worked get's all the hours worked data in es
def retrieve_hours_worked():
    hours_worked = {}
    for id, d_type, doc in map_over_data("_type:hours_worked", es):
        hours_worked[json.dumps([doc['@timestamp'][0:10], doc['operator']])] = {'_id':id,
                                                                                'total_cds': doc.get('total_cds', 0),
                                                                                'total_projects': doc.get('total_projects', 0),
                                                                                '@timestamp': doc['@timestamp'],
                                                                                'operator': doc['operator']}
                                                                                
    return hours_worked

def add_data_for_operators(es):
    hours_worked = retrieve_hours_worked()
    index = Config.get('es', 'index')
    updated_keys = set()
    items = []
    project_records_updated = 0
    for id, d_type, doc in map_over_data("_type:project AND _exists_:discs", es):
        key_for_hours_worked = json.dumps([doc['@timestamp'][0:10], doc['operator']])
        if hours_worked.has_key(key_for_hours_worked) and (not doc.get('added_to_hours_worked', False)):
            project_records_updated += 1
            doc['added_to_hours_worked'] = True
            data = hours_worked[key_for_hours_worked]
            data['total_discs'] = data.get('total_discs', 0) + doc['discs']
            data['total_projects'] = data.get('total_projects', 0) + 1
            data['total_images'] = data.get('total_images', 0) + doc['image_count']
            items.append({'_type':'project','_index':index,'_id':id,'_op_type':'update','doc':doc})
            updated_keys.add(key_for_hours_worked)
    # now add the hours worked items
    for key in updated_keys:
        doc = hours_worked[key]
        id = doc.pop('_id')
        items.append({'_type':'hours_worked','_index':index,'_id':id,'_op_type':'update','doc':doc})
    logger.info("%d project records updated, %d user records updated" % (project_records_updated, len(updated_keys)))
    helpers.bulk(es, items)


    
# upload_new_hours gets the records in the hours_worked index and uploads the new ones
# if the old ones have been modified, an error is thrown
def upload_new_hours(es):
    index = Config.get('es', 'index')
    es_items = {}
    items = []
    spreadsheet_items = get_hours()
    for id, d_type, doc in map_over_data("_type:hours_worked", es):
        es_items[(doc['@timestamp'][0:10], doc['operator'])] = (doc, id)
    count = 0
    updated = 0
    for key in spreadsheet_items.keys():
        (doc, id) = es_items.get(key, (False, False))
        if not doc:
            count += 1
            (date, operator) = key
            items.append({'_type' : 'hours_worked',
                          '_index' : index,
                          '@timestamp' : parser.parse(date).date(),
                          'operator' : operator,
                          'hours' : spreadsheet_items[key]})
        elif doc['hours'] != spreadsheet_items[key]:
            # update
            updated += 1
            doc['hours'] = spreadsheet_items[key]
            items.append({'_type':'hours_worked','_index':index,'_id':id,'_op_type':'update','doc':doc})
    logger.info("%d new user, %d updated records from spreadsheet uploaded to ES" % (count, updated))
    helpers.bulk(es, items)
            
if __name__ == '__main__':
    es = get_es()
    upload_new_hours(es)
    add_data_for_operators(es)
    
    
