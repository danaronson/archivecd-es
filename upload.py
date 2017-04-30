from uuid import UUID
import re
import logging
import ConfigParser
import sys
import pdb
from dateutil import parser
import urllib2
import traceback
import json
from elasticsearch import Elasticsearch, helpers, serializer, compat, exceptions

Config = ConfigParser.ConfigParser()
Config.read("config.txt")

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



# if we should use elasticsearch then import and create the variable
use_es = Config.has_section('es') and Config.get('es','use_es') and ('True' == Config.get('es', 'use_es'))
if use_es:

    if Config.get('es','user'):
        es = Elasticsearch([Config.get('es', 'host')], http_auth=(Config.get('es', 'user'), Config.get('es', 'password')),
                       port=int(Config.get('es', 'port')), use_ssl=('True' == Config.get('es','use_ssl')),
                       url_prefix = Config.get('es', 'url_prefix'), serializer=JSONSerializerPython2())
    else:
        es = Elasticsearch([Config.get('es', 'host')], 
                       port=int(Config.get('es', 'port')), use_ssl=('True' == Config.get('es','use_ssl')),
                       url_prefix = Config.get('es', 'url_prefix'), serializer=JSONSerializerPython2())
    


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
if 0 == len(logger.handlers):
    logger.addHandler(ch)



# the default line pattern for archivecd log lines
pattern = re.compile("^(\d\d\d\d-\d\d-\d\d\s+\d\d:\d\d:\d\d,\d+)\s+([^\s]+\(.*\))\s+([^\s]+)\s+([^\s]+)\s+([^\s]+\.py)\s+(.*)$")
file_data_pattern = re.compile("^(.+)_(.+)\.log$")
project_finished_pattern = re.compile("^project_finished: <class 'iaclient.Finished'>\((.*)\)$")

# add_metadata addes information from the line to metadata fields, which will be written to ES
def add_metadata(groups, metadata):
    # add the operator
    if groups[5].startswith("OPERATOR: "):
        metadata['operator'] = groups[5][len("OPERATOR: "):]
        return
    if groups[2] == "ERROR":
        metadata['error'] = True
        return
    if groups[5].startswith("project_finished:"):
        match = re.search(project_finished_pattern, groups[5])
        if match:
            finished_data = eval(match.groups()[0])
            metadata['status'] = finished_data['status']
            if 'ok' == finished_data['status']:
                result = finished_data['result'][1]
                metadata['itemid'] = result['itemid']
                metadata['url'] = "https://archive.org/metadata/" + result['itemid']
                metadata['title'] = result['title']
                metadata['artists'] = result['artists']
        return
    
            
    
def process(match):
    logger.debug("Found match")

def upload(file_name, data=None):
    index = Config.get('es', 'index')
    items = []
    match = re.search(file_data_pattern, file_name)
    (file_time_stamp, uploader_mac_address) = match.groups()
    file_dt = parser.parse(file_time_stamp)

    # we skip this if it is already in es
    res = es.search(index=index, body={"query":{"bool":{"must":[{"match":{"@timestamp":file_dt}},
                                                                {"match":{"mac_id":uploader_mac_address}}]}}})

    if (0 != len(res['hits']['hits'])):
        logger.info("already loaded '%s', skipping", file_name)
        return

    metadata = {'_type' : 'project', '_index' : index, '@timestamp' : file_dt, 'mac_id' : uploader_mac_address, 'log_file_name': file_name}
    logger.debug("reading data from %s", file_name)
    i = 0
    if not data:
        data = open(file_name).read()
    for line in data.split('\n'):
        logger.debug("processing line %d", i)
        match = re.search(pattern, line)
        if match:
            groups = match.groups()
            items.append({'_type': 'log_line', '_index' : index, '@timestamp': parser.parse(groups[0]),
                          'thread' : groups[1], 'log_level' : groups[2], 'module' : groups[3],
                          'file' : groups[4], 'message' : groups[5], 'line' : i,
                          'log_file_name' : file_name})
            add_metadata(groups, metadata)
        else:
            # if a line doesn't parse correctly then we assume that it is a text continuation of the message from the previous line
            items[-1]['message'] += line
            logger.debug("continuation from line %d", items[-1]['line'])
        i += 1
    logger.debug("done reading data from %s", file_name)
    items.append(metadata)
    logger.debug("bulk upload of %d items", len(items))
    helpers.bulk(es, items)
        
    logger.debug("done with bulk upload of %d items", len(items))



def read_files(prefix, log_file_names):
    for line in open(log_file_names).read().splitlines():
        logger.debug("downloading and processing '%s'", line)
        response = urllib2.urlopen(prefix + line)
        try:
            upload(line, response.read())
        except:
            traceback.print_exc()
            pdb.set_trace()
            
    



    
