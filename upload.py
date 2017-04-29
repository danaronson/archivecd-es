import re
import logging
import ConfigParser
import sys
import pdb
from dateutil import parser
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

Config = ConfigParser.ConfigParser()
Config.read("config.txt")

# if we should use elasticsearch then import and create the variable
use_es = Config.has_section('es') and Config.get('es','use_es') and ('True' == Config.get('es', 'use_es'))
if use_es:
    from elasticsearch import Elasticsearch, helpers
    if Config.get('es','user'):
        es = Elasticsearch([Config.get('es', 'host')], http_auth=(Config.get('es', 'user'), Config.get('es', 'password')),
                       port=int(Config.get('es', 'port')), use_ssl=('True' == Config.get('es','use_ssl')),
                       url_prefix = Config.get('es', 'url_prefix'))
    else:
        es = Elasticsearch([Config.get('es', 'host')], 
                       port=int(Config.get('es', 'port')), use_ssl=('True' == Config.get('es','use_ssl')),
                       url_prefix = Config.get('es', 'url_prefix'))
    



# the default line pattern for archivecd log lines
pattern = re.compile("^(\d\d\d\d-\d\d-\d\d\s+\d\d:\d\d:\d\d,\d+)\s+([^\s]+\(.*\))\s+([^\s]+)\s+([^\s]+)\s+([^\s]+\.py)\s+(.*)$")
file_date_pattern = re.compile("^(.+)_(.+)\.log$")

def process(match):
    logging.debug("Found match")

def upload(file_name):
    items = []
    index = Config.get('es', 'index')
    match = re.search(file_date_pattern, file_name)
    (file_time_stamp, uploader_mac_address) = file_name.split('_')
    items.append({'_type' : 'project', '_index' : index, '@timestamp' : parser.parse(file_time_stamp), 'mac_id' : uploader_mac_address})
    logging.debug("reading data from %s", file_name)
    for i, line in enumerate(open(file_name)):
        logging.debug("processing line %d", i)
        match = re.search(pattern, line)
        if match:
            groups = match.groups()
            items.append({'_type': 'log_line', '_index' : index, '@timestamp': parser.parse(groups[0]),
                          'thread' : groups[1], 'log_level' : groups[2], 'module' : groups[3],
                          'file' : groups[4], 'message' : groups[5], 'line' : i})
        else:
            # if a line doesn't parse correctly then we assume that it is a text continuation of the message from the previous line
            items[-1]['message'] += line
            logging.debug("continuation from line %d", items[-1]['line'])
    logging.debug("done reading data from %s", file_name)
    if use_es:
        logging.debug("bulk upload of %d items", len(items))
        helpers.bulk(es, items)
        logging.debug("done with bulk upload of %d items", len(items))



    
