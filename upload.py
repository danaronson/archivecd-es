# UUID is needed for eval some of the JSON from the archivecd log
from uuid import UUID

import fcntl
import logging
import ConfigParser
import re
import sys
import os
import pdb
from dateutil import parser
import urllib2
import traceback
import json
import time
from internetarchive import get_item, get_tasks
from elasticsearch import Elasticsearch, helpers, serializer, compat, exceptions

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


# we want to exit if another upload process is running
lock_fd = open(config_path + "/lockfile", 'w+')
try:
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except IOError as e:
    logger.warning("Another Uploader is already running")
    sys.exit(0)

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
    





debugging = ('True' == Config.get('default', 'debug'))

# the default line pattern for archivecd log lines
pattern = re.compile("^(\d\d\d\d-\d\d-\d\d\s+\d\d:\d\d:\d\d,\d+)\s+([^\s]+\(.*\))\s+([^\s]+)\s+([^\s]+)\s+([^\s]+\.py)\s+(.*)$")
file_data_pattern = re.compile("^(.+)_(.+)\.log$")
project_finished_pattern = re.compile("^project_finished: <class 'iaclient.Finished'>\((.*)\)$")
identify_album_finished_pattern = re.compile("^identify_album_finished: <class 'iaclient.Finished'>\((.*)\)$")
log_file_name_pattern = re.compile("\<.*\>(.*\.log)\</a\>")
rename_scan_pattern = re.compile('^.*rename_scan.*\sto\s*(.*)$')

cddb_prefix = "CDDB disc id: "
musicbrainz_prefix = "MusicBrainz disc id "

# add_metadata addes information from the line to metadata fields, which will be written to ES
def add_metadata(groups, metadata, png_files):
    # add the operator
    if groups[5].startswith("OPERATOR: "):
        metadata['operator'] = groups[5][len("OPERATOR: "):].lower()
        return

    match = re.search(rename_scan_pattern, groups[5])
    if match:
        png_files.add(match.group(1))

    match = re.search(identify_album_finished_pattern, groups[5])
    if match:
        metadata['identify'] = match.groups()[0]
        return

    match = re.search(project_finished_pattern, groups[5])
    if match:
        finished_data = eval(match.groups()[0])
        status = finished_data['status']
        # fix up spelling error
        if 'canceled' == status:
            status = 'cancelled'
        metadata['status'] = status
        if 'ok' == status:
            result = finished_data['result'][1]
            metadata['itemid'] = result['itemid']
            metadata['url'] = "https://archive.org/metadata/" + result['itemid']
            metadata['title'] = result['title']
            metadata['artists'] = result['artists']
            metadata['status'] = 'scanned'
        elif 'error' == finished_data['status']:
            metadata['error_string'] = finished_data['error']
        return
    
    if groups[5].startswith(cddb_prefix):
        metadata['CDDBid'] = groups[5][len(cddb_prefix):-1]
        return
    
    if groups[5].startswith(musicbrainz_prefix):
        metadata['MusicBrainzid'] = groups[5][len(musicbrainz_prefix):-1]
        return
    

                                   
    
            
    
def upload(es, file_name, data=None, length=-1):
    index = Config.get('es', 'index')
    items = []
    match = re.search(file_data_pattern, file_name)
    (file_time_stamp, uploader_mac_address) = match.groups()
    file_dt = parser.parse(file_time_stamp)

    # we skip this if it is already in es (unless we are debugging, then we aren't going to upload anyway)
    if not debugging:
        res = es.search(index=index, body={"query":{"bool":{"must":[{"match":{"@timestamp":file_dt}},
                                                                {"match":{"mac_id":uploader_mac_address}}]}}})

        if (0 != len(res['hits']['hits'])):
            logger.info("already loaded '%s', skipping", file_name)
            return

    logger.debug("reading data from %s", file_name)
    i = 0
    if not data:
        data = open(file_name).read()
        length = len(data)

    metadata = {'_type' : 'project', '_index' : index, '@timestamp' : file_dt, 'mac_id' : uploader_mac_address, 'log_file_name': file_name, 'log_length': length,
                'CDDBid' : 'unknown', 'MusicBrainzid' : 'unknown', 'elapsed_time' : 0, 'identify' : 'unknown',
                'status' : 'unknown'}

    png_files = set()
    start_time = None
    for line in data.split('\n'):
        logger.debug("processing line %d", i)
        match = re.search(pattern, line)
        if match:
            groups = match.groups()
            timestamp = parser.parse(groups[0])
            if None == start_time:
                start_time = timestamp
            items.append({'_type': 'log_line', '_index' : index, '@timestamp': timestamp,
                          'thread' : groups[1], 'log_level' : groups[2], 'module' : groups[3],
                          'file' : groups[4], 'message' : groups[5], 'line' : i,
                          'log_file_name' : file_name})
            add_metadata(groups, metadata, png_files)
        else:
            # if a line doesn't parse correctly then we assume that it is a text continuation of the message from the previous line
            items[-1]['message'] += line
            logger.debug("continuation from line %d", items[-1]['line'])
        i += 1

    metadata['elapsed_time'] = (timestamp - start_time).seconds
    metadata['image_count'] = len(png_files)
    logger.debug("done reading data from %s", file_name)
    items.append(metadata)
    logger.debug("bulk upload of %d items", len(items))
    if not debugging:
        helpers.bulk(es, items)
    logger.debug("done with bulk upload of %d items", len(items))
    return metadata




def process_all_logs(prefix, es):
    data = urllib2.urlopen(prefix).read()
    log_file_names = []
    scroll = []
    if not debugging:
        scroll = helpers.scan(es, index = Config.get('es', 'index'), doc_type="project", scroll='5m')
        
    for res in scroll:
        log_file_names.append(res['_source']['log_file_name'])

    for log_file_name in log_file_name_pattern.findall(data):
        url = prefix + log_file_name
        if log_file_name in log_file_names:
            logger.debug("skipping  '%s', already in index", url)
        else:
            logger.debug("downloading and processing '%s'", url)
            response = urllib2.urlopen(url)
            try:
                upload(es, log_file_name, response.read(), response.headers['content-length'])
            except:
                logger.error("Unexpected error, while uploading '%s'", url)
                raise

    

def update_deriving(es):
    logger.debug("looking for 'deriving' or 'scanned' entries")
    query = {  "query": {    "bool": {      "must": [        {          "query_string": {            "analyze_wildcard": True,            "query": "_type:project AND (status:deriving OR status:scanned OR status:uploading)"}}]}}}
    results = es.search(index='archivecd-2017.05.06', body=query,size=10000)
    items = []
    deriving = 0
    finished = 0
    uploading = 0
    for res in results['hits']['hits']:
        doc = res['_source'].copy()
        identifier = doc['itemid']
        put_found = False
        deriving_found = False
        for task in get_tasks(identifier):
            args = task.args
            if 'derive' == args.get('next_cmd', ''):
                deriving_found = True
                break
            elif 's3-put' == args.get('comment', ''):
                put_found = True
        if deriving_found:
            status = 'deriving'
            deriving += 1
        elif put_found:
            status = 'uploading'
            uploading += 1
        metadata = get_item(identifier).metadata
        if 0 != len(metadata):
            if metadata.has_key('ocr'):
                status = 'finished'
                finished += 1            
            doc['collection'] = ";".join(metadata['collection'])
            doc['boxid'] = metadata.get('boxid', 'unknown')
            doc['collection-catalog-number'] = metadata.get('collection-catalog-number', 'unknown')
        doc['status'] = status
        if doc != res['_source']:
            logger.debug("updated '%s' to %s" % (identifier, status))
            items.append({'_type':res['_type'],'_index':res['_index'],'_id':res['_id'],'_op_type':'update','doc':doc})
    if not debugging:
        helpers.bulk(es, items)
    logger.debug("found %d entries, %d of them are uploading %d of them are deriving, %d of them have finished" % (results['hits']['total'], uploading, deriving, finished))




# run that sucker    
if __name__ == "__main__":
    # we fork with one the parent processing the log files and the child updating the derived entires
    if 0 == os.fork():
        update_deriving(get_es())
    else:
        process_all_logs(sys.argv[1], get_es())
        os.wait()

