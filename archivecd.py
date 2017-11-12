# default interfaces (config/es/logging)
from elasticsearch import Elasticsearch, helpers, serializer, compat, exceptions
import threading
import logging
import ConfigParser
import os
import sys
import json
import internetarchive
import inspect
import scandata
import pdb

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




def get_item_metadata(item_id):
    return internetarchive.get_item(item_id).item_metadata


class ArchiveCD():

    LOG_LEVELS = {"CRITICAL":logging.CRITICAL,"ERROR":logging.ERROR,"WARNING":logging.WARNING,"INFO":logging.INFO,"DEBUG":logging.DEBUG,"NOTSET":logging.NOTSET}

    def __init__(self, config_file='config.txt', name=__name__):
        # read from same directory as this
        base_name = os.path.dirname(sys.argv[0])
        if '' == base_name:
            self.config_path = "."
        else:
            self.config_path = base_name
        self.config = ConfigParser.SafeConfigParser()
        config_file_name = self.config_path + "/" + config_file
        if 0 == len(self.config.read(config_file_name)):
            raise IOError("Could not find config file: '%s'\n" % config_file_name)
        self.logger = logging.getLogger(name)
        self.logger.setLevel(ArchiveCD.LOG_LEVELS[self.config.get('logging', 'level')])
        # create console handler and set level to debug
        ch = logging.StreamHandler()

        # create console handler and set level to debug
        logging_file = self.config.get('logging', 'file')
        if 'stdout' == logging_file:
            ch = logging.StreamHandler()
        else:
            ch = logging.FileHandler(logging_file)

        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # add formatter to ch
        ch.setFormatter(formatter)

        # add ch to logger
        if 0 == len(self.logger.handlers):
            self.logger.addHandler(ch)

        self.debug = 'true' == self.config.get('default', 'debug').lower()

        self.es = Elasticsearch([self.config.get('es', 'host')], 
                         port=int(self.config.get('es', 'port')), use_ssl=('True' == self.config.get('es','use_ssl')),
                         url_prefix = self.config.get('es', 'url_prefix'), serializer=JSONSerializerPython2(),
                         timeout=30)

    # using scrolling, map over a query
    def map_over_data(self, query, size=10000, source=True):
        index = self.config.get('es', 'index')
        query = {  "query": {    "bool": {      "must": [        {          "query_string": {            "analyze_wildcard": True,            "query": query}}]}}}
        self.logger.debug("Querying ES for: '%s'" % query)
        page = self.es.search(index=index, body=query,scroll='2m',size=size, _source=source)
        reported_size =  page['hits']['total']
        count = 0
        while True:
            if 0 == len(page['hits']['hits']):
                break
            for res in page['hits']['hits']:
                source = res.get('_source', False)
                yield res['_id'], res['_type'], source
                count += 1
            if count == reported_size:
                break
            page = self.es.scroll(scroll_id = page['_scroll_id'], scroll = '2m')


    def get_rip_data_from_item(self, item):
        sd_instance = scandata.ScanData(item=item,logger=self.logger)
        try:
            rip_info = sd_instance.get_main_rip_info()
        except Exception as err:
            self.logger.warning('%s %s while looking for rip info from %s' % (type(err).__name__, err, item.name))
            return None
        return rip_info

    def bulk(self, items):
        if self.debug:
            self.logger.warning('in debug mode, not uploading to es')
            return []
        else:
            return helpers.bulk(self.es, items)


class Item():
    def __init__(self, item_name):
        self.name = item_name
        self.item = internetarchive.get_item(item_name)
                
