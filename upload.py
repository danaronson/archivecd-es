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
import datetime
import urllib2
import traceback
import json
import time
import internetarchive 
from elasticsearch import Elasticsearch, helpers, serializer, compat, exceptions
import gapi
import iaweb
import scandata
import archivecd




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
def add_metadata(groups, metadata, png_files, item_ids):
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
        metadata['@timestamp'] = parser.parse(groups[0])
        metadata['fixed_time'] = True
        if 'ok' == status:
            result = finished_data['result'][1]
            item_id = result['itemid']
            metadata['itemid'] = result['itemid']
            metadata['url'] = "https://archive.org/metadata/" + result['itemid']
            metadata['title'] = result['title']
            metadata['artists'] = result['artists']

            # mark it a duplicate if it is already in item_ids
            if item_id in item_ids:
                metadata['status'] = 'duplicate'
            else:
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
    

                                   
    
def get_es_itemids(acd):
    item_ids = set()
    for id, d_type, doc in acd.map_over_data('_type:project', source=['itemid']):
        item_id = doc.get('itemid', False)
        if item_id:
            item_ids.add(item_id)
    return item_ids

def upload(acd, file_name, item_ids, data=None, length=-1, already_checked_in_es=False):
    index = acd.config.get('es', 'index')
    items = []
    match = re.search(file_data_pattern, file_name)
    (file_time_stamp, uploader_mac_address) = match.groups()
    file_dt = parser.parse(file_time_stamp)

    # we skip this if it is already in es
    if already_checked_in_es and file_name in get_log_file_names_in_es(acd):
        acd.logger.info("already loaded '%s', skipping", file_name)
        return

    acd.logger.debug("reading data from %s", file_name)
    i = 0
    if not data:
        data = open(file_name).read()
        length = len(data)

    metadata = {'_type' : 'project', '_index' : index, '@timestamp' : file_dt, 'mac_id' : uploader_mac_address, 'log_file_name': file_name, 'log_length': length,
                'CDDBid' : 'unknown', 'MusicBrainzid' : 'unknown', 'elapsed_time' : 0, 'identify' : 'unknown',
                'status' : 'unknown'}

    mac_id = uploader_mac_address.strip().replace(':','').lower()
    try:
        metadata['host_name'] = machine_names[mac_id]
    except KeyError:
        pass
    png_files = set()
    start_time = None
    for line in data.split('\n'):
        acd.logger.debug("processing line %d", i)
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
            add_metadata(groups, metadata, png_files, item_ids)
        else:
            # if a line doesn't parse correctly then we assume that it is a text continuation of the message from the previous line
            items[-1]['message'] += line
            acd.logger.debug("continuation from line %d", items[-1]['line'])
        i += 1

    metadata['elapsed_time'] = (timestamp - start_time).seconds
    metadata['image_count'] = len(png_files)
    acd.logger.debug("done reading data from %s", file_name)
    if 'duplicate' == metadata['status']:
        acd.logger.info('got duplicate, updating %s ' % metadata['itemid'])
    items.append(metadata)
    acd.logger.debug("bulk upload of %d items", len(items))
    acd.bulk(items)
    acd.logger.debug("done with bulk upload of %d items", len(items))
    return metadata




def get_log_file_names_in_es(acd):
    ret = set()
    for id, d_type, doc in acd.map_over_data('_type:project', source=['log_file_name']):
        ret.add(doc['log_file_name'])
    return ret

def process_all_logs(prefix, acd):
    acd.logger.info('processing logs from: %s' % prefix)
    data = urllib2.urlopen(prefix).read()
    log_file_names = get_log_file_names_in_es(acd)
    for log_file_name in log_file_name_pattern.findall(data):
        url = prefix + log_file_name
        if log_file_name in log_file_names:
            acd.logger.debug("skipping  '%s', already in index", url)
        else:
            acd.logger.debug("downloading and processing '%s'", url)
            response = urllib2.urlopen(url)
            try:
                upload(acd, log_file_name, get_es_itemids(acd), response.read(), response.headers['content-length'], already_checked_in_es=True)
            except:
                acd.logger.error("Unexpected error, while uploading '%s'", url)
                raise

    

def update_all_curate_states(acd):
    index = acd.config.get('es', 'index')
    item_curate_states = {}
    ia = iaweb.IAWeb()
    for curate_state in ['dark', 'freeze', "un-dark", "NULL"]:
        items = ia.get_items({'w_collection' : 'acdc*',
                              'w_curatestate' : curate_state})
        for item in items:
            item_curate_states[item.strip().lower()] = curate_state
    items = []
    for id, d_type, doc in acd.map_over_data("_type:project AND (status:deriving OR status:scanned OR status:uploading)"):
        identifier = doc['itemid'].lower()
        curate_state = item_curate_states.get(identifier, 'unknown')
        if not doc.has_key('curate_state') or (doc['curate_state'] != curate_state):
            doc['curate_state'] = curate_state
            items.append({'_type':d_type,'_index':index,'_id':id,'_op_type':'update','doc':doc})
    acd.logger.debug('updated curate_state of %d items', len(items))
    acd.bulk(items)

            


# get_items_for_trace_rip_speeds returns items to insert into elasticsearch for per track rip speeds and strategies
def get_items_for_track_rip_speeds(doc, sd, index):
    items = []
    disc_num = 0
    for disc_id, track_rip_speeds in sd.get_main_rip_info().iteritems():
        disc_num += 1
        for data in track_rip_speeds:
            speed_dict = {'_type' : 'track_info', '_index' : index, '@timestamp' : doc['@timestamp'], 'itemid' : doc['itemid'], 'disc_id': disc_id, 'disc_num' : disc_num,
                          'track_num' : data[0]}
            for speed_data in data[1]:
                speed_dict['strategy_' + speed_data['strategy'].lower()] = speed_data['time']
            items.append(speed_dict)
    return items


def update_es_doc(acd, es_id, d_type, doc, items, updates, index):
    updates['count'] += 1
    doc_orig = doc.copy()
    status = doc['status']
    identifier = doc['itemid']
    put_count = 0
    deriving_found = False
    for task in internetarchive.get_tasks(identifier):
        args = task.args
        if 'derive' == args.get('next_cmd', ''):
            deriving_found = True
            break
        elif 's3-put' == args.get('comment', ''):
            put_count += 1
    if deriving_found:
        status = 'deriving'
        updates['deriving'] += 1
    elif 1 < put_count:
        status = 'uploading'
        updates['uploading'] += 1
    item = archivecd.Item(identifier)
    metadata = item.item.metadata
    if 0 != len(metadata):
        if metadata.has_key('ocr'):
            status = 'finished'
            updates['finished'] += 1            
        if not doc.has_key('got_metadata'):
            doc['got_metadata'] = True
            doc['collection'] = ";".join(metadata['collection'])
            doc['boxid'] = metadata.get('boxid', 'unknown')
            doc['collection-catalog-number'] = metadata.get('collection-catalog-number', 'unknown')
            doc['scanning_center'] = metadata.get('scanningcenter', 'unknown')
            # also, let's add the scandata stuff
            sd = scandata.ScanData(item = item, logger = acd.logger)
            if None != sd.data:
                data = sd.data
                doc['scan_wait_time'] = sd.get_scan_bias()
                doc['first_template'] = sd.get_first_scan_template()
                doc['discs'] = len(data['technical_metadata']['discs'])
                tab_data = data['analytics']['tabs']
                for key in tab_data:
                    doc[key + '_time_focused'] = tab_data[key]['total_time_focused']
                items += get_items_for_track_rip_speeds(doc, sd, index)
            
    doc['status'] = status
    if doc != doc_orig:
        acd.logger.debug("updated '%s' to %s" % (identifier, status))
        items.append({'_type':d_type,'_index':index,'_id':es_id,'_op_type':'update','doc':doc})
    
def update_deriving(acd):
    index = acd.config.get('es', 'index')
    acd.logger.debug("looking for 'deriving', 'uploading' or 'scanned' entries")
    items = []
    updates = {'deriving' : 0, 'finished' : 0, 'uploading' : 0,
               'count' : 0}
    for es_id, d_type, doc in acd.map_over_data("_type:project AND (status:deriving OR status:scanned OR status:uploading)"):
        try:
            update_es_doc(acd, es_id, d_type, doc, items, updates, index)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            acd.logger.error('Error while trying to get rip speeds for "%s"' % (doc['itemid']))
            for line in traceback.format_exception(exc_type, exc_value,
                                                   exc_traceback):
                acd.logger.error(line.strip())
    acd.bulk(items)
    acd.logger.debug("found %d entries, %d of them are uploading %d of them are deriving, %d of them have finished" %
                     (updates['count'], updates['uploading'], updates['deriving'], updates['finished']))




# run that sucker    
if __name__ == "__main__":
    # we fork with one the parent processing the log files and the child updating the derived entires
    acd = archivecd.ArchiveCD(config_file = 'config.txt')

    # we want to exit if another upload process is running
    lock_fd = open(acd.config_path + "/lockfile", 'w+')
    machine_names = gapi.Gapi(acd.config).get_machine_names()
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError as e:
        acd.logger.warning("Another Uploader is already running")
        sys.exit(0)

    if 0 == os.fork():
        update_deriving(acd)
        update_all_curate_states(acd)
        pass
    else:
        items_to_process = ['archivecd-logs']

        first = datetime.datetime(2018, 1, 1)
        now = datetime.datetime.now()

        year = first.year

        while datetime.datetime(year, 1, 1) < now:
            for month in range(1,13):
                log_date = datetime.datetime(year, month, 1)
                item_name = log_date.strftime('archivecd-logs-%Y-%m')
                items_to_process.append(item_name)

            year += 1

        for item in items_to_process:
            if internetarchive.get_item(item).exists:
                process_all_logs('https://archive.org/download/%s/' % item, acd)
        os.wait()

