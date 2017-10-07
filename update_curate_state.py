# this routine retrieves item_metadata for all es scanned projects and updates the curate_state field of the project

import archivecd
import threading
import re

def get_some_curate_states(thread_id, item_ids, item_id_lock, lock, stop_event, curate_states, acd_instance):
    count = 0
    while True:
        item_id_lock.acquire()
        try:
            item_id = item_ids.pop()
        except IndexError:
            break
        finally:
            item_id_lock.release()
        if stop_event.is_set():
            break 
        if 0 == (count % 100):
            acd_instance.logger.debug("%d: %d" % (thread_id, count))
        count += 1
        curate_state = determine_curate_state(archivecd.get_item_metadata(item_id))
        lock.acquire()
        curate_states[item_id] = curate_state
        lock.release()

def get_all_curate_states(thread_count, acd_instance):
    docs = []
    item_ids = []
    stop_event = threading.Event()
    lock = threading.Lock()
    item_id_lock = threading.Lock()
    threads = []
    count = 0
    curate_states = {}
    acd_instance.logger.debug("Getting all scanned projects from elasticsearch")
    for id, d_type, doc in acd_instance.map_over_data("_type:project AND (status:ok OR status:deriving OR status:scanned OR status:finished)",
                                                   source=['itemid']):
        itemid = doc['itemid']
        item_ids.append(doc['itemid'])
    acd_instance.logger.debug("There are %d item id's to fetch" % len(item_ids))
    acd_instance.logger.debug("Getting all item_metadata from ia")
    for i in range(thread_count):
        thread = threading.Thread(target=get_some_curate_states, args=(i, item_ids, item_id_lock, lock, stop_event, curate_states, acd_instance))
        threads.append(thread)
        thread.start()
    try:
        while True:
            count = 0
            for i in range(thread_count):
                if threads[i].is_alive():
                    count += 1
                threads[i].join(1)
            if 0 == count:
                break
    except KeyboardInterrupt:
        acd_instance.logger.warn("CTRL-C received, waiting for therads to stop")
        stop_event.set()
    for i in range(thread_count):
        threads[i].join()
    acd_instance.logger.debug("Finished getting all item_metadata from ia")
    return curate_states


curation_state_pattern = re.compile("\[state\](.*)\[\/state\]")

def determine_curate_state(item_metadata):
    if item_metadata.has_key('is_dark'):
        return 'dark'
    else:
        try:
            match = re.search(curation_state_pattern, item_metadata['metadata']['curation'].lower())
            if match:
                return match.group(1)
        except:
            pass
    return 'NULL'
    
def update_curate_state(curate_states, acd):
    state = ''
    items = []
    index = acd.config.get('es', 'index')
    keys = curate_states.keys()
    for id, d_type, doc in acd.map_over_data("_type:project AND (status:ok OR status:deriving OR status:scanned OR status:finished)"):
        item_id = doc['itemid']
        if item_id in keys:
            cs = curate_states[doc['itemid']]
            if doc.get('curate_state','') != cs:
                doc['curate_state'] = cs
                items.append({'_type':d_type,'_index':index,'_id':id,'_op_type':'update','doc':doc})
    return items

    

            
def doit():            
    # create the instance
    acd = archivecd.ArchiveCD()
    curate_states = get_all_curate_states(int(acd.config.get('item_metadata', 'thread_count')), acd)
    items = update_curate_state(curate_states,acd)
    #acd.bulk(items)

# run that sucker    
if __name__ == "__main__":
    doit()
    
