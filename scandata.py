#!/usr/bin/env python3
import sys
import json
import logging
import re
import internetarchive
import archivecd


# TODO: Parse other import events (that do not relate to ripping):
# - Time taken from review item to project finished (finalise done, presumably)
#   Finalise can take a few minutes if there are a lot of scans - we might want
#   to optimise that if it turns out to cost a lot of time (hence, parsing the
#   events)
# - Parse scan events, figure out when the last scan event was, and see if it
#   can tell us if the scanner was waiting (unclear, as the scanner may also be
#   editing the disc metadata (but perhaps this does not happen often and we can
#   discard it)

# state = (None, started, finished)
STATE_UNKNOWN = 0
STATE_STARTED = 1
STATE_FINISHED = 2

STATE_RIP_UNKNOWN = 0
STATE_RIP_STARTED = 1
STATE_RIP_FINISHED = 2


class ScanData():

    def __init__(self, data=None, file=None, logger=None, item=None, name=None):
        self.data = None
        if data:
            self.data = data
        elif file:
            self.data = json.load(open(file_name, 'r'))
        else:
            if name:
                item = archivecd.Item(name)
            if item:
                for f in item.item.files:
                    if (f['name'].lower() == 'scandata.json') or (re.search('scandata json', f['format'], re.IGNORECASE)):
                        self.data = internetarchive.File(item.item, f['name']).download(return_responses=True, retries=3).json()
        if logger:
            self.logger = logger
        else:
            logging.basicConfig(level=logging.WARNING)
            self.logger = logging.getLogger(__name__)


    def get_rip_breakdown(self, discid):
        def get_track(desc):
            desc = desc.replace('Reading track ', '')
            track = desc[:desc.find(' ')]
            return int(track)
    
        state = {'state': STATE_RIP_UNKNOWN, 'track': None, 'strategy': None}
    
        update_events = filter(lambda x: x[0] == 'rip' and x[1] == 'update', self.events)
        discid_events = filter(lambda x: x[3]['mb_discid'] == discid, update_events)
    
        # TODO: detect slow re-rips?
        # TODO: how to deal with rip re-starts
    
        last_desc = None
    
        tracks = {}
    
        for event in discid_events:
            self.logger.debug('state: {}'.format(state))
            self.logger.debug('event:{}'.format(event))
            event_type = event[0]
            event_msg = event[1]
            event_time = event[2]
            event_args = event[3]
    
    
            desc = event_args['task_description']
    
            if last_desc == desc:
                continue
            last_desc = desc
    
            if desc.startswith('Reading track '):
                assert state['state'] in (STATE_RIP_UNKNOWN, STATE_RIP_FINISHED), 'Previous rip not finished'
                track = get_track(desc)
                state = {'state': STATE_RIP_STARTED, 'track': track, 'strategy':
                        event_args['strategy'], 'start_time': event_time}
    
            elif desc.startswith('Calculating CRC32') or desc.startswith('Encoding'):
                assert state['state'] in (STATE_RIP_STARTED, STATE_RIP_FINISHED), 'Rip was not started?'
    
                # Scandata is kind of messed up, we sometimes just outright miss one
                # of the events, so now we accept any (start and finished), and as a
                # result we might not always have a state that has a start_time.
                if 'start_time' in state:
                    assert state['start_time'] < event_time, 'Events not chronological?'
    
                    track = state['track']
                    
                    # Any other event = and of previous rip
                    if track not in tracks:
                        tracks[track] = []
    
                    tracks[track].append({'strategy': state['strategy'], 'time':
                        event_time - state['start_time']})
    
                    state = {'state': STATE_RIP_FINISHED, 'track': None,
                             'strategy': None}
    
    
        assert state['state'] == STATE_RIP_FINISHED, 'Last rip did not finish'
    
        from pprint import pprint, pformat
    
        tracks_l = sorted(tracks.items())
        self.logger.debug(tracks_l)
    
        return tracks_l
    
    #        if event_msg == 'update':
    #            event_args['task_description']
    #            print('UPDATE', event[3]['task_description'], event[3]['strategy'])
    #            #start=Reading track 1 of 10
    #            #end=Any next event
    
    
    def get_main_rip_info(self):
        self.events = self.data['analytics']['events']
        state = STATE_UNKNOWN
        important_rip_events = []
    
        data = {}
        for event in self.events:
            event_type = event[0]
            if event_type != "rip":
                continue
    
            event_msg = event[1]
            event_time = event[2]
            event_args = event[3]
    
            if event_msg == "start" or event_msg == "complete":
                important_rip_events.append({"type": event_type, "msg": event_msg,
                                         "time": event_time,
                                         "discid": event_args['mb_discid']})
    
    
        start_time = None
        stop_time = None
        current_discid = None
    
        time_per_rip = []
    
        for event in important_rip_events:
            if event["msg"] == "start":
                if state == STATE_STARTED:
                    assert event["discid"] == current_discid, 'Start event for a new discid without finishing the previous one'
                    self.logger.debug('Found restart, time taken until now: {}'.format(event["time"] - start_time))
                else:
                    start_time = event["time"]
                    current_discid = event["discid"]
    
                state = STATE_STARTED
    
            if event["msg"] == "complete":
                assert state == STATE_STARTED, 'Completed event without start event preceding it'
                assert event["discid"] == current_discid, 'Finished event for a different discid than last start event'
                stop_time = event["time"]
    
                time_per_rip.append({"time": stop_time - start_time,
                                    "discid": event["discid"]})
    
                state = STATE_FINISHED
    
            # Can only be STATE_UNKNOWN at the start
            assert state != STATE_UNKNOWN, 'State is unknown after first rip event'
    
        # Final state should be finished
        assert state == STATE_FINISHED, 'Final state is not finished'
    
        discids = [x["discid"] for x in time_per_rip]
    
        assert len(discids) == len(list(set(discids))), 'Duplicate disc IDs! Bugged log file?'
    
        from pprint import pprint, pformat
        self.logger.debug(pformat(time_per_rip))
    
        total_time = sum([x["time"] for x in time_per_rip])
        self.logger.debug('Total time: {}'.format(total_time))
    
        for discid in discids:
            data[discid] = self.get_rip_breakdown(discid)
        return data
    
    
    def get_scan_bias(self, before_template = 'cd_face'):
        last_rip_time = -1
        last_scan_time = -1
        template_name = ''
        for event in self.data['analytics']['events']:
            if ('rip' == event[0]) and ('complete' == event[1]):
                last_rip_time = event[2]
            elif 'scan' == event[0]:
                if 'start' == event[1]:
                    template_name = event[3]['template']
                if 'complete' == event[1]:
                    if before_template == template_name:
                        break
                    last_scan_time = event[2]
        if -1 == last_scan_time:
            total_rip_time = 0
            for key, value in self.get_main_rip_info().iteritems():
                for strategy, time in value[1].iteritems():
                    total_rip_time += time
            return -total_rip_time
        else:
            return last_scan_time - last_rip_time

    def get_first_scan_template(self):
        for event in self.data['analytics']['events']:
            if 'scan' == event[0] and 'start' == event[1]:
                return event[3]['template']
        return 'Unknown'


