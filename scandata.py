#!/usr/bin/env python3
import sys
import json
import logging

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

logging.basicConfig(level=logging.WARNING)

class ScanData():

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def set_logger(self, logger):
        self.logger = logger

    def load_from_data(self, data):
        self.events = data['analytics']['events']

    def load_from_file(self, file_name):
        self.load_from_data(json.load(open(file_name, 'r')))


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
    
        run_buckets = {}
    
        for (track_no, track) in tracks_l:
            for run in track:
                self.logger.debug(run)
                strategy = run['strategy']
                if strategy not in run_buckets:
                    run_buckets[strategy] = 0.
    
                time = run['time']
    
                run_buckets[strategy] += time

        self.logger.debug(pformat(run_buckets))
        return run_buckets
    
    #        if event_msg == 'update':
    #            event_args['task_description']
    #            print('UPDATE', event[3]['task_description'], event[3]['strategy'])
    #            #start=Reading track 1 of 10
    #            #end=Any next event
    
    
    def get_main_rip_info(self):
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
    
    



