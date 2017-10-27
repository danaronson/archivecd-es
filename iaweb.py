# utility routines

import os
import ConfigParser
import pdb
import urllib2

class IAWeb:
    GET_IDS_URL = 'https://archive.org/metamgr.php?%sf=exportIDs'
    def __init__(self):
        config = ConfigParser.ConfigParser()
        config.read(os.environ['HOME'] + '/.config/ia.ini')
        self.user = config.get('cookies','logged-in-user')
        self.sig = config.get('cookies','logged-in-sig')

    def get_items(self, constraints = {}):
        constraint_string = ""
        for key, value in constraints.items():
            constraint_string += "%s=%s&" % (key, value)
        request_string = self.GET_IDS_URL % constraint_string
        request = urllib2.Request(request_string,
                                  headers={"Cookie" : "logged-in-sig=%s; logged-in-user=%s" % (self.sig, self.user)})
        return urllib2.urlopen(request).read().split('\n')


if __name__ == "__main__":
    ia = IAWeb()
    item_curate_states = {}
    for curate_state in ['dark', 'freeze', "un-dark", "NULL"]:
        items = ia.get_items({'w_collection' : 'acdc*',
                              'w_curatestate' : curate_state})
        for item in items:
            item_curate_states[item.strip().lower()] = curate_state
        print 'there are %d %s items' % (len(items), curate_state)
