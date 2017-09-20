# routines for interfacing with google sheets

import httplib2
from apiclient import discovery
from oauth2client.file import Storage

class Gapi:
    def __init__(self, Config):
        self.config = Config
        credentials = Storage(Config.get('gapi', 'credential_file')).get()
        http = credentials.authorize(httplib2.Http())
        discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                        'version=v4')
        self.service = discovery.build('sheets', 'v4', http=http,
                                       discoveryServiceUrl=discoveryUrl)
        

    def get_machine_names(self):
        mac_mapping = {}
        #
        id = self.config.get('machine_names', 'id')
        result = self.service.spreadsheets().values().get(
            spreadsheetId=id, range='Sheet1').execute()
        values = result.get('values', [])
        for row in values[1:]:
            try:
                mac_mapping[row[1].strip().replace(':','').lower()] = row[0]
            except:
                pass
        return mac_mapping


