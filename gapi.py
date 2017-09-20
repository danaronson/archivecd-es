# routines for interfacing with google sheets

import httplib2
from apiclient import discovery
from oauth2client.file import Storage
from dateutil import parser

class Gapi:
    def __init__(self, Config):
        self.config = Config
        credentials = Storage(Config.get('gapi', 'credential_file')).get()
        http = credentials.authorize(httplib2.Http())
        discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                        'version=v4')
        service = discovery.build('sheets', 'v4', http=http,
                                  discoveryServiceUrl=discoveryUrl)
        self.spreadsheets = service.spreadsheets().values()
        

    def get_machine_names(self):
        mac_mapping = {}
        #
        id = self.config.get('machine_names', 'id')
        result = self.spreadsheets.get(spreadsheetId=id, range='Sheet1').execute()
        values = result.get('values', [])
        for row in values[1:]:
            try:
                mac_mapping[row[1].strip().replace(':','').lower()] = row[0]
            except:
                pass
        return mac_mapping

    def get_hours(self):
        id = self.config.get('timesheet', 'id')
        range_name = self.config.get('timesheet', 'range_name') 
        result = self.spreadsheets.get(spreadsheetId=id, range=range_name).execute()
        values = result.get('values', [])
        #
        items = {}
        records = {}
        if not values:
            print('No data found.')
        else:
            for row in values[2:]:
                operator = row[1].strip()
                if 0 != len(operator):
                    for index in range(len(row)- 2):
                        date = values[0][index+2].strip()
                        hours = row[index + 2].strip()
                        if (0 != len(date)) and (0 != len(hours)):
                            date = parser.parse(date)
                            items[(str(date)[0:10], operator)] = float(hours)
        return items



