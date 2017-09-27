# routines for interfacing with google sheets

import pygsheets
from oauth2client.service_account import ServiceAccountCredentials
from dateutil import parser

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']


class Gapi:
    def __init__(self, Config):
        self.config = Config
        self.gc = pygsheets.authorize(credentials=ServiceAccountCredentials.from_json_keyfile_name(Config.get('gapi','service_file'), SCOPES))

    def get_machine_names(self):
        mac_mapping = {}
        #
        worksheet = self.gc.open_by_key(self.config.get('machine_names', 'id')).worksheet_by_title('Sheet1')
        for row in worksheet.get_all_values(returnas='matrix'):
            try:
                mac_mapping[row[1].strip().replace(':','').lower()] = row[0]
            except:
                pass
        return mac_mapping


    # parse all hours worked entries for sheets(tabs) that do not have the word 'schedule' in the title
    def get_hours(self):
        gsheet = self.gc.open_by_key(self.config.get('timesheet', 'id'))
        items = {}
        records = {}
        for worksheet in gsheet.worksheets():
            if -1 == worksheet.title.lower().find('schedule'):
                values = worksheet.get_all_values(returnas='matrix')
            #
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



