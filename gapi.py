# routines for interfacing with google sheets

import pygsheets
from oauth2client.service_account import ServiceAccountCredentials
from dateutil import parser
import httplib2
import logging

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']


class Gapi:
    def __init__(self, Config, logger=None):
        # trying to fix timeouts:  https://github.com/nithinmurali/pygsheets/issues/84#issuecomment-307655891
        http_client = httplib2.Http( timeout=50)
        self.config = Config
        self.gc = pygsheets.authorize(credentials=ServiceAccountCredentials.from_json_keyfile_name(Config.get('gapi','service_file'), SCOPES),
                                      http_client=http_client,retries=3)
        self.logger = logger
        if not self.logger:
            self.logger = logging.getLogger(__name__)

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
                    generic_operator_name = row[2].strip()
                    data = {}
                    if (0 != len(generic_operator_name)):
                        data['generic_operator_name'] = generic_operator_name
                    if 0 != len(operator):
                        for index in range(len(row)- 2):
                            date = values[0][index+2].strip()
                            hours = row[index + 2].strip()
                            if (0 != len(date)) and (0 != len(hours)):
                                try:
                                    date = parser.parse(date)
                                except:
                                    self.logger.error('Parsing error at %s:(%d, %d), string is "%s"' % (worksheet, 0, index + 2, date))
                                    raise
                                data["hours"] = float(hours)
                                items[(str(date)[0:10], operator)] = data.copy()
        return items



