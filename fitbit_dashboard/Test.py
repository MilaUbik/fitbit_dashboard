import configparser as config_parser
import datetime as dt
import json
import os
from .OAuth2Server import OAuth2Server
from . import influx
from datetime import date
from datetime import datetime

import fitbit
from fitbit.api import Fitbit


class Test:
    def __init__(self):
        self.config = config_parser.ConfigParser()
        self.config.read('config.ini')
        self.token = self.config.get('connection', 'token')
        self.client_id = self.config.get('connection', 'client_id')
        self.client_secret = self.config.get('connection', 'client_secret')
        self.redirect_uri = self.config.get('connection', 'redirect_uri')
        self.api_call_left = int(self.config.get('variable', 'api_call_left'))

        heart_folder = 'heart'
        heart_file = 'heart_{}.json'
        self.sleep_folder = 'sleep'
        self.sleep_file = 'sleep_{}.json'
        food_folder = 'food'
        food_files = ['food_{}.json', 'food_water_{}.json']
        body_folder = 'body'
        body_file = 'body_{}.json'
        activity_folder = 'activity'
        activity_files = ['activity_calories{}.json',
                          'activity_steps{}.json',
                          'activity_distance{}.json',
                          'activity_floors{}.json',
                          'activity_elevation{}.json']

        self.all_folder = [heart_folder, food_folder, body_folder,
                           activity_folder]
        self.all_file = [heart_file, food_files, body_file, activity_files]

        self.activities = ['activities/calories',
                           'activities/steps',
                           'activities/distance',
                           'activities/floors',
                           'activities/elevation']

        foods = ['foods/log/caloriesIn', 'foods/log/water']

        self.time_series_resource = [
            'activities/heart', foods, 'body/fat', self.activities]

        self.token = self.config.get('connection', 'token')
        self.server = OAuth2Server(self.client_id, self.client_secret)
        self.today = date.today()
        self.yesterday = self.today - dt.timedelta(days=1)
        self.create_category_path(self.sleep_folder)
        [self.create_category_path(folder) for folder in self.all_folder]

    def create_category_path(self, folder):
        if not os.path.exists(folder):
            os.makedirs(folder)

    def token_updater(self, token):
        cfgfile = open('config.ini', 'w')
        self.config.set('connection', 'token', token['access_token'])
        self.config.set('connection', 'refresh_token', token['refresh_token'])
        self.config.write(cfgfile)
        cfgfile.close()

    def test_connection(self, fitbit):
        profile = fitbit.user_profile_get()
        if profile:
            print('You are authorized to access data for the user: {}'.format(
                profile['user']['fullName']))
        else:
            fitbit.client.refresh_token()
            print('token refresh')

    def days_between(self, d1, d2):
        return abs((d2 - d1).days)

    def save_file(self, folder, file, date, result):
        f = open(os.path.join(folder, file).format(date), 'w')
        json.dump(result, f)
        f.close()

    def save_time_series(self, activity, folder, file, base_date=None, end_date=None):
        if not base_date and not end_date:
            base_date = self.yesterday
            end_date = self.today
        days = self.days_between(base_date, end_date)
        if activity == 'sleep':
            self.server.fitbit.API_VERSION = 1.2
            [self.save_file(folder, file, cdate, self.server.fitbit.get_sleep(cdate)) for cdate in
             [end_date - dt.timedelta(days=day) for day in range(1, days)]]
            self.server.fitbit.API_VERSION = 1
        else:
            [self.save_file(folder, file, cdate,
                            self.server.fitbit.intraday_time_series(resource=activity, base_date=cdate)) for cdate in
             [end_date - dt.timedelta(days=day) for day in range(0, days)]]

    def save_all(self, date_start, date_end):
        self.save_time_series('sleep', self.sleep_folder, self.sleep_file, base_date=date_start, end_date=date_end)
        # get heath, food, body, activities
        for folder, file, resource in zip(self.all_folder, self.all_file, self.time_series_resource):
            if isinstance(resource, list):
                for index, path in enumerate(resource):
                    self.save_time_series(path, folder, file[index], base_date=date_start, end_date=date_end)
            else:
                self.save_time_series(resource, folder, file, base_date=date_start, end_date=date_end)

    def run(self):
        print('start')
        try:
            date_start = self.config.get('variable', 'date_start')
            date_start = datetime.strptime(date_start, '%Y-%m-%d').date()
            date_end = self.config.get('variable', 'date_end')
            date_end = datetime.strptime(date_end, '%Y-%m-%d').date()

            if self.token:
                refresh_token = self.config.get(
                    'connection', 'refresh_token')
                self.server.fitbit = Fitbit(self.client_id, self.client_secret, access_token=self.token,
                                            refresh_token=refresh_token, refresh_cb=self.token_updater)
                self.test_connection(self.server.fitbit)
            else:
                self.server.browser_authorize()
                self.test_connection(self.server.fitbit)
                self.token_updater(self.server.fitbit.client.session.token)

            if date_start and date_end:
                self.save_all(date_start, date_end)

            else:
                self.save_all(self.yesterday, self.today)

            print('finish')
        except fitbit.api.exceptions.HTTPTooManyRequests as e:
            print('too many requests! waiting {0} minutes'.format(
                e.retry_after_secs % 60))
            print('exit')
        finally:
            influx.import_data()
