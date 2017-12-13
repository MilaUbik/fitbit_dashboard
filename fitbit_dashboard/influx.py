import json
import os
from influxdb import InfluxDBClient
from datetime import datetime
import datetime as dt

sleep_cat = {'wake': 4,
             'rem': 3,
             'light': 2,
             'deep': 1
             }

calories_cat = {0: 'sedentary',
                1: 'lightly active',
                2: 'fairly active',
                3: 'very active'
                }


def write_sleep_measure(influx_client, list_data, list_type):
    points = list_data['levels'][list_type]

    for point in points:
        date = datetime.strptime(point.get('dateTime'), '%Y-%m-%dT%H:%M:%S.%f')
        seconds = point.get('seconds')
        sleep_type = point.get('level')
        bulk = []
        for second in range(0, seconds):
            delta_sec = dt.timedelta(seconds=second)
            point_tmp = json.loads('{}')
            point_tmp['measurement'] = 'sleep'
            point_tmp['time'] = date + delta_sec
            point_tmp['fields'] = {'seconds': 1, 'sleep_type': sleep_cat[sleep_type]}
            point_tmp['tags'] = {'level': sleep_type}
            # leepSeriesHelper(seconds=1, sleep_type=sleep_cat[sleep_type],level= sleep_type, time=date + delta_sec)
            bulk.append(point_tmp)
        # print(bulk)
        influx_client.write_points(bulk)


def write_heart_measure(influx_client, list_data):
    heart_intraday = list_data['activities-heart-intraday']
    # print(input_heart)
    heart_date = datetime.strptime(list_data['activities-heart'][0]['dateTime'], '%Y-%m-%d')
    heart_list = heart_intraday['dataset']
    # heart_date = datetime.timestamp(heart_date)
    bulk = []

    for point in heart_list:
        heart_time = point.get('time').split(':')
        hours = int(heart_time[0])
        minutes = int(heart_time[1])
        seconds = int(heart_time[2])
        heart_hour = dt.timedelta(hours=hours, minutes=minutes,
                                  seconds=seconds)
        date = heart_date + heart_hour
        heartbeat = point.get('value')
        point_tmp = json.loads('{}')
        point_tmp['measurement'] = 'heart'
        point_tmp['time'] = date
        point_tmp['fields'] = {'value': heartbeat}
        point_tmp['tags'] = {'date': heart_date.date()}
        bulk.append(point_tmp)
    influx_client.write_points(bulk)


def write_steps_measure(influx_client, list_data):
    steps_intraday = list_data['activities-steps-intraday']
    steps_date = datetime.strptime(list_data['activities-steps'][0]['dateTime'], '%Y-%m-%d')
    steps_list = steps_intraday['dataset']
    # heart_date = datetime.timestamp(heart_date)
    bulk = []

    for point in steps_list:
        steps_time = point.get('time').split(':')
        hours = int(steps_time[0])
        minutes = int(steps_time[1])
        seconds = int(steps_time[2])
        steps_hour = dt.timedelta(hours=hours, minutes=minutes,
                                  seconds=seconds)
        date = steps_date + steps_hour
        steps_count = point.get('value')
        point_tmp = json.loads('{}')
        point_tmp['measurement'] = 'steps'
        point_tmp['time'] = date
        point_tmp['fields'] = {'value': steps_count}
        point_tmp['tags'] = {'date': steps_date.date()}
        bulk.append(point_tmp)
    influx_client.write_points(bulk)


def write_calories_measure(influx_client, list_data):
    calories_intraday = list_data['activities-calories-intraday']
    calories_date = datetime.strptime(list_data['activities-calories'][0]['dateTime'], '%Y-%m-%d')
    calories_list = calories_intraday['dataset']
    # heart_date = datetime.timestamp(heart_date)
    bulk = []

    for point in calories_list:
        calories_time = point.get('time').split(':')
        hours = int(calories_time[0])
        minutes = int(calories_time[1])
        seconds = int(calories_time[2])
        calories_hour = dt.timedelta(hours=hours, minutes=minutes,
                                     seconds=seconds)
        date = calories_date + calories_hour
        calories_count = point.get('value')
        level = int(point.get('level'))
        point_tmp = json.loads('{}')
        point_tmp['measurement'] = 'calories'
        point_tmp['time'] = date
        point_tmp['fields'] = {'value': calories_count}
        point_tmp['tags'] = {'date': calories_date.date(), 'type': calories_cat[level]}
        bulk.append(point_tmp)
    influx_client.write_points(bulk)


def import_data():
    client = InfluxDBClient('localhost', 8086, 'root', 'root', 'example')
    client.create_database('example')
    activity_dir = os.listdir('activity')
    for file in activity_dir:
        if 'calories' in file:
            with open('activity/{}'.format(file)) as data_file:
                input_calories = json.load(data_file)
                write_calories_measure(client, input_calories)

            print('File: {} inserted'.format(file))
        if 'steps' in file:
            with open('activity/{}'.format(file)) as data_file:
                input_steps = json.load(data_file)
                write_steps_measure(client, input_steps)

            print('File: {} inserted'.format(file))

    sleep_dir = os.listdir('sleep')
    for file in sleep_dir:
        with open('sleep/{}'.format(file)) as data_file:
            input_sleep = json.load(data_file)
            main_sleep_list = [index for index in input_sleep['sleep'] if
                               index['isMainSleep'] is True and index['type'] == 'stages']
            # print(len(main_sleep_list))
            if len(main_sleep_list) > 0:
                main_sleep = json.loads(json.dumps(main_sleep_list[0]))
                write_sleep_measure(client, main_sleep, 'data')

                if 'shortData' in main_sleep['levels']:
                    write_sleep_measure(client, main_sleep, 'shortData')

        print('File: {} inserted'.format(file))

    heart_dir = os.listdir('heart')
    for file in heart_dir:
        with open('heart/{}'.format(file)) as data_file:
            input_heart = json.load(data_file)
            write_heart_measure(client, input_heart)

        print('File: {} inserted'.format(file))
