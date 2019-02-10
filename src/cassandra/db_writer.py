#!/usr/bin/env python3
# db_writer.py

"""Write meta data into Cassandra"""

from src.cassandra.db_connector import CassandraConnector
from src.kafka.utils import get_url_from_key, get_s3_key
from src.utils import get_date_from_timestamp
import json
from cassandra import RequestExecutionException


class DBWriter(object):

    def __init__(self):
        self.db = CassandraConnector()
        self.session = self.db.get_session()

    def insert_new_to_frame(self, msginfo):

        insert_command = insert_frame_command(msginfo)
        print(insert_command)
        try:
            self.session.execute(insert_command)
        except:
            print('Executation error.')

    def update_statistic(self, msginfo, cnt, keyframes):
        command = update_statistic_command(msginfo, cnt, keyframes)
        print(command)
        try:
            self.session.execute(command)
        except:
            print('Update error.')


def insert_frame_command(msginfo):
    """This greatly related to format parsed by parse_objs funciton in keyframes.py"""
    camid = 'dashcam_' + str(msginfo['camera'])
    timestamp = msginfo['timestamp']
    store_date = get_date_from_timestamp(timestamp)
    s3_key = get_s3_key(camid, timestamp)

    insert_json = {}
    insert_json['dashcam_id'] = camid
    insert_json['store_date'] = store_date
    insert_json['store_time'] = timestamp
    insert_json['is_keyframe'] = msginfo['is_keyframe']
    insert_json['storage_link'] = get_url_from_key(s3_key)
    insert_json['obj_tags'] = msginfo['objs']
    insert_json['scenes'] = msginfo['scenes']
    insert_json['location'] = msginfo['location']

    insert_command = 'INSERT INTO frames JSON \'' + json.dumps(insert_json) + '\'';
    return insert_command


def update_statistic_command(msginfo, cnt, keyframes):
    """This is the information needed to update the statistics"""
    dashcam_id = 'dashcam_' + str(msginfo['camera'])
    store_date = get_date_from_timestamp(msginfo['timestamp'])
    location = msginfo['location']
    total = 0
    command = "UPDATE statistic SET "
    for key in cnt:
        subcommand = str(key) + ' = ' + str(key) + ' + ' + str(cnt[key]) + ','
        total += cnt[key]
        command += subcommand
    command += " total = total + " + str(total) + ','
    command += " keyframes = keyframes + " + str(keyframes)
    command += " WHERE dashcam_id = \'" + dashcam_id + "\'"
    command += " AND store_date= \'" + str(store_date) + "\'"
    command += " AND location= \'" + location + '\';'
    return command
