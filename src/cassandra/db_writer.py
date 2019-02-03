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

        insert_command = 'INSERT INTO frame JSON \'' + json.dumps(insert_json) + '\'';
        try:
            res = self.session.execute(insert_command)
        except:
            print('Executation error.')
