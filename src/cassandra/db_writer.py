#!/usr/bin/env python3
# db_writer.py

"""Write meta data into Cassandra"""

from db_connector import CassandraConnector
from src.kafka.utils import get_url_from_key, get_s3_key
from src.utils import get_date_from_timestamp


class DBWriter(object):

    def __init__(self):
        self.db = CassandraConnector()
        self.session = self.db.get_session()

    def insert_new_to_frame(self, msginfo):
        camid = msginfo['camera']
        timestamp = msginfo['timestamp']
        date = get_date_from_timestamp(timestamp)
        s3_key = get_s3_key(camid, timestamp)
        storage_link = get_url_from_key(s3_key)
        is_keyframe = msginfo['keyframe']
        obj_tags = msginfo['objs']

        insert_command = ("INSERT INTO frame(dashcam_id, date, store_time, is_keyframe"
                          "storage_link, obj_tags) values(%s, %s, %s, %s, %s, %s)")

        value_list = [camid, date, timestamp, is_keyframe, storage_link, obj_tags]
        self.session.execute(insert_command, value_list)
