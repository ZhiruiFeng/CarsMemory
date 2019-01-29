#!/usr/bin/env python3
# db_writer.py

"""Write meta data into Cassandra"""

from db_connector import CassandraConnector
import datetime


class DBWriter(object):

    def __init__(self):
        self.db = CassandraConnector()
        self.session = self.db.get_session()

    def insert_new_to_frame(self, frameinfo):
        dashcam_id = frameinfo['cam_id']
        storage_link = frameinfo['storage_link']
        obj_tags = frameinfo['obj_tags']
        # Decide the generate time later
        gen_date = datetime.date.today().strftime("%y-%m-%d")
        store_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        insert_command = ("INSERT INTO frame(dashcam_id, date, store_time,"
                          "storage_link, obj_tags) values(%s, %s, %s, %s, %s)")

        value_list = [dashcam_id, gen_date, store_time, storage_link, obj_tags]
        self.session.execute(insert_command, value_list)
