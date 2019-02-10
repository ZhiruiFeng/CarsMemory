#!/usr/bin/env python3
# writer_s3.py

"""Write data to AWS S3"""

import cv2
import os
from src.params import TMP_KEY_PREFIX, STORE_KEY_PREFIX, TMP_FOLER
from src.utils import get_date_from_timestamp
from src.awss3.connector_s3 import S3Connector
import botocore


class S3TmpWriter(object):
    """Used in porter consumer"""

    def __init__(self, bucket_name):
        self.connector = S3Connector(bucket_name)
        self.key_prefix = STORE_KEY_PREFIX

    def _get_tmp_key(self, cam_id, timestamp):
        str_today = get_date_from_timestamp(timestamp)
        return self.key_prefix + str_today + '/' + str(cam_id) + '/' + str(timestamp) + '.jpg'

    def _write_local_file(self, img, timestamp):
        tmp_folder = TMP_FOLER
        if not os.path.exists(tmp_folder):
            os.makedirs(tmp_folder)
        filename = tmp_folder + str(timestamp) + '.jpg'
        cv2.imwrite(filename, img)
        return filename

    def upload_as_public(self, img, cam_id, timestamp):
        s3_key = self._get_tmp_key(cam_id, timestamp)
        local_file = self._write_local_file(img, timestamp)
        self.connector.upload_local_file_public(local_file, s3_key)
        return s3_key

    def upload_public_delete_local(self, img, cam_id, timestamp):
        s3_key = self._get_tmp_key(cam_id, timestamp)
        local_file = self._write_local_file(img, timestamp)
        self.connector.upload_local_file_public(local_file, s3_key)
        os.remove(local_file)
        return s3_key

    def upload_not_public(self, img, cam_id, timestamp):
        s3_key = self._get_tmp_key(cam_id, timestamp)
        local_file = self._write_local_file(img, timestamp)
        self.connector.upload_local_file(local_file, s3_key)
        return s3_key

    def upload_not_public_delete_local(self, img, cam_id, timestamp):
        s3_key = self._get_tmp_key(cam_id, timestamp)
        local_file = self._write_local_file(img, timestamp)
        self.connector.upload_local_file(local_file, s3_key)
        os.remove(local_file)
        return s3_key


class S3StoreWriter(object):

    def __init__(self, bucket_name):
        self.connector = S3Connector(bucket_name)
        self.key_prefix = STORE_KEY_PREFIX

    def _get_s3_key(self, cam_id, timestamp):
        str_today = get_date_from_timestamp(timestamp)
        return self.key_prefix + str_today + '/' + str(cam_id) + '/' + str(timestamp) + '.jpg'

    def _get_tmp_key(self, cam_id, timestamp):
        return TMP_KEY_PREFIX + str(cam_id) + '/' + str(timestamp) + '.jpg'

    def delete_tmp_obj(self, cam_id, timestamp):
        try:
            tmp_key = self._get_tmp_key(cam_id, timestamp)
            obj = self.connector.get_object_with_key(tmp_key)
            obj.delete()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

    def archive_tmp_obj(self, cam_id, timestamp):
        "In order to avoid loss achieve, we only delete useless objects"
        # tmp_key = self._get_tmp_key(cam_id, timestamp)
        store_key = self._get_s3_key(cam_id, timestamp)
        # self.connector.move_obj_within(tmp_key, store_key)
        return store_key
