#!/usr/bin/env python3
# sampling.py

"""To compress the incoming video streaming by sampling."""
from src.awss3.reader_s3 import S3VideoReader
from src.params import MY_BUCKET
from src.utils import GeoLocator
import boto3
import botocore
import cv2
import os
import json


class VideoSampler(object):
    """The sampler could read videos from the folder"""

    def __init__(self, batch):
        self.batch = batch
        self.record_filename = 'dashcash_record.txt'
        self.s3reader = S3VideoReader(MY_BUCKET)
        self.resource = boto3.resource('s3')
        self.bucket = self.resource.Bucket(MY_BUCKET)
        self.visited = set()
        self.cur_video = None
        self.re_file = None
        self.allvideo = None
        self.record_file_key = None
        self.local_file = None
        self.cap = None
        self.location = None
        self.geoLocator = GeoLocator()

    def add_video(self, video_folder_path):
        self.recode_file_key = video_folder_path + self.record_filename
        self.local_file = '/tmp/'+self.record_filename
        try:
            self.bucket.download_file(self.recode_file_key, self.local_file)
            with open(self.local_file, 'r') as re_file:
                content = re_file.readlines()
                for line in content:
                    self.visited.add(line.strip())
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("An unvisited folder")
                # If local has some historical recods, need delete
                if os.path.isfile(self.local_file):
                    os.remove(self.local_file)
            else:
                raise
        self.re_file = open(self.local_file, 'a')
        self.allvideo = self.s3reader.get_urls_in_folder(video_folder_path)
        if not self.next_video():
            print('all video visited')
            exit(-1)

    def read(self):
        success, image = self.cap.read()
        cnt = 0
        while cnt < self.batch:
            if not success:
                if not self.next_video():
                    return False, None, None
            success, image = self.cap.read()
            cnt += 1
        if not success:
            if not self.next_video():
                return False, None, None
            success, image = self.cap.read()
        return success, image, self.location

    def next_video(self):
        if self.cur_video:
            self.visited.add(self.cur_video)
            self.re_file.write(self.cur_video + '\n')
            self.re_file.close()
            try:
                self.resource.Object(MY_BUCKET, self.recode_file_key).delete()
                self.bucket.upload_file(self.local_file, self.recode_file_key, ExtraArgs={'ACL': 'public-read'})
            except botocore.exceptions.ClientError as e:
                self.bucket.upload_file(self.local_file, self.recode_file_key, ExtraArgs={'ACL': 'public-read'})
            self.re_file = open(self.local_file, 'a')

        for url in self.allvideo:
            if url[-3:] == 'txt' or url in self.visited:
                continue
            self.cur_video = url
            # Get related location
            lat, lon = self.read_related_position(url, self.record_file_key)
            if lat and lon:
                self.location = self.geoLocator.get_state(lat, lon)
                if not self.location:
                    self.location = 'Oversea'
            else:
                self.location = 'Unknown'
            if self.cap:
                self.cap.release()
            self.cap = cv2.VideoCapture(self.cur_video)
            print("Ingest a new video {}, location {}".format(self.cur_video, self.location))
            return True
        return False

    def release(self):
        if self.cap:
            self.cap.release()

    def read_related_position(self, url, record_file_key):
        segments = url.split('/')
        raw_name = segments[-1].split('.')[0]
        info_name = raw_name + '.json'
        key_prefix = '/'.join(record_file_key.split('/')[:-2])
        file_key = key_prefix + '/info/' + info_name
        localfile = '/tmp/' + info_name
        try:
            self.bucket.download_file(file_key, localfile)
            with open(self.local_file, 'r') as info_file:
                json_data = info_file.read()
                data = json.loads(json_data)
                slice = data['locations'][0]
                return slice['latitude'], slice['lontitude']
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("Could not find the infomation file")
                return None, None
            else:
                raise
