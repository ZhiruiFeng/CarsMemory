#!/usr/bin/env python3
# sampling.py

"""To compress the incoming video streaming by sampling."""


class VideoSampler(object):

    def __init__(self, cap, batch):
        self.batch = batch

    def add_video(self, cap):
        self.cap = cap

    def read_img(self):
        success, image = self.cap.read()
        if not success:
            return None
        cnt = 1
        while success and cnt < self.batch:
            success, image = self.cap.read()
            cnt += 1
        return image
