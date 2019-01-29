#!/usr/bin/env python3
# video producers

"""
Simulate real-time video streaming:
1. The source could be a video file.
2. The source could also be a folder contains a sequence of keyframes.
"""

from multiprocessing import Process


class VideoStream(Process):
    def __init__(self):
        super().__init__()

    def run(self):
        # TODO
