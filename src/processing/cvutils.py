#!/usr/bin/env python3
# cvutils.py

"""Some functions for computer vision"""

import cv2
import imutils
import numpy as np


def get_fps(video):
    return video.get(cv2.CAP_PROP_FPS)


def rotate(img):
    height, width, _ = img.shape
    if height > width:
        img = np.rot90(img)
    img_gray = cv2.cvtColor(cv2.cvtColor(img.astype(np.uint8), cv2.COLOR_BGR2RGB), cv2.COLOR_BGR2GRAY)
    up = img_gray[:10]
    down = img_gray[-10:]
    if up.sum() > down.sum():
        return img
    else:
        img = np.rot90(img)
        return np.rot90(img)


def resize(img, width):
    return imutils.resize(img, width=width)


def bright_color(img):
    return cv2.cvtColor(img.astype(np.uint8), cv2.COLOR_BGR2RGB)
