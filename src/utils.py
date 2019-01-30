#!/usr/bin/env python3
# utils.py

import datetime
from geopy.geocoders import Nominatim


def unix_time(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()


def unix_time_millis(dt):
    return int(unix_time(dt) * 1000.0)


def get_curtimestamp_cassandra(dt):
    """The form needed by cassandra"""
    return unix_time_millis(datetime.datetime.now())


class GeoLocator(object):
    def __init__(self):
        self.locator = Nominatim(user_agent="dashcash")

    def get_address(self, lat, lon):
        location = self.locator.reverse(lat, lon)
        if 'address' in location:
            return location.raw['address']
        else:
            return None
