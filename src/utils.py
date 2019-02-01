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


def get_curtimestamp_millis():
    """The form needed by cassandra, is count as milliseconds
    We use this format across the whole Kafka system
    """
    return unix_time_millis(datetime.datetime.now())


def get_date_from_timestamp(dt):
    """Transfer millis timestamp back to date"""
    date = datetime.datetime.fromtimestamp(dt/1000.0)
    return date.strftime("%Y%m%d")


class GeoLocator(object):
    def __init__(self):
        self.locator = Nominatim(user_agent="dashcash")

    def get_address(self, lat, lon):
        location = self.locator.reverse(lat, lon)
        if 'address' in location:
            return location.raw['address']
        else:
            return None
