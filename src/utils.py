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
        self.address = None
        self.us_state_abbrev = {'Alabama': 'AL',
                                'Alaska': 'AK',
                                'Arizona': 'AZ',
                                'Arkansas': 'AR',
                                'California': 'CA',
                                'Colorado': 'CO',
                                'Connecticut': 'CT',
                                'Delaware': 'DE',
                                'Florida': 'FL',
                                'Georgia': 'GA',
                                'Hawaii': 'HI',
                                'Idaho': 'ID',
                                'Illinois': 'IL',
                                'Indiana': 'IN',
                                'Iowa': 'IA',
                                'Kansas': 'KS',
                                'Kentucky': 'KY',
                                'Louisiana': 'LA',
                                'Maine': 'ME',
                                'Maryland': 'MD',
                                'Massachusetts': 'MA',
                                'Michigan': 'MI',
                                'Minnesota': 'MN',
                                'Mississippi': 'MS',
                                'Missouri': 'MO',
                                'Montana': 'MT',
                                'Nebraska': 'NE',
                                'Nevada': 'NV',
                                'New Hampshire': 'NH',
                                'New Jersey': 'NJ',
                                'New Mexico': 'NM',
                                'New York': 'NY',
                                'North Carolina': 'NC',
                                'North Dakota': 'ND',
                                'Ohio': 'OH',
                                'Oklahoma': 'OK',
                                'Oregon': 'OR',
                                'Pennsylvania': 'PA',
                                'Rhode Island': 'RI',
                                'South Carolina': 'SC',
                                'South Dakota': 'SD',
                                'Tennessee': 'TN',
                                'Texas': 'TX',
                                'Utah': 'UT',
                                'Vermont': 'VT',
                                'Virginia': 'VA',
                                'Washington': 'WA',
                                'West Virginia': 'WV',
                                'Wisconsin': 'WI',
                                'Wyoming': 'WY'}

    def get_address(self, lat, lon):
        query = (lat, lon)
        location = self.locator.reverse(query)
        if 'address' in location.raw:
            self.address = location.raw['address']

    def get_state(self, lat, lon):
        self.get_address(lat, lon)
        if self.address and self.address['country_code'] == 'us':
            fullname = self.address['state']
            return self.us_state_abbrev[fullname]
        else:
            return None
