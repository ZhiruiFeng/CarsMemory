from cassandra.cluster import Cluster
import pandas as pd
import datetime
import json


class dbManager(object):
    """Used to get data from cassandra"""

    def __init__(self):
        self.cluster = Cluster(['ec2-52-38-20-47.us-west-2.compute.amazonaws.com'], port='9042')
        self.session = self.cluster.connect('playground')

    def search_statistic(self):
        command = "SELECT json * FROM statistic"
        try:
            query_result = self.session.execute(command)
        except:
            print("Query executation error.")
        dict_res = []
        for row in query_result:
            dict_res.append(json.loads(row[0]))
        return dict_res

    def dict_to_df(self, query_result):
        df = pd.DataFrame.from_dict(query_result)
        return df

    def get_statistic(self):
        query_result = self.search_statistic()
        statistic = self.dict_to_df(query_result)
        return statistic
