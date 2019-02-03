"""
The model is deployed on AWS Lambda, accompied with API Gateway.
Designed as API server is to decouple the computation consuming
part out of our system cluster and have auto-scalling ability to
decrease high latency time in popular hour.

Model used: TensorFlow Object Detection Model
https://github.com/tensorflow/models/tree/master/research/object_detection
"""

import requests
import json
from src.params import OBJ_API_URL
import time


def detect_object(img_address):
    """Since porter could be late, so we try 5 time per request"""
    print(img_address)
    url = OBJ_API_URL
    data = {"imageUrls": [img_address]}
    json_data = json.dumps(data)
    try_index = 0
    while try_index < 5:
        response = requests.post(url=url, data=json_data)
        res = json.loads(response.text)
        if 'entries' in res:
            return res['entries'][img_address]
        try_index += 1
        time.sleep(try_index)
    print('Data loss for {}:{}'.format(img_address, res))
    return None
