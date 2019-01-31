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


def detect_object(img_address):
    url = OBJ_API_URL
    data = {"imageUrls": [img_address]}
    json_data = json.dumps(data)
    response = requests.post(url=url, data=json_data)
    res = json.loads(response.text)
    return res['entries'][img_address]
