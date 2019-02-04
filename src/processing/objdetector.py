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
    """Example output format
    { "entries": {"https://assets.regus.com/images/nwp/gen-interior-office-01-404x192.jpg":
                     [{ "name": "64: potted plant", "probability": 78.450584,
                        "box": [ 0.117765754, 0.23262283, 0.5129187, 0.3484358]},
                      { "name": "1: person", "probability": 51.46939,
                        "box": [ 0.08490905, 0.43873444, 0.98695076, 0.7388295]},
                      { "name": "86: vase", "probability": 49.7411,
                        "box": [ 0.40540102, 0.25193954, 0.50996864, 0.30689353]},
                      { "name": "62: chair", "probability": 48.952774,
                        "box": [ 0.3945906, 0.54693127, 0.9546944, 0.7233542]}
                    ]
                }}
    """
    print('[Obj] ' + img_address)
    url = OBJ_API_URL
    data = {"imageUrls": [img_address]}
    json_data = json.dumps(data)
    response = requests.post(url=url, data=json_data)
    res = json.loads(response.text)
    if 'entries' in res:
        return res['entries'][img_address]
    else:
        print('[Obj] Data loss for {}:{}'.format(img_address, res))
        return None
