"""
We need this module to find the scene in the picture.
The model planed to deploy on Lambda with API Gateway.
"""

import requests
import json
from src.params import SCENE_API_URL, ALGORITHMIA_KEY
import Algorithmia


def detect_scene(img_address):
    """The self-deployed API on AWS Lambda, see dashscene_api.py
    Currently unable to use due to some AWS permission issues.
    """
    print("[Scene] " + img_address)
    url = SCENE_API_URL
    data = {"imageUrl": img_address}
    json_data = json.dumps(data)
    try_index = 0
    while try_index < 5:
        response = requests.post(url=url, data=json_data)
        res = json.loads(response.text)
        if 'prediction' in res:
            return res['entries'][img_address]
        try_index += 1
        time.sleep(try_index * 0.1)
    print('[Scene] Data loss for {}:{}'.format(img_address, res))
    return None


def detect_scene_algorithmia(img_address):
    """Example return format:
    {'predictions': [{'class': 'crosswalk', 'prob': 0.48030322790145874},
                     {'class': 'downtown', 'prob': 0.1904478818178177},
                     {'class': 'street', 'prob': 0.1708705723285675},
                     {'class': 'plaza', 'prob': 0.04097772017121315},
                     {'class': 'fountain', 'prob': 0.022315144538879395}]}
    """
    print("[Scene] " + img_address)
    input = {"image": img_address}
    client = Algorithmia.client(ALGORITHMIA_KEY)
    algo = client.algo('deeplearning/Places365Classifier/0.1.9')
    try_index = 0
    while try_index < 5:
        res = algo.pipe(input).result
        if "predictions" in res:
            return res
        try_index += 1
        time.sleep(try_index * 0.1)
    print('[Scene] Data loss for {}:{}'.format(img_address, res))
    return None
