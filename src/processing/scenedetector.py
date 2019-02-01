"""
We need this module to find the scene in the picture.
The model planed to deploy on Lambda with API Gateway.
"""

import requests
import json
from src.params import SCENE_API_URL


def detect_scene(img_address):
    url = SCENE_API_URL
    data = {"imageUrls": [img_address]}
    json_data = json.dumps(data)
    response = requests.post(url=url, data=json_data)
    res = json.loads(response.text)
    return res['entries'][img_address]
