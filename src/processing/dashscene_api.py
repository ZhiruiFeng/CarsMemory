"""
Here wraps the scene detection NN modle as an API.
The ML modle refers to MIT Places365-CNNs for Scene Classification
https://github.com/CSAILVision/places365
"""

from boto.s3.key import Key
from boto.s3.connection import S3Connection
from flask import Flask
from flask import request
from flask import json

import torch
from torch.autograd import Variable as V
import torchvision.models as models
from torchvision import transforms as trn
from torch.nn import functional as F
import os
from PIL import Image
import time


BUCKET_NAME = 'dashcash'
MODEL_KEY = 'lambda/resnet18_places365.pth.tar'
MODEL_LOCAL_PATH = 'resnet18_places365.pth.tar'
LABEL_KEY = 'lambda/categories_places365.txt'
LABEL_LOCAL_PATH = 'categories_places365.txt'

app = Flask(__name__)


@app.route('/', methods=['POST'])
def index():
    request_info = json.loads(request.get_data().decode('utf-8'))
    print(request_info)
    res = predict(request_info['imageUrl'])
    print(res)
    return json.dumps(res)


def predict(image_url):
    # Load model
    conn = S3Connection()
    bucket = conn.get_bucket(BUCKET_NAME)
    key_label = Key(bucket)
    key_label.key = LABEL_KEY
    file_name = LABEL_LOCAL_PATH
    if not os.access(file_name, os.W_OK):
        key_label.get_contents_to_filename(file_name)
    key_model = Key(bucket)
    key_model.key = MODEL_KEY
    model_file = MODEL_LOCAL_PATH
    if not os.access(model_file, os.W_OK):
        key_model.get_contents_to_filename(model_file)
    model = models.__dict__['resnet18'](num_classes=365)
    checkpoint = torch.load(model_file, map_location=lambda storage, loc: storage)
    state_dict = {str.replace(k, 'module.', ''): v for k, v in checkpoint['state_dict'].items()}
    model.load_state_dict(state_dict)
    model.eval()

    # load the image transformer
    centre_crop = trn.Compose([
        trn.Resize((256, 256)),
        trn.CenterCrop(224),
        trn.ToTensor(),
        trn.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ])

    classes = list()
    with open(file_name) as class_file:
        for line in class_file:
            classes.append(line.strip().split(' ')[0][3:])
    classes = tuple(classes)

    img_name = str(int(time.time()*1000)) + '.jpg'
    os.system('wget -O ' + img_name + ' ' + image_url)
    img = Image.open(img_name)
    input_img = V(centre_crop(img).unsqueeze(0))
    logit = model.forward(input_img)
    h_x = F.softmax(logit, 1).data.squeeze()
    probs, idx = h_x.sort(0, True)

    res = {}
    predictlist = []
    for i in range(0, 3):
        scene = {}
        scene['class_name'] = classes[idx[i]]
        scene['probs'] = float(probs[i])
        predictlist.append(scene)
    res['prediction'] = predictlist
    return res
