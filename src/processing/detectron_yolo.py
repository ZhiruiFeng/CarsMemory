from models.yolo.models import *
from models.yolo.utils import *

import cv2
import os, sys, time, datetime, random
import torch
import numpy as np
from torch.utils.data import DataLoader
from torchvision import datasets, transforms
from torch.autograd import Variable

import matplotlib.pyplot as plt
import matplotlib.patches as patches
from PIL import Image

from models.yolo.sort import *


class YoloDetectron():

    def __init__(self):

        # Basical setting related to the YOLO mdoel
        self.config_path = '/home/ubuntu/workspace/CarsMemory/models/yolo/config/yolov3.cfg'
        self.weights_path = '/home/ubuntu/workspace/CarsMemory/models/yolo/config/yolov3.weights'
        self.class_path = '/home/ubuntu/workspace/CarsMemory/models/yolo/config/coco.names'
        self.img_size = 416
        self.conf_thres = 0.8
        self.nms_thres = 0.4

        # Load model and weights
        self.model = Darknet(self.config_path, img_size=self.img_size)
        self.model.load_weights(self.weights_path)
        self.model.eval()
        self.classes = utils.load_classes(self.class_path)
        self.Tensor = torch.FloatTensor

        # About image color
        self.cmap = plt.get_cmap('tab20b')
        self.colors = [self.cmap(i)[:3] for i in np.linspace(0, 1, 20)]
        self.mot_tracker = Sort()

    def _detect_image(self, img):
        # scale and pad image
        ratio = min(self.img_size/img.size[0], self.img_size/img.size[1])
        imw = round(img.size[0] * ratio)
        imh = round(img.size[1] * ratio)
        img_transforms = transforms.Compose([transforms.Resize((imh, imw)),
             transforms.Pad((max(int((imh-imw)/2),0), max(int((imw-imh)/2),0), max(int((imh-imw)/2),0), max(int((imw-imh)/2),0)),
                            (128, 128, 128)),
             transforms.ToTensor(),
             ])
        # convert image to Tensor
        image_tensor = img_transforms(img).float()
        image_tensor = image_tensor.unsqueeze_(0)
        input_img = Variable(image_tensor.type(self.Tensor))
        # run inference on the model and get detections
        with torch.no_grad():
            detections = self.model(input_img)
            detections = utils.non_max_suppression(detections, 80)
        return detections[0]

    def processing(self, frame):
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        pilimg = Image.fromarray(frame)
        detections = self._detect_image(pilimg)

        img = np.array(pilimg)
        pad_x = max(img.shape[0] - img.shape[1], 0) * (self.img_size / max(img.shape))
        pad_y = max(img.shape[1] - img.shape[0], 0) * (self.img_size / max(img.shape))
        unpad_h = self.img_size - pad_y
        unpad_w = self.img_size - pad_x
        if detections is not None:
            tracked_objects = self.mot_tracker.update(detections.cpu())

            # unique_labels = detections[:, -1].cpu().unique()
            # n_cls_preds = len(unique_labels)
            for x1, y1, x2, y2, obj_id, cls_pred in tracked_objects:
                box_h = int(((y2 - y1) / unpad_h) * img.shape[0])
                box_w = int(((x2 - x1) / unpad_w) * img.shape[1])
                y1 = int(((y1 - pad_y // 2) / unpad_h) * img.shape[0])
                x1 = int(((x1 - pad_x // 2) / unpad_w) * img.shape[1])

                color = self.colors[int(obj_id) % len(self.colors)]
                color = [i * 255 for i in color]
                cls = self.classes[int(cls_pred)]
                cv2.rectangle(frame, (x1, y1), (x1+box_w, y1+box_h), color, 4)
                cv2.rectangle(frame, (x1, y1-35), (x1+len(cls)*19+60, y1), color, -1)
                cv2.putText(frame, cls + "-" + str(int(obj_id)), (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 1, (255,255,255), 3)
        return frame
