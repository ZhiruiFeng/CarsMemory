#!/usr/bin/env python3
# cvutils.py

"""Some functions for computer vision"""

import cv2
import imutils
import numpy as np
import src.processing.visualutils as vis_util
import os
from PIL import Image


def get_fps(video):
    return video.get(cv2.CAP_PROP_FPS)


def rotate(img):
    height, width, _ = img.shape
    if height > width:
        img = np.rot90(img)
    img_gray = cv2.cvtColor(cv2.cvtColor(img.astype(np.uint8), cv2.COLOR_BGR2RGB), cv2.COLOR_BGR2GRAY)
    up = img_gray[:10]
    down = img_gray[-10:]
    if up.sum() > down.sum():
        return img
    else:
        img = np.rot90(img)
        return np.rot90(img)


def resize(img, width):
    return imutils.resize(img, width=width)


def bright_color(img):
    return cv2.cvtColor(img.astype(np.uint8), cv2.COLOR_BGR2RGB)


def load_image_into_numpy_array(image):
    (im_width, im_height) = image.size
    return np.array(image.getdata()).reshape(
        (im_height, im_width, 3)).astype(np.uint8)


def draw_from_json(json_data, outfolder, bucket):
    tmpfoler = '/tmp/dashcash/'
    url = json_data['storage_link']
    is_keyframe = json_data['is_keyframe']
    name = url.split('/')[-1]
    key = '/'.join(url.split('/')[4:])
    file_name = tmpfoler + name
    bucket.download_file(key, file_name)
    image = Image.open(file_name)
    image_np = load_image_into_numpy_array(image)
    image_np_expanded = np.expand_dims(image_np, axis=0)
    obj_tags = json_data['obj_tags']
    detection_boxes = []
    detection_classes = []
    detection_scores = []
    category_index = {}
    cnt = 0
    if not obj_tags:
        img = Image.fromarray(image_np)
        img.save(outfolder + name)
        return
    for key in obj_tags:
        cnt += 1
        item = obj_tags[key]
        box = [float(item['x1']), float(item['y1']), float(item['x2']), float(item['y2'])]
        detection_boxes.append(box)
        detection_classes.append(cnt)
        detection_scores.append(1.0)
        category_index[cnt] = {'id': cnt, 'name': item['class_name']}
    predict = {}
    predict['detection_boxes'] = np.array(detection_boxes)
    predict['detection_classes'] = np.array(detection_classes)
    predict['detection_scores'] = np.array(detection_scores)
    predict['num_detections'] = cnt
    vis_util.visualize_boxes_and_labels_on_image_array(
        image_np,
        predict['detection_boxes'],
        predict['detection_classes'],
        predict['detection_scores'],
        category_index,
        use_normalized_coordinates=True,
        line_thickness=2,
        skip_scores=True,
        is_keyframe=is_keyframe)
    img = Image.fromarray(image_np)
    img.save(outfolder + name)
    os.remove(file_name)


def images_to_video(images_folder, suffix, out_video_name):
    images = []
    for f in os.listdir(images_folder):
        if f.endswith(suffix):
            images.append(f)

    images.sort()
    # Determine the width and height from the first image
    image_path = os.path.join(images_folder, images[0])
    frame = cv2.imread(image_path)
    height, width, channels = frame.shape

    # Define the codec and create VideoWriter object
    fourcc = cv2.VideoWriter_fourcc(*'mp4v') # Be sure to use lower case
    out = cv2.VideoWriter(out_video_name, fourcc, 3.0, (width, height))

    for image in images:

        image_path = os.path.join(images_folder, image)
        frame = cv2.imread(image_path)

        out.write(frame) # Write out frame to video

        if (cv2.waitKey(1) & 0xFF) == ord('q'): # Hit `q` to exit
            break

    # Release everything if job is finished
    out.release()
    cv2.destroyAllWindows()

    print("The output video is {}".format(out_video_name))
