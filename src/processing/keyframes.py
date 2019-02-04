#!/usr/bin/env python3
# keyframes.py

"""Extract keyframes of annotation for human checking"""

from collections import Counter


def parse_objs(objs):
    """Parse the json returned by object detection model,
    After parsing, the format will fit cassandra schema.
    """
    if not objs:
        return None, Counter()
    cnt = Counter()
    res = {}
    number = 1
    for item in objs:
        obj_class = item['class']
        obj_name = item['name'].split(':')[1]
        obj_name = obj_name.strip()
        cnt[obj_class] += 1
        x1, y1, x2, y2 = item['box']
        new_obj = {'class_name': obj_name,
                   'x1': x1,
                   'y1': y1,
                   'x2': x2,
                   'y2': y2}
        res[number] = new_obj
        number += 1
    return res, cnt


def parse_scene(scenes, max_num):
    """After parsing, the format would be easier to analyze,
    and it will fit cassandra schema.
    """
    if not scenes:
        return None
    res = []
    cnt = 0
    for item in scenes:
        if cnt >= max_num:
            break
        class_name = item['class']
        res.append(class_name)
        cnt += 1
    return res
