#!/usr/bin/env python3
# keyframes.py

"""Extract keyframes of annotation for human checking"""

from collections import Counter
from src.params import SCENE_NUM


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


def parse_scene(scenes, max_num=SCENE_NUM):
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


def judge_value(msginfo):
    # TODO This part related to the generated report
    return True


def parse_mapper(msginfo, max_num=SCENE_NUM):
    objs = msginfo['objs']
    newobjs, cnt = parse_objs(objs)
    msginfo['objs'] = objs
    msginfo['counts'] = cnt
    scenes = msginfo['scenes']
    newscenes = parse_scene(scenes, max_num)
    msginfo['scenes'] = newscenes
    msginfo['valuable'] = judge_value(msginfo)
    msginfo['is_keyframe'] = True  # Initial it as true
    return msginfo
