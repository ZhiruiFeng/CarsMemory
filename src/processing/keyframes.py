#!/usr/bin/env python3
# keyframes.py

"""Extract keyframes of annotation for human checking"""

from collections import Counter


def parse_objs(objs):
    cnt = Counter()
    res = {}
    number = 1
    for item in objs:
        obj_class = item['class']
        obj_name = item['name'].split(':')[-1]
        obj_name = obj_name.strip()
        cnt[obj_class] += 1
        x1, y1, x2, y2 = item['box']
        new_obj = {'class': obj_class,
                   'x1': x1,
                   'y1': y1,
                   'x2': x2,
                   'y2': y2}
        res[number] = new_obj
        number += 1
    return res, cnt
