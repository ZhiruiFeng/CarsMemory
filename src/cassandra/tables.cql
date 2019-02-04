# Cassandra Schema for DashCash project.

# KEYSPACE CREATION
CREATE KEYSPACE dashcash WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2};

# ENTER dashcash
USE dashcash

# TYPE tag CREATION
CREATE TYPE object (
    class_name text,
    x1 float,
    y1 float,
    x2 float,
    y2 float
);

# TABLE frame CREATION
CREATE TABLE frames (
    dashcam_id text,
    store_date text,
    store_time timestamp,
    is_keyframe boolean,
    storage_link text,
    obj_tags map<int, frozen<object>>,
    sceces list<text>,
    PRIMARY KEY ((dashcam_id, store_date), store_time)
);

# SAMPLE INSERT
INSERT INTO frame (dashcam_id, store_date, store_time, is_keyframe, storage_link, obj_tags)
values (
    'test_cam',
    '20190201',
    '1549015332929',
    False,
    'http://test.com',
    {1:{class_name:'car', x2:0.1, y1:0.1, x1:100.0, y2:100.0}}
)

# ADD obj_tags
UPDATE frame SET obj_tags = obj_tags + {2:{class:'person', x1:10, y1:10, x2:90, y2:90}}
WHERE dashcam_id='test_cam'
AND date='2019-01-25'
AND store_time = '2019-01-25 02:21:00';
