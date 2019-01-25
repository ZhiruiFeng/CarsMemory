# KEYSPACE CREATION
CREATE KEYSPACE dashcash WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

# ENTER dashcash
USE dashcash

# TYPE tag CREATION
CREATE TYPE object (
    class text,
    x1 int,
    y1 int,
    x2 int,
    y2 int
);

# TABLE frame CREATION
CREATE TABLE frame (
    dashcam_id text,
    date text,
    store_time timestamp,
    is_keyframe boolean,
    storage_link text,
    obj_tags map<int, frozen<object>>,
    PRIMARY KEY ((dashcam_id, data), store_time)
);

# SAMPLE INSERT
INSERT INTO frame (dashcam_id, date, store_time, storage_link, obj_tags)
values (
    'test_cam',
    '2019-01-25',
    '2019-01-25 02:21:00',
    'http://test.com',
    {1:{class:'car', x1:0, y1:0, x2:100, y2:100}}
)

# ADD obj_tags
UPDATE frame SET obj_tags = obj_tags + {2:{class:'person', x1:10, y1:10, x2:90, y2:90}}
WHERE dashcam_id='test_cam'
AND date='2019-01-25'
AND store_time = '2019-01-25 02:21:00';
