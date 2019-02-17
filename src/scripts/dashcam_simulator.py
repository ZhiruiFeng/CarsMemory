"""Dividing data into user files"""
from src.awss3.connector_s3 import S3Connector
from src.params import MY_BUCKET
import sys


def divide_data(folder_key):
    videos_folder = folder_key + 'videos/'
    info_folder = folder_key + 'info/'
    frame_folder = folder_key + 'frame-10s/'
    user_cnt = 100

    connector = S3Connector(MY_BUCKET)
    video_keys = connector.get_objs_keys_with_prefix(videos_folder)
    video_keys.sort()
    size = len(video_keys)
    for i in range(size):
        user_id = i % user_cnt
        if i % 100:
            print(i)
        the_video_key = video_keys[i]
        video_name = the_video_key.split('/')[-1]
        name = video_name.split('.')[0]
        the_info_key = info_folder + name + '.json'
        the_frame_key = frame_folder + name + '.jpg'
        target_folder = 'dataset/dashcam/'
        target_folder_user = target_folder + 'user_' + str(user_id) + '/'

        target_video_key = target_folder_user + 'videos/' + video_name
        target_info_key = target_folder_user + 'info/' + name + '.json'
        target_frame_key = target_folder_user + 'frame-10s/' + name + '.jpg'
        connector.copy_obj_within(the_video_key, target_video_key)
        connector.copy_obj_within(the_info_key, target_info_key)
        connector.copy_obj_within(the_frame_key, target_frame_key)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage producer_starter.py <s3_folder_key>")
        exit(-1)
    if str(sys.argv[2])[-1] != '/':
        print("The second parameter should be a s3 foldr key, end with '/'")
        exit(-1)
    s3_folder_key = str(sys.argv[2])
    divide_data(s3_folder_key)
