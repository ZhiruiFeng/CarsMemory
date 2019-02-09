"""Thie function is to check whether the solving records works"""
import sys
sys.path.append("../../")
from src.processing.sampling import VideoSampler


sampler = VideoSampler(5)
folder_key = "dataset/samples-1k/videos/"

sampler.add_video(folder_key)

cnt = 0
while cnt < 100:
    success, image = sampler.read()
    cnt += 1
    if not success:
        break
sampler.release()
