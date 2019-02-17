import sys
sys.path.append('../../')

from src.awss3.reader_s3 import S3VideoReader
from src.processing.objdetector import detect_object_imglists
import time
from collections import defaultdict


reader = S3VideoReader('dashcash')
urls = reader.get_urls_in_folder("store/20190215/1")

sizes = [1, 2, 4, 8]
experiment_times = 50

ans = defaultdict(list)

for size in sizes:
    for i in range(experiment_times):
        imglist = [urls[i]]
        start_time = time.time()
        res = detect_object_imglists(imglist)
        spend_time = time.time() - start_time
        print(spend_time)
        if res:
            ans[str(size)].append(spend_time)
        else:
            ans[str(size)].append(0)

    print("########## Size {}".format(size))
    if all(ans[str(size)]):
        print(ans[str(size)])
        print(sum(ans[str(size)])/float(experiment_times))
        print(sum(ans[str(size)])/(float(experiment_times)*size))
    else:
        print("Invalid")
        print(ans[str(size)])


############
# Result
############

# The average latency is 6.6s per image.
