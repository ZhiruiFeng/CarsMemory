# Explanation of Starters


**Topic Starter**

Start the public topics like `org`, `url`, `value`. They are corresponding to the original image frames, the S3 key got after porter's delivery, and all meta data got after extractor's parsing and now waiting to be wrote into database.

**Consumer Starter**

Only start Porter, Detector and Librarian in multiprocessing form. Since these consumer are Daemons for the whole cluster's throughput. Advance step will consider using Kubernetes.

**Sinker Starter**

 Sinker currently need to start manually, later we will consider using Spark Streaming.

**Extractor Starter**

Being created only when new camera connect to the system as input.

Should first the topic for extractor, which is related to the camera_id, and extractor will mainly consume data from that specific topic and send result to `value`.

**Producer Starter**

It will related to camera, and here for testing we sent videos from the whole folder.

**Camera Starter**

Since both of Producer and Extractor are related to camera, if all modules run properly, we could combine them together.
