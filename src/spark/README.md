# Spark Streaming Usage

Here we need to consider whether using spark streaming could improve the performance, which we means decease the processing latency and load balance.

![Consumer](../../images/challenge.png)

According to these consumers' functions and positions in this pipeline, we could find these facts:

* Object detector and scene detector don't need to do a lot of computational tasks, since the ML models are deployed on AWS Lambda as API.
* Porter and Librarian also has litter effort on calculation, all it need to do is send command to S3 and for Porter it may have huge network traffic load.


And all above four are not data source specific, which means it doesn't need to be specific to `cam_id`.

Based on these feature, they take little advantage of Spark Streaming. So we just need to use them as multiprocessing, and one advance approach to further wrap them with Kubernetes to realize auto-scaling.

* Extractor has these requirements:
  * Should be data source specific and the frames should be ordered after gathering from different partitions:
    * Python process, could use MinHeap as a buffer sort the incoming data.
    * Spark Streaming's `DStream` don't support sort function, which need transform to `TransformedDStream` first.
  * Need to compare the difference between consecutive two frames:
    * Python could easily do it using for loop.
    * Spark Streaming don't have native support for this function, and the alternative solution is not efficient.

Since source specific, the incoming data rate is slow. According to all these points, we think it's not efficient for the first version to use Spark Streaming. We could just use multiprocessing.

* DB Sinker is not source specific, and its main function is write data in Cassandra and update the statistic of feature's distribution.
  * Spark Streaming's reduceByValue could be very suitable for it, and the database connect could solve the write process well.

So, only for Sinker, using streaming processing framework could have a lot benefits. And using direct connect with Kafka could make the configuring easier, we have no need to consider the number of workers.

For other consumer, using multiprocessing is enough, and if equipped with auto-scaling technology, it will be easy to decide the size of consumer group and start processor. And the performance would be even better than using spark.
