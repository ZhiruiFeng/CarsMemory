#!/bin/bash
/usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2,com.datastax.spark:spark-cassandra-connector_2.11:2.3.2,anguenot:pyspark-cassandra:0.9.0 --py-files config.py --conf spark.cassandra.connection.host=ec2-52-38-20-47.us-west-2.compute.amazonaws.com --executor-memory 4500m --driver-memory 5500m sink.py
