# Progress Tests

Here are some tests during the development period for assuring specific processes. And these tests are not unit tests, since we focus on checking the input and output format and the connection of whole pipeline.

* Kafka_communication

It tests that Kafka module, producer, consumer and Cassandra Sinker could work.

* Video

It tests Kafka's configuration could handle large message like video stream.

* Pipeline

It tests for a single video producer, all types of consumers could work as their functions.

* Functional_tests

An informal way of testing whether some fundamental functions in this project is functioning properly.

* Stress Tests

This experiment is to test the ability of this pipeline to handle large throughput, like how many realtime video stream it could connect to, how many FPS in total, how large the throughput could be, and how large the latency could be.

The goal is to find the bottleneck and then refine the architecture.

![Monitor]('../images/monitor_snapshot.png')
