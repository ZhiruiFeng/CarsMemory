# Models

This system decouples each processing module as independent unit using Kafka as a message bus for data's transmitting. This structure could avoid data loss, and make the system more flexible for embedding diverse ML models and algorithms for application, which could facilitate the landing of AI/ML functions for data scientist.

Here includes some existing ML models our system uses to generate auto-annotation meta-data.

## Object Detection

* YOLO

The GitHub repo could be found [here](https://github.com/cfotache/pytorch_objectdetecttrack)
