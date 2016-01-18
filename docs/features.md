FEATURES
========

Scribengin exists to provide an easily configurable, general solution to the problem of moving big data in a reliable, highly available way.  Scribengin is a cutting edge product built on top of already reliable technologies to provide a reliable, flexible, and scalable solution.

###Flexibility
- Support for many different kinds of data sources/sinks
- Sources and sinks can easily be expanded to add support for new data sources/sinks
- Can move data from any source to any sink
- Custom Processors - filter, transform, copy data within the pipeline
- Out-the-box support for
  - HDFS
  - Kafka
  - S3

###Scalability
- Built with YARN
- Easily configure how many nodes to work on any given dataflow
- Run multiple dataflows simultaneously
- Chain dataflows together
- Scalable, Expandable, and Highly Available

###Reliability 
- Data is guaranteed to not be lost, and duplication kept to a minimum
- Automatically replace nodes in the cluster that unexpectedly fail
- Dataflows will resume upon unexpected failure
- Dataflows can be paused, stopped, resumed by an administrator
- Reliable Kafka writer - improvements made to the default Kafka writer to support overcoming Kafka failures
- Configuration is stored in a central, highly available place for all nodes in the cluster (based on Zookeeper)

###Visibility
- Graphical, real time metrics monitoring using Kibana
- Logs stored in an expandable, highly available repository (elasticsearch)

###Testability
- Can be deployed to simulate a working cluster locally using Docker images for testing or development
- Can be deployed in a single JVM for unit testing



