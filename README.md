#NeverwinterDP

Scribengin
==========
Pronounced "Scribe Engine" 

Scribengin is a highly reliable (HA) and performant event/logging transport that registers data under defined schemas in a variety of end systems.  Scribengin enables you to have multiple flows of data from a source to a sink. Scribengin will tolerate system failures of individual nodes and will do a complete recovery in the case of complete system failure.

Reads and writes data from sources/sinks:
- Kafka
- HDFS
- S3


The Problem
======
The core problem is how to reliably and at scale have a distributed application write data to multiple destination data systems.  This requires the ability to todo data mapping, partitioning with optional filtering to the destination system.

Status

Definitions
======

- A **Dataflow** - is data being moved from a single source to a single sink
- **Source** - is a system that is being read to get data from (Kafka, Kinesis e.g.)
- **Sink** - is a destination system that is being written to (HDFS, Hbase, Hive e.g.)


Technologies Used
======
- Built in Java
- YARN
- Zookeeper
- Kafka, S3, HDFS - data sources/sinks


Design
======
![Scribengin Cluster Design](/ScribenginCluster.png "Scribengin Cluster Design")