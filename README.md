
Scribengin 
==========
Pronounced "Scribe Engine" 

Scribengin is a highly reliable (HA) and performant event/logging transport that registers data under defined schemas in a variety of end systems.  Scribengin enables you to have multiple flows of data from a source to a sink. Scribengin will tolerate system failures of individual nodes and will do a complete recovery in the case of complete system failure.

Reads and writes data from sources/sinks:
- Kafka
- HDFS
- S3



####The Problem
The core problem is how to reliably and at scale have a distributed application write data to multiple destination data systems.  This requires the ability to todo data mapping, partitioning with optional filtering to the destination system.



####Definitions

- A **Dataflow** - is data being moved from a single source to a single sink
- **Source** - is a system that is being read to get data from (Kafka, Kinesis e.g.)
- **Sink** - is a destination system that is being written to (HDFS, Hbase, Hive e.g.)
- **Scribe** - is a data processor used to filter, copy, duplicate, transform, etc. any data moving between source and sink


####Technologies Used
- Built in Java
- Gradle
- YARN
- Zookeeper
- Elasticsearch
- Kafka, S3, HDFS - data sources/sinks


####Design
![Scribengin Cluster Design](/docs/ScribenginCluster.png "Scribengin Cluster Design")


####Dataflow Submission
![Scribengin Dataflow Submission Design](/docs/dataflowsubmission.png "Scribengin Dataflow Submission Design")


####Developer Setup
```
git clone https://github.com/Nventdata/NeverwinterDP/
git checkout dev/master
cd NeverwinterDP
./gradlew clean build install eclipse  -x test

#Now you can import the project into Eclipse
```

####Release
```
cd NeverwinterDP/release
../gradlew clean build release -x test
```


####Developer Notes on Scribengin modules
- scribengin.core
  - Core scribengin code
  - Scribe, Dataflows, VM Master, Dataflow Masters
- lib.yara
  - Yara is our framework to capture metrics
- module.*
  - Mock/test modules to set up servers for unit testing
  - Used to for testing Kafka, elasticsearch, etc
- scribengin.release
  - Code to release Scribengin for deployment
- registry.*
  - Registry is the central place for configurations, inter-node communication, and status tracking
  - Based on Zookeeper
  
