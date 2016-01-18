
Scribengin
==========

#Contents#
1. [Overview](#overview)
2. [The Problem](#the-problem)
3. [The Scribengin Solution](#the-scribengin-solution)
4. [Feature List](#feature-list)
5. [Terminology Guide](#terminology-guide)
6. [Quickstart Guide](#quickstart-guide)
7. [How to launch Scribengin in any cluster](#how-to-launch-a-cluster-in-an-arbitrary-environment-ie-in-house-cluster-aws-etc)
5. [Developer Info](#developer-info)
6. [Developer Guidelines](#developer-guidelines)
6. [Releasing Code](#release)

####Overview
Pronounced "Scribe Engine" 

Scribengin is a highly reliable (HA) and performant event/logging transport that registers data under defined schemas in a variety of end systems.  Scribengin enables you to have multiple flows of data from a source to a sink. Scribengin will tolerate system failures of individual nodes and will do a complete recovery in the case of complete system failure.

Reads and writes data from sources/sinks:
- Kafka
- HDFS
- S3


####The Problem
The core problem is how to reliably and at scale have a distributed application write data to multiple destination data systems.  This requires the ability to todo data mapping, partitioning with optional filtering to the destination system.

####The Scribengin Solution:
A distributed, highly reliable ETL system that can handle multiple sources and sinks
![Scribengin](docs/images/ScribeIntro.png "Scribengin")




###Feature List

- [A high level overview of what Scribengin can do](docs/features.md)

####Terminology Guide
Learn the Scribengin terminology, and get acquainted with Scribengin at a high level.

- [Terminology Guide](docs/terminology.md)

####Quickstart Guide
Get Scribengin launched quickly!  This will walk you how to start a Scribengin instance in YARN.

- [QuickStart Guide](docs/deployment/scribengin-cluster-setup-quickstart.md)


####How to launch a cluster in an arbitrary environment (i.e. in-house cluster, AWS, etc)

- [Arbitrary Cluster Guide](docs/deployment/arbitrary-cluster-guide.md)


####Developer Info
- [Get your dev environment setup, learn to write a dataflow](docs/dataflowDevelopment/dataflowDevTableOfContents.md)
- [Operator Development Guide](docs/dataflowDevelopment/operator-dev-guide.md)

####Developer Guidelines
- [Code Conventions](docs/devAndTestingGuidelines/code-convention-howto.md)
- [Code Organization](docs/devAndTestingGuidelines/code-organization-howto.md)
- [Dataflow Performance and Validation](docs/devAndTestingGuidelines/dataflow-performance-and-validation-howto.md)

####Release
```
cd NeverwinterDP
./gradlew clean build release -x test

#Code and jars will be released to NeverwinterDP/release/build/release/
```


  