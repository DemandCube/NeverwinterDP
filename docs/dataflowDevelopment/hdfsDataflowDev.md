HDFS Dataflow Development
=========================


- [Introduction](#introduction)
- [Sample Code](#sample-code)
- [A Kafka to HDFS Dataflow](#a-kafka-to-hdfs-dataflow)
  - [Overview](#overview)  
  - [Partitioning](#partitioning)
  - [Important Code Differences](#important-code-differences)
- [The Dataflow Submitter](#the-dataflow-submitter)
- [Writing a Unit Test](#writing-a-unit-test)
- [Launching in a real Scribengin Cluster](#launching-in-a-real-scribengin-cluster)

#Introduction#

This howto will show you how to develop your own dataflow and to push data from Kafka to HDFS.  

#Sample code#
You can find sample code in the Scribengin package com.neverwinterdp.scribengin.dataflow.example.*. The code comes complete with unit tests and full comments.

#A Kafka to HDFS Dataflow

##Overview

This example is almost idential to creating a dataflow that pushes data from Kafka to Kafka.  We'll even be using the same DataStreamOperator!

The diagram below shows how this dataflow will string together.

- The input is a Kafka topic called "input.topic"
- An operator reads this topic and pushes the data into the configured output data set
- The output will be HDFS

![HDFS Dataflow Example](../images/API_Example_Simple_HDFS.png "Simple HDFS Dataflow Example")

##Partitioning

We'll be setting our default parallelism to 8, so our operator will create 8 partitions in HDFS.  Our default location for writing data is set to ```build/working/storage/hdfs/output```, so our final output will have a file structure similar to this in HDFS

```
hdfs://{homedir}/build/working/storage/hdfs/output
├── partition-0
│   └── segment-000001.dat
├── partition-1
│   └── segment-000001.dat
├── partition-2
│   └── segment-000001.dat
├── partition-3
│   └── segment-000001.dat
├── partition-4
│   └── segment-000001.dat
├── partition-5
│   └── segment-000001.dat
├── partition-6
│   └── segment-000001.dat
└── partition-7
│   └── segment-000001.dat
│   └── segment-000002.dat
```

##Important Code Differences 

The main changes we'll have to implement are setting two new properties:
```java
    //Where in the registry to store HDFS info (Not suggested to change this)
    HDFSRegistryPath = props.getProperty("dataflow.hdfsRegistryPath", "/storage/hdfs/output");
    
    //Where in HDFS to store our data
    HDFSLocation = props.getProperty("dataflow.hdfsLocation", "build/working/storage/hdfs/output");
```

We'll use these properties to define our HDFS output DataSet.
```java
    //Our output sink will be HDFS
    //"output" - the dataset's name
    //HDFSRegistryPath - where in the registry to HDFS config
    //HDFSLocation - the path in HDFS to put our data
    DataSet<Message> outputDs = dfl.createOutput(new HDFSStorageConfig("output", hdfsLocation));

```


#The Dataflow Submitter
Full code can be found [here](https://github.com/Nventdata/NeverwinterDP/blob/master/scribengin/dataflow/example/src/main/java/com/neverwinterdp/scribengin/dataflow/example/hdfs/ExampleHdfsDataflowSubmitter.java)


#Writing a Unit Test

The main difference to note in this unit test is how we do the verification of our data stored in HDFS.  We use an internal Scribengin class to be able to read our partitioned data easily.  Study the function ```readDirsRecursive()``` for how it works.  Full code can be found [here](https://github.com/Nventdata/NeverwinterDP/blob/master/scribengin/dataflow/example/src/test/java/com/neverwinterdp/scribengin/dataflow/example/hdfs/ExampleHdfsDataflowSubmitterTest.java)


```java
  /**
   * Use our HDFSSource to read our data through all partitions
   * @param fs HDFS File system
   * @param hdfsPath Path our data is saved to
   * @return count of records in HDFS
   * @throws Exception
   */
  private int readDirsRecursive(FileSystem fs, String hdfsPath) throws Exception{
    int count = 0;
    
    //Configure our HDFS storage object
    HDFSStorageConfig storageConfig = new HDFSStorageConfig("output", hdfsPath);
    HDFSStorage storage = new HDFSStorage(registry, fs, storageConfig);
    
    //Get our source object from the storage object
    HDFSSource source = storage.getSource();
    //Get all source streams
    SourcePartitionStream[] sourceStream = source.getLatestSourcePartition().getPartitionStreams();
    //Read from each individual source stream
    for(int i = 0; i < sourceStream.length; i++) {
      SourcePartitionStreamReader reader = sourceStream[i].getReader("reader-for-stream-" + i);
      Message message = null;
      //Count the number of messages
      while((message = reader.next(3000)) != null) {
        count++;
      }
      reader.close();
    }
    
    return count;
  }
```


##Launching in a real Scribengin Cluster

The process of submitting to a live cluster is the same as in the simple dataflow's example.  Specific parameters may be required, but otherwise the process is the same. 

Please refer to [the simple dataflow development guide](simpleDataflowDev.md#launching-in-a-real-scribengin-cluster)