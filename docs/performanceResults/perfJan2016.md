Scribengin Performance January 2016
===================================
#Contents#
1. [Methodology](#methodology)
2. [Kafka Results](#kafka-results)
3. [Kakfa to Kafka Conclusions](#kafka-to-kafka-conclusions)
4. [HDFS Results](#hdfs-results)
5. [Kafka to HDFS Conclusions](#kafka-to-hdfs-conclusions)


#Methodology#

##Dataflow
The dataflow we'll be using to test Scribengin will be our splitter dataflow that's been enhanced for tracking messages.  This way we'll be able to test a complex, wired dataflow and also be able to validate all messages have been processed.

This dataflow will simulate a real case scenario for Scribengin - parsing logs.  Every record passed through contains data as well as a log level.  The log levels are INFO, WARNING, and ERROR.  The logs are split up by level by Scribengin, then each log level is processed individually to move into final storage.

In this test, we'll be moving data from Kafka, into intermediary Kafka topics, and finally moving the data into a final sink.  The final sinks tested are **Kafka** and **HDFS**.  

Scribengin will need to manage moving data between 5 topics with 8 partitions each.

All tests are run in AWS.

##Description

![Dataflow](../images/Splitter.png)

The **Data Generator** is responsible for generating data into Kafka.  The data is written into 8 partitions into a single topic.

The **Splitter Operator** reads data in from the **Input** Kafka topic, parses what the log level is, and moves the data into the corresponding Kafka topic.

The **Persister Operators** reads data from their corresponding topic and moves the data into the **Aggregate** Kafka topic.

Finally, the **Data Validator** reads the data in from the **Aggregate** Kafka topic and ensures all records have been successfully moved.

##Software Configurations
<table>
<tr> <td><b>Dataflow Conf</b></td> 
<td>
<pre>Number of Workers     8  
Executors per Worker  2
Default Parallelism   8 </pre> </td> </tr>

<tr><td><b>Kafka Conf</b></td> 
<td>
<pre>num.network.threads          5       
num.io.threads               8
socket.send.buffer.bytes     1048576
socket.receive.buffer.bytes  1048576
socket.request.max.bytes     104857600
default.replication.factor   2
log.segment.bytes            400000000
</pre>
</td></tr>

<tr><td><b>HDFS Conf</b></td> 
<td>
<pre>dfs.replication   2
</pre>
</td></tr>

<tr><td><b>YARN Conf</b></td>
<td>
<pre>
yarn.nodemanager.resource.cpu_vcores      3      
yarn.nodemanager.resource.memory_mb       3072  
yarn.nodemanager.vmem-pmem-ratio          3     
yarn.nodemanager.vmem-check-enabled       true  
yarn.scheduler.minimum-allocation-mb      614   
yarn.scheduler.maximum-allocation-mb      614   
yarn.scheduler.minimum-allocation-vcores  1     
yarn.scheduler.maximum-allocation-vcores  1     
</pre>
</td>

</tr>
</table>


##AWS Configuration

All AWS containers are configured to use EBS provisioned IOP (SSD) volumes. 

| Machine  | vCPUs | RAM  |
| -------- | ----  | ---- |
| m4.xlarge | 4     | 16 GB |
| m4.large | 2     | 8 GB |
| t2.medium | 2     | 4 GB |
| t2.small | 1     | 2 GB |

##Cluster Configuration

All clusters use the following configurations in addition to number/configuration of Hadoop Workers and Kafka brokers.

| Role          | Type      | Num Of Instances     |
| ------------- | --------- | -------------------- |
| Hadoop-Master | t2.medium |  1                   |
| Zookeeper     | t2.small  |  1                   |
| Elasticsearch | t2.small  |  1                   |

#KAFKA Results

|Hardware/Software                                          | Records/sec     | Bytes/sec   |
| --------------------------------------------------------- | --------------- |-------------|
| 3x **m4.large** Hadoop-Worker <br> 5x **t2.medium** Kafka | 13,500          | 11,500,000  |
| 3x **m4.large** Hadoop-Worker <br> 5x **m4.large** Kafka  | 17,000          | 15,000,000  |
| 4x **m4.large** Hadoop-Worker <br> 5x **m4.large** Kafka  | 21,000          | 20,500,000  |
| 5x **m4.large** Hadoop-Worker <br> 5x **m4.large** Kafka  | 26,200          | 23,000,000  |




#HDFS Results

|Hardware/Software                                          | Records/sec     | Bytes/sec   |
| --------------------------------------------------------- | --------------- |-------------|
| 3x **m4.large** Hadoop-Worker <br> 5x **m4.large** Kafka  | 17,600          | 15,300,000  |
| 4x **m4.large** Hadoop-Worker <br> 5x **m4.large** Kafka  | 23,000          | 21,800,000  |
| 5x **m4.large** Hadoop-Worker <br> 5x **m4.large** Kafka  | 29,100          | 24,900,000  |



#Conclusions

Scribengin is limited by CPU, network speed, and disk speed.

More cores means Scribengin will be able to handle higher levels of parallelism more easily, and spread the CPU load across multiple machines.

Network speed is crucial for a cluster to work effectively together.  An operator can only receive data as quickly as the network will transfer it.

Finally, disk speed is also crucial.  When the point of an operation is to read and write data, disk speed can be a limiting factor.  SSD's are highly recommended to get the most out of your Scribengin cluster.

The cluster is partially limited by the amount of network throughput.  Cloudwatch reported the machines at close to 800 mbps consistently while doing tests with 5 Hadoop Worker nodes, which is expected to be close to maximum for m4.large's from our net testing using iperf.  With replication for HDFS and Kafka each set to 2, the system is under a heavy network load for this test.  

We also can see some possible disk writing issues when dealing with writing high volumes of data to HDFS.  The iostat tool reported that average disk wait times between 200-300 milliseconds on EBS volumes.






