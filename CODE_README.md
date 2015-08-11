#Overview#

The code is organized in the following hierachy

```
NeverwinterDP
  ├── lib
  │   ├── utils
  │   ├── buffer
  │   └── yara
  ├── module
  │   ├── commons
  │   ├── elasticsearch
  │   └── kafka
  ├── registry
  │   ├── core
  │   ├── vm
  │   └── vm-sample
  └── scribengin
  |   ├── core
  |   ├── dataflow
  |   │   └── log-sample
  |   └── release
  ├── client
  │   └── swingui
  ├── jvmagent
  │   ├── DemoApp
  │   ├── bootstrap
  │   └── registry
  ├── release
```

where the first level can be considered as a project. The seccond level is the components of the project. A project can be depended on another project or a component of the other project

#Lib#

The lib project contains many small and independant library that use to solve a speicific problem or requirement. The library component is usually small, depend only on the jre or 1 or 2 other thrird party libraries.

##utils##

1. The com.neverwinterdp.util contains many util classes that help to manage json, exception, reflection, list, map...
2. The com.neverwinterdp.util.text contains the util classes that help to fomat the text, convert the number , byte to string format , to print out the data in the tabular format.
3. The com.neverwinterdp.util.io contains classes that help to manage the files, read/write from/to InputStream and OutputStream.

##buffer##

The buffer project is based the chronicle queue https://github.com/OpenHFT/Java-Chronicle, which is a high, reliable and persisted framework. The buffer project extends the chronicle project by implementing a multiple queue that allow one thread to write to a segment while another thread dequeue from another segment. So the read and write thread does not block each other.

##yara##

Yara project is a metric project base on the codahale or later drop wizard project concept. The main different between codahale and yara is yara used the QDigest algorithm from https://github.com/addthis/stream-lib/tree/master/src/main/java/com/clearspring/analytics/stream/quantile. The main advantage from qdigest algorithm is it used less memorey and allow to merge the data which is very important to merge the metric from the different server. you can AlgorithmComparatorUnitTest to see the different performance and accuracy of the codahale, QDigest and TDigest algorithm.

#Module#

The goal of the module project is to create a service api/framework that allow to implement, wire and deploy the services in the same manager. The project goal is also to wrap the other project such kafka, zookeeper, elasticsearch as a service of the neverwinterdp.

##commons##

The commons component contains the service framework and api that base on the google guice project.

##elasticsearch##

The elasticsearch project contains the service code wrapper that allow to run the elasticsearch as a neverwinterdp service. It also contains other code sunch the elasticsearch client wrapper, the multi node server to simulate the elasticsearch cluster for the unit test.

##kafka##

The kafka module project contains: 

1. The service code wrapper that allow to run the kafka as a neverwinterdp service. 
2. The multi node server to simulate the kafka cluster for the unit test.
3. The AckKafkaWriter to allow buffer and resend a message if the message is sent to a dead broker and does not get an ack.
4. The KafkaPartitionReader to allow to read the message from a kafka partition, commit or rollback to a previous commit read.
5. The Kafka send and check tool. The tool allow to send a set of message to a topic and the check tool will retrieve the messages and check to see if there is any lost or duplicated message.

#Registry#

The registry is a centralized service that maintain the data in a hierachry structure. The service is used for maintaining configuration information, naming, providing distributed synchronization, and providing group services. The current registry is implemented on top of the zookeeper project.

##core##

The core component contains:

1. A Registry api that allow to create,retrieve, update the data in the registry
2. A zookeeper Registry implementation
3. A distributed lock framework that allow multiple servers synchronously access and read/write/update a resource
4. A distributed queue framework
5. A leader election framework to allow a server is elected as a leader and the other stay as a backup.
6. A task framework is a a generic framework that keep track of the task list, available tasks to process, the assigned tasks, the status of the tasks... The task framework works as follow. A task master will read the configuration, compute and create a task list in the registry. The task worker will pickup a task from the available task list, create a heartbeat and mark the task as RUNNING and start working on the task. The worker may suspended the task and put back the task in the available list and another worker may pick up the task again and resume the task. If a worker fail at the middle, the task master should detect the broken heartbeat and put back the task in the available list.
7. The transaction event framework or txevent framework. The framework allow one client broadcast an event and obtain a watcher, the watcher will notify the client when an other server receive the event , complete to process the event or other custom status.

##vm##

The vm component is a sub project of the registry. The vm project allows to dynamically manage a group of vm (virtual machine). The vm can be allocated on request and return to the vm pool when it terminates. The framework consist of a vm master and  1 or more master slave. The vm master is responsible to allocate the vm, register it with the registry and detect the broken or terminated vm, return them to the vm pool. 

The vm project come with 2 implementations:

1. The single jvm implementation, which all the vms run in the same jvm. Each vm has its own thread to run and manage the vm. The single jvm implementation is designed for unit testing.

2. The yarn implemetation is an implementation on top of hadoop yarn project. In this implementation, the vm master is a yarn application master, it wait for the request and allocate the yarn container, register it with the vm registry framework.

##vm-sample##

The project contains a single class VMSampleApp which print out some hello message. You can run it with the unit vm unit test framework or in a vm yarn application framework.

#Scribengin#

The scribengin is a project that allow to move the data from different source type to the different sink in a fast and reliable manner.

##core##

The core component consist:

1. storage source: The source concept is a data repository that partition into multiple data stream. The source api consist of the Source, SourceStream and SourceStreamReader.   
2. storage sink:
3. scribengin service:
4. dataflow and dataflow task:
5. dataflow service:
6. dataflow worker:

##dataflow log-sample##

##release##

#Release#
