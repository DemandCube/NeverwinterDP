#Hardware and JVM Memory Allocation#

In order to have an optimized configuration, you need to know how much RAM you have, how many CPUs, how much RAM need to give to each process. If you allocate more RAM to your process than the amount of RAM that your server have, the os will have to use swap space and make your server significantly slow down. 

For example - hadoop-master has 4 processes - namenode, secondary namenode, resource manager, datanode manager - and if you allocate 1024MB to each process while your server has only 2GB RAM, then it is a bad configuration.

Some rules to make a good configuration:

1. You should know your how much RAM is needed for the OS and the basic services. The rest should give to your server processes. For example, zookeeper or kafka configuration, the linux os will need 512 - 1024M. If you have 2G RAM machine, then you can allocate 1024M or 1536M to kafka or zookeeper.

2. Some service is IO limited, CPU limited or memory limited. You need to know what type of limitations your services are likely to have.  If your service is IO bound or CPU bound and you give more RAM to your service, it won't make any difference. Usually, the rule of thumb is:
    * If your service is a database like elasticsearch, hbase, RDBM ... they are memory bound and the more RAM the better
    * If your service has a lot of clients that connect to it, it will be CPU bound.
    * If your service is a batch process, then it is IO bound. In case the service is memory bound, a bigger instance with more memory will give better performance. 
    * In case of CPU limited or IO limited, more small instances will give better performance. 4 instances of 4G RAM will give better performance  than 1 16G RAM instance.

3. Our services:
    * Zookeeper is likely CPU bound and memory bound.
    * Kafka: Is definitely IO and CPU bound.  CPU limitations depend on the number of connections and topics.
    * Hadoop-Master: is memory and CPU bound depending on the number of connections. 
      * Most of memory is used to store DFS structure and the information of the blocks. 
      * We do not store much data on DFS so a 8GB RAM should be enough. 3GB for the namenode and 1.5GB for the other processes.
    * Hadoop-Worker: Since hadoop worker is used to run our YARN application, it will depends on your nature of the YARN application and the number of YARN containers created. Give 1024MB to yarn resource manager and dfs manager, the rest is reserved for yarn app. You can use 16GB for hadoop-worker instance
    * Elasticsearch:  database type so the more memory the better
    * Monitoring: just a servlet that connect to elasticsearch so 1GB - 2GB instance should be ok for all type of test


#Message Generation And Validation#

1. Technique to track the messages
    * The message generator should include the vm id or host id, chunk id and an unique sequence number into the message. The reason to include a chunk id is to deal with the very long test, which can generate billion of the messages which will be hard or not efficient to validate.
    * The design should allow more than one vm can run the message generator at the same time and store the information in a hierarchy that the validate tool can use to aggregate the information 

2. Technique to validate the messages
    * Create 1 vm or multiple vm, each vm should run one or more validate tool that validate 1 stream. The validate tool should parse the  message to extract the vm id, chunk id and the sequence number. The final result should be stored into the registry.
    * There should be a client that wait for all the validate tool vm terminate, compare the validate result with the generated information. Report the comparison result or report all the error or unexpected result.
    * Our current tracking framework can validate and detect the lost or duplicated messages.


#Performance And Validation Test Requirements#

1. The performance test should allocate memory heap to zookeeper, kafka, hadoop services correctly and optimized

2. The performance test be run with different number of kafka and hadoop worker. Usually double number of hadoop worker and kafka should gain at least 65% - 90%. The level of parallelism, number of worker and executor, number of the kafka partitions should be configured correctly in order to use the allocated hardware.For example if we have 6 kafka instances and we create a topic with 3 partitions so only 3 kafka instances are used. Same for number of workers and executors.

3. The performance test should run with different number of messages 10M, 100M ... messages

4. The performance test should run with different message size 256, 512, 1024.................

5. For the performance test, the validated data has to be correct first, before the number of throughput, elapsed time... are considered


6. Interpret The Result And Expected Behaviors
    * Ideally, all the metric should be constant which mean IO, CPU, Memory usage
    * The server reach its physical limit when either CPU or IO reach the limit. For ex, if we see that the CPU reach 100% of usage, then we need to find the CPU hotspot to optimize. If we know that the disk transfer time is 50M/s and our throughput is 35-40MB/s then probably we reach the io limit and we have no more room to optimize. The only way to optimize is to allocate more machnine, allocate more stream, partition so we can have more task to run in parallel. 
    * For the data intensive application such hadoop, our scribengin application... We usually have the IO bound problem , not CPU bound. The disk I/O performance is affected by the transfer time and seek time. If we have an other component that write to the disk such the log , buffer... It can affect significantly our sink/source performance. SSD improve a lot the seek time. But we need to find out that our IO bound is caused by our sink/source operation or other operation or usage %.
    * When any resource (CPU, Memory, IO) reach the limit, we should expect more warn, error, most of them should be timeout problem as the task, record... are not processed in an expected amount of time.
