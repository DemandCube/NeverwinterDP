Performance And Validation Test Requirements

1. Create 1 or multiple vm, using our vm framework. The vm will generate the log. Each log message should include an id and later can validate by our validator tool.
2. The VM should periodically save the status and progress report to the registry in a hierachy structure. Remember that we can have more than one vm that 
generate the log message. 
3. Create 1 or multiple vm, using our vm framework to validate output data, base on the information and report in the registry.
4. The test should be able to simulate different kind of server failure such kafka restart, kafka replace, worker and master failure for each dataflow(remember 
    that chain many dataflow together).
5. Should be able to launch and monitor from the swingui. Should be able to monitor with kibana
6. Need more powerful hardware to test
7. Ideally by september, the demo test has to be able to run smoothly for at least 12h with dataflow worker and master failure simulation.


Technique

1. Technique to track the messages
- The vm that run the log generator should include the vm id or host id and an unique sequence number into the log message
- The log generator should save the information such the host id, the number of messages it generates and all the other necessary information
in order the validate tool can retrieve and assert
- The design should allow more than one vm can run at the same time and store the information in a hierarchy that the validate tool can aggregate the information 

2. Technique to validate the messages
- Create 1 vm or multiple vm, each vm should run one or more validate tool that validate 1 stream. The validate tool should parse the log message to extract the vm id and the sequence number. The final result should be stored into the registry.
- There should be a client that wait for all the validate tool vm terminate, compare the validate result with the generated information. Report the comparison result or all the error or unexpected result.
- Our current tracking framework can validate and detect the lost or duplicated message. 

3. Metric Collection

Here is sample of metric that we should collect at a specific point of time. We can add more detail metric later.

Timestamp                 Server         Metric Name                      Metric

16/6/2015 13:00:30        host-1         CPU Usage                         # cycles(70%)
  16/6/2015 13:00:30        host-1           Thread Count                               50
  16/6/2015 13:00:30        host-1           Thread Blocked Count                        1
  16/6/2015 13:00:30        host-1           GC                                # cycles(%)
  16/6/2015 13:00:30        host-1         Memory Usage                         195MB(60%)
16/6/2015 13:00:30        host-1         Disk Usage                            51GB(60%)
  16/6/2015 13:00:30        host-1         Throughput in MB                         10MB/s
  16/6/2015 13:00:30        host-1         Throughput in records             25k records/s
  16/6/2015 13:00:30        host-1            Source read Avg                1 record/50ns           
  16/6/2015 13:00:30        host-1            HDFS Sink Write Avg            1 record/50ns           
  16/6/2015 13:00:30        host-1            Kafka Sink Write Avg           1 record/50ns           
  16/6/2015 13:00:30        host-1         log Message Count                           175
  16/6/2015 13:00:30        host-1           log info                                  100
  16/6/2015 13:00:30        host-1           log warn                                   50
  16/6/2015 13:00:30        host-1           log error                                  25


  - We need to find a way to draw the chart with different metric measurement to show the relation of the problems, for ex when the CPU, Memory, IO reach the limit we expect more error... Probably a 3D chart can show this type of relations.

  - We can add more metric to measure the CPU, Memory, IO for the other components such Logger, each sink, source....

  4. Interpret The Result And Expected Behaviors

  - Ideally, all the metric should be constant which mean IO, CPU, Memory usage

  - The server reach its physical limit when either CPU or IO reach the limit. For ex, if we see that the CPU reach 100% of usage, then we need to find the CPU hotspot to optimize. If we know that the disk transfer time is 50M/s and our throughput is 35-40MB/s then probably we reach the io limit and we have no more room to optimize. The only way to optimize is to allocate more machnine, allocate more stream, partition so we can have more task to run in parallel. 

  - For the data intensive application such hadoop, our scribengin application... We usually have the IO bound problem , not CPU bound. The disk I/O performance is affected by the transfer time and seek time. If we have an other component that write to the disk such the log , buffer... It can affect significantly our sink/source performance. SSD improve a lot the seek time. But we need to find out that our IO bound is caused by our sink/source operation or other operation or usage %.

  - When any resource (CPU, Memory, IO) reach the limit, we should expect more warn, error, most of them should be timeout problem as the task, record... are not processed in an expected amount of time.


