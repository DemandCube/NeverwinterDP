Dataflow Chain For Log Processing Diagram


`````````

                                                          --------------------------------        -------------
                                                    /-----|  Kafka INFO Topic | Dataflow | -----> |  HDFS/S3  |
                                                    |     |------------------------------|   |    |------------
                                                    |                                        |
                                                    |                                        |
                                                    |     -------------------------------|   |    |----------|
                  |--------------------- ------|    |     |  Kafka WARN Topic | Dataflow | -----> |  HDFS/S3 |
Log Appender ===> | Kafka Log Topic | Dataflow | ---------|------------------------------|   |    ------------
                  -----------------------------|    |                                        |
                                                    |                                        |
                                                    |     --------------------------------|  |    |-------------|
                                                    |     |  Kafka ERROR Topic | Dataflow |-----> |  HDFS/S3    |
                                                    \-----|-------------------------------|  |    |--------------
                                                                                             |
                                                                                             |  
                                                                                             |
                                                                                             |     --------------------------
                                                                                             ----> |  ElasticSearch Sink    |
                                                                                                   |-------------------------
`````````

Demo Requirements:

1. Create a kafka log appender, configure hadoop, zookeeper, kafka ... to use the kafka log appender to collect the log data into a log topic
2. Create a dummy server that is periodically output the log data to the kafka log topic as well.
3. Create a splitter dataflow that split the log into 3 topics: INFO, WARNING, ERROR
4. Create a persister dataflow that save the log into the HDFS or S3. All the log should be output to elasticsearch as well.
5. Each dataflow should enhanced the message with perfix or suffix "Processed by the dataflow ${dataflowId}"
6. Create a chain framework that allow to chain multiple dataflow into one configuration and run by a single command


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
