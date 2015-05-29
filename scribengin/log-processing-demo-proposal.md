The goal is to create a dataflow chain as the diagram:


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

Requirements:

1. Create a kafka log appender, configure hadoop, zookeeper, kafka ... to use the kafka log appender to collect the log data into a log topic
2. Create a dummy server that is periodically output the log data to the kafka log topic as well.
3. Create a splitter dataflow that split the log into 3 topics: INFO, WARNING, ERROR
4. Create a persister dataflow that save the log into the HDFS or S3. All the log should be output to elasticsearch as well.
5. Each dataflow should enhanced the message with perfix or suffix "Processed by the dataflow ${dataflowId}"
6. Create a chain framework that allow to chain multiple dataflow into one configuration and run by a single command
