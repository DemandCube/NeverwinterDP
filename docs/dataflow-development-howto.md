#Overview#

This howto will show you how to create a single dataflow, how to configure and how to run

In order to create a dataflow  or dataflow chain you need to have the data, understand your data structure, come up an idea how do you want to transform your data and where do you want to store the transformed data.

The following log splitter sample will show you how to create a dataflow, a dataflow chain with the requirements:

1. Prepare the data:
 - Create a kafka log appender, configure hadoop, zookeeper, kafka ... to use the kafka log appender to collect the log data into a log topic
 - Create a dummy server that is periodically output the log data to the kafka log topic as well.
2. Implement the log splitter dataflow
  - Create a splitter dataflow that split the log into 3 categories info, warn, error according to the log level
  - Store each log category in 3 different kafka topic info, warn, error
3. Implement the log splitter dataflow chain
  - Reuse the log splitter as 2
  - Create a persister dataflow, the persister should take the message output by (2) and store to kafka, hdfs or s3
4. Configure, deploy and run
  - Create a configuration for the single dataflow and dataflow chain
  - Deploy the dataflow or dataflow chain to scribengin
  - Run the dataflow or the dataflow chain

You can find the log sample code in the NeverwinterDP/scribengin/dataflow/log-sample project. The code come with unit test, release script, and run script that allow you to deploy and run the sample in a single command.

#Develop The Log Splitter Dataflow#

##The Log Splitter Diagram##

````````                                                  
                                                        Scribe Engine
                                            ........................................
                                            .                                      .       ...........
                                            .                       - - - - - -  - . - - > .  Info   .
                                            .                       |              .       ...........
                                            .                                      .
 ................      ...............      .     ...............   |              .       ...........
 . Log Generator. - >  . Kafka Topic . - -  . - > . Log Splitter. -  - - - - - - - . - - > .  Warn   .
 ................      ...............      .     . .............   |              .       ...........
                                            .                                      .
                                            .                                      .       ...........
                                            .                       | - - - - - -  . - - > .  Error  .
                                            .                                      .       ...........
                                            ........................................

`````````

##Implement The Dataflow Log Splitter Scribe##

A Dataflow Scribe is a class that allow you to customize how to transform the data and how to and where to stored the transformed data.

`````````
  public class LogMessageSplitter extends ScribeAbstract {
    int count = 0;

    public void process(DataflowMessage dflMessage, DataflowTaskContext ctx) throws Exception {
      Log4jRecord log4jRec = JSONSerializer.INSTANCE.fromBytes(dflMessage.getData(), Log4jRecord.class) ;
      String level = log4jRec.getLevel().toLowerCase();
      ctx.write(level, dflMessage);

      count++ ;
      if(count > 0 && count % 10000 == 0) {
        ctx.commit();
      }
    }
  }

`````````

DataflowMessage:     is an object that hold your data in byte format.
DataflowTaskContext: is an object that hold the dataflow running environment and resources 


##Configure The Dataflow Log Splitter##

The configuration look as follows

`````````
  {
    "id" :   "log-splitter-dataflow",
    "name" : "log-splitter-dataflow",

    "numberOfWorkers" : 1,
    "numberOfExecutorsPerWorker" : 3,
    "taskSwitchingPeriod" : 5000,
    "maxRunTime": 90000,
    "scribe" : "com.neverwinterdp.dataflow.logsample.LogMessageSplitter",

    "sourceDescriptor" : {
      "type" : "kafka", "topic" : "log4j", "zk.connect" : "zookeeper-1:2181", "reader" : "raw", "name" : "LogSplitterDataflow"
    },

    "sinkDescriptors" : {
      "default" : {
        "type" : "elasticsearch", 
        "address" : "elasticsearch-1:9300", "indexName": "dataflow_app_log", "mappingType": "com.neverwinterdp.util.log.Log4jRecord"
      },

      "info" : {
        "type" : "HDFS", "location" : "/log-sample/hdfs/info"
      },

      "warn" : {
        "type" : "HDFS", "location" : "/log-sample/hdfs/warn"
      },

      "error" : {
        "type" : "HDFS", "location" : "/log-sample/hdfs/error"
      }
    }
  }

`````````

Where:

* id                        : The unique identifier for the dataflow. You cannot have 2 dataflow with the same id running at the same time.
* name                      : The name of the dataflow, you can have one or more dataflow with the same name
* numberOfWorkers           : The number of vm or server the scribengin should allocate for your dataflow
* numberOfExecutorsPerWorker: The number of executor or thread to run the dataflow task per worker
* taskSwitchingPeriod       : How long an executor should work on a task before it switches to the other one
* maxRunTime                : You can control how long your dataflow should run. -1 value will make the dataflow run forever
* scribe                    : The scribe logic class that you can control how to transform your data and where to output the transformed data
* sourceDescriptor          : The configuration for the data source. Depending on the source type, you have the different configuration and parameter
 * Kafka                    :
 * HDFS                     :
 * S3                       :
* sinkDescriptors           : The list of the configuration for the data sink. Depending on the sink type you have different configuration and parameter
 * Kafka                    :
 * HDFS                     :
 * S3                       :


##Package The Dataflow Log Splitter##

The dataflow should be packaged as show below

`````````
  log-sample
    ├── bin
    │   ├── run-chain.sh
    │   ├── run-dataflow-chain.sh
    │   └── run-splitter.sh
    ├── conf
    │   ├── chain
    │   │   ├── hdfs-log-dataflow-chain.json
    │   │   ├── kafka-log-dataflow-chain.json
    │   │   ├── local
    │   │   │   ├── hdfs-log-dataflow-chain.json
    │   │   │   ├── kafka-log-dataflow-chain.json
    │   │   │   ├── log-dataflow-chain.json
    │   │   │   └── s3-log-dataflow-chain.json
    │   │   └── s3-log-dataflow-chain.json
    │   └── splitter
    │       ├── kafka-to-hdfs-log-splitter-dataflow.json
    │       ├── kafka-to-kafka-log-splitter-dataflow.json
    │       ├── kafka-to-s3-log-splitter-dataflow.json
    │       └── local
    │           └── kafka-to-kafka-log-splitter-dataflow.json
    └── libs
      ├── scribengin.dataflow.log-sample-1.0-SNAPSHOT-tests.jar
      └── scribengin.dataflow.log-sample-1.0-SNAPSHOT.jar

`````````

Where the conf and lib directory are mandatory and the bin directory is optional

* conf: The dataflow configuration, dataflow chain configuration, log4j configuration ... should be placed in this directory
* lib:  The lib directory should contain the library of your scribe class and its dependencies
* bin:  This is an optional directory. In order to a dataflow, you may need many extra steps such launch the data generator, deploy the dataflow package... and then run. You may want to write a script to simplify the launch process and the script should be placed in the bin directory

##Deploy And Run The Dataflow Log Splitter##

In order to deploy and run a dataflow, you need to setup a scribengin cluster. See scribengin-cluster-setup-quickstart.md for more detail instruction.

Make sure that your scribengin cluster is running:

1. Make sure zookeeper is running
2. Make sure hadoop-master and hadoop-worker are running
3. The command $path/scribengin/bin/shell.sh scribengin info should give a good report and status

To run a dataflow , you will need to upload the dataflow app to dfs and then submit the dataflow to the scribengin. The scribengin service will be responsible to init and launch the dataflow master according to the dataflow configuration. The dataflow master will continue to init the registry and launch the dataflow tasl worker.


Command to upload and run the dataflow

````````
SHELL=./scribengin/bin/shell.sh

#########################################################################################################################
# Upload The App                                                                                                        #
#########################################################################################################################

$SHELL vm upload-app --local $APP_DIR --dfs /applications/log-sample

#########################################################################################################################
# Launch A Single Dataflow                                                                                              #
#########################################################################################################################
$SHELL dataflow submit \
  --dfs-app-home /applications/log-sample \
  --dataflow-config $DATAFLOW_DESCRIPTOR_FILE \
  --dataflow-id log-splitter-dataflow-1 --max-run-time 180000
````````

In the log sample program , You can find the log splitter script log-sample/src/app/bin/run-splitter.sh. The script does:

1. Upload the dataflow package
2. Generate the message
 * Submit the log generator app 
 * Wait the log generator app to terminate
3. Run the log splitter dataflow
 * Submit the log splitter dataflow
 * Wait for the dataflow termination
 * Print the dataflow splitter info
4. Run the log message validator
 * Submit the log validator app
 * Wait for the log validator app terminates.
5. Print scribengin , registry and dataflow information

````````

SHELL=./scribengin/bin/shell.sh

#########################################################################################################################
# Upload The App                                                                                                        #
#########################################################################################################################
$SHELL vm upload-app --local $APP_DIR --dfs /applications/log-sample

#########################################################################################################################
# Launch The Message Generator                                                                                          #
#########################################################################################################################
$SHELL vm submit \
   --dfs-app-home /applications/log-sample \
   --registry-connect zookeeper-1:2181 --registry-db-domain /NeverwinterDP --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl \
   --name vm-log-generator-1  --role vm-log-generator --vm-application  com.neverwinterdp.dataflow.logsample.vm.VMToKafkaLogMessageGeneratorApp \
   --prop:report-path=/applications/log-sample/reports --prop:num-of-message=$NUM_OF_MESSAGE --prop:message-size=$MESSAGE_SIZE

$SHELL vm wait-for-vm-status --vm-id vm-log-generator-1 --vm-status TERMINATED --max-wait-time 45000

#########################################################################################################################
# Launch A Single Dataflow                                                                                              #
#########################################################################################################################
$SHELL dataflow submit \
  --dfs-app-home /applications/log-sample \
  --dataflow-config $DATAFLOW_DESCRIPTOR_FILE \
  --dataflow-id log-splitter-dataflow-1 --max-run-time 180000


$SHELL dataflow wait-for-status --dataflow-id log-splitter-dataflow-1 --status TERMINATED
$SHELL dataflow info --dataflow-id log-splitter-dataflow-1 --show-all


#########################################################################################################################
# Launch Validator                                                                                                      #
#########################################################################################################################
$SHELL vm submit  \
  --dfs-app-home /applications/log-sample \
  --registry-connect zookeeper-1:2181  --registry-db-domain /NeverwinterDP --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl \
  --name vm-log-validator-1 --role log-validator  --vm-application com.neverwinterdp.dataflow.logsample.vm.VMLogMessageValidatorApp \
  --prop:report-path=/applications/log-sample/reports \
  --prop:num-of-message-per-partition=$NUM_OF_MESSAGE \
  --prop:wait-for-termination=300000 \
  $LOG_VALIDATOR_OPTS

$SHELL vm wait-for-vm-status --vm-id vm-log-validator-1 --vm-status TERMINATED --max-wait-time 300000

#########################################################################################################################
# Print Info                                                                                                            #
#########################################################################################################################
$SHELL vm info
$SHELL registry dump --path /applications/log-sample

````````

#Develop A Dataflow Chain#

The dataflow chain is designed in a way that the output of the previous dataflow is the input of the next dataflow.

##The Log Splitter Dataflow Chain Diagran##

`````````

                                                        Scribe Engine
                                            ...........................................
                                            .                                         .
                                            .                          ............   .       ...........
                                            .                       - -. Persister. - . - - > .  Info   .
                                            .                       |  ............   .       ...........
                                            .                                         .
 ................      ...............      .     ...............   |  ............   .       ...........
 . Log Generator. - >  . Kafka Topic . - -  . - > . Log Splitter. -  - . Persister. - . - - > .  Warn   .
 ................      ...............      .     . .............      ............   .       ...........
                                            .                       |                 .
                                            .                          ............   .       ...........
                                            .                       | -. Persister. - . - - > .  Error  .
                                            .                          ............   .       ...........
                                            .                                         .
                                            ...........................................


`````````

##Implement The Dataflow Log Splitter And Persister Scribe##

1. We can reuse the log message splitter from the previous dataflow
2. Implement a log persister that will save the log message to the different location according to the sink configuration.

``````
  public class LogMessageSplitter extends ScribeAbstract {
    int count = 0;

    public void process(DataflowMessage dflMessage, DataflowTaskContext ctx) throws Exception {
      Log4jRecord log4jRec = JSONSerializer.INSTANCE.fromBytes(dflMessage.getData(), Log4jRecord.class) ;
      String level = log4jRec.getLevel().toLowerCase();
      ctx.write(level, dflMessage);

      count++ ;
      if(count > 0 && count % 10000 == 0) {
        ctx.commit();
      }
    }
  }

  public class LogMessagePerister extends ScribeAbstract {
    int count = 0 ;

    public void process(DataflowMessage dflMessage, DataflowTaskContext ctx) throws Exception {
      String[] sink = ctx.getAvailableSinks();
      for(String selSink : sink) {
        ctx.write(selSink, dflMessage);
      }

      count++ ;
      if(count > 0 && count % 10000 == 0) {
        ctx.commit();
      }
    }
  }

``````

##Dataflow Chain Configuration##

The dataflow chain configuration is quite similar to the dataflow configuration. The different in the configuration is in the chain configuration, you have a list of dataflow configuration and you have to make sure that the output (sink) of one dataflow is the input (source) of the other dataflow. 

`````````
{
  "submitter": "Order",

  "descriptors": [
    {
      "id" :   "log-splitter-dataflow",
      "name" : "log-splitter-dataflow",

      "numberOfWorkers" : 2,
      "numberOfExecutorsPerWorker" : 3,
      "maxRunTime": 4800000,
      "taskSwitchingPeriod" : 30000,
      "scribe" : "com.neverwinterdp.dataflow.logsample.LogMessageSplitter",

      "sourceDescriptor" : {
        "type" : "kafka", "topic" : "log4j", "zk.connect" : "zookeeper-1:2181", "reader" : "raw", "name" : "LogSplitterDataflow"
      },

      "sinkDescriptors" : {
        "default" : {
          "type" : "elasticsearch", 
          "address" : "elasticsearch-1:9300", "indexName": "dataflow_app_log", "mappingType": "com.neverwinterdp.util.log.Log4jRecord"
        },

        "warn" : {
          "type" : "kafka", "topic" : "log4j.warn", "zk.connect" : "zookeeper-1:2181", "name" : "LogSplitterDataflow"
        },

        "error" : {
          "type" : "kafka", "topic" : "log4j.error", "zk.connect" : "zookeeper-1:2181", "name" : "LogSplitterDataflow"
        },

        "info" : {
          "type" : "kafka", "topic" : "log4j.info", "zk.connect" : "zookeeper-1:2181", "name" : "LogSplitterDataflow"
        }
      }
    },

    {
      "id" :   "log-persister-dataflow-info",
      "name" : "log-persister-dataflow",
      "numberOfWorkers" : 2,
      "numberOfExecutorsPerWorker" : 3,
      "maxRunTime": 5400000,
      "taskSwitchingPeriod" : 30000,
      "scribe" : "com.neverwinterdp.dataflow.logsample.LogMessagePerister",

      "sourceDescriptor" : {
        "type" : "kafka", "topic" : "log4j.info", "zk.connect" : "zookeeper-1:2181", "reader" : "record", "name" : "LogSampleDataflow"
      },

      "sinkDescriptors" : {
        "hdfs" : {
          "type" : "HDFS", "location" : "/log-sample/hdfs/info"
        }
      }
    },

    {
      "id" :   "log-persister-dataflow-warn",
      "name" : "log-persister-dataflow",
      "numberOfWorkers" : 2,
      "numberOfExecutorsPerWorker" : 3,
      "maxRunTime": 5400000,
      "taskSwitchingPeriod" : 30000,
      "scribe" : "com.neverwinterdp.dataflow.logsample.LogMessagePerister",

      "sourceDescriptor" : {
        "type" : "kafka", "topic" : "log4j.warn", "zk.connect" : "zookeeper-1:2181", "reader" : "record", "name" : "LogSampleDataflow"
      },

      "sinkDescriptors" : {
        "hdfs" : {
          "type" : "HDFS", "location" : "/log-sample/hdfs/warn"
        }
      }
    },

    {
      "id" :   "log-persister-dataflow-error",
      "name" : "log-persister-dataflow",
      "numberOfWorkers" : 2,
      "numberOfExecutorsPerWorker" : 3,
      "maxRunTime": 5400000,
      "taskSwitchingPeriod" : 30000,
      "scribe" : "com.neverwinterdp.dataflow.logsample.LogMessagePerister",

      "sourceDescriptor" : {
        "type" : "kafka", "topic" : "log4j.error", "zk.connect" : "zookeeper-1:2181", "reader" : "record", "name" : "LogSampleDataflow"
      },

      "sinkDescriptors" : {
        "hdfs" : {
          "type" : "HDFS", "location" : "/log-sample/hdfs/error"
        }
      }
    }
  ]
}

`````````

* submitter: allow you to select the submitter type , either order or parallel
 * order make sure that the dataflow submit in the order, the dataflow has to successfull submit and have the running status before the next one is submitted
 * parallel allow all the dataflows are submitted in the same time by a isolated thread.
* descriptors: The list of the dataflow descriptor

##Package The Dataflow Chain##

Same as the dataflow package.  In fact you can package multiple dataflow or a dataflow chain into one package and deploy once.


##Run The Dataflow Chain##

As running the dataflow, you have to make sure that your scribengin cluster is running:

* Make sure zookeeper is running
* Make sure hadoop-master and hadoop-worker are running
* The command $path/scribengin/bin/shell.sh scribengin info should give a good report and status

The following script is take from log-sample/src/app/bin/run-dataflow-chain.sh. It allows you to run deploy the dataflow package, run the message generator, run the dataflow chain and then run the validator

``````````
SHELL=./scribengin/bin/shell.sh


#########################################################################################################################
# Upload The App                                                                                                        #
#########################################################################################################################
$SHELL vm upload-app --local $APP_DIR --dfs /applications/log-sample

#########################################################################################################################
# Launch The Message Generator                                                                                          #
#########################################################################################################################
START_MESSAGE_GENERATION_TIME=$SECONDS
$SHELL vm submit \
   --dfs-app-home /applications/log-sample \
   --registry-connect zookeeper-1:2181 --registry-db-domain /NeverwinterDP --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl \
   --name vm-log-generator-1  --role vm-log-generator --vm-application  com.neverwinterdp.dataflow.logsample.vm.VMToKafkaLogMessageGeneratorApp \
   --prop:report-path=/applications/log-sample/reports --prop:num-of-message=$NUM_OF_MESSAGE --prop:message-size=$MESSAGE_SIZE

$SHELL vm wait-for-vm-status --vm-id vm-log-generator-1 --vm-status TERMINATED --max-wait-time 45000
MESSAGE_GENERATION_ELAPSED_TIME=$(($SECONDS - $START_MESSAGE_GENERATION_TIME))
echo "MESSAGE GENERATION TIME: $MESSAGE_GENERATION_ELAPSED_TIME" 
#########################################################################################################################
# Launch A Dataflow Chain                                                                                               #
#########################################################################################################################
START_DATAFLOW_CHAIN_TIME=$SECONDS
$SHELL dataflow submit-chain \
  --dfs-app-home /applications/log-sample \
  --dataflow-chain-config $DATAFLOW_DESCRIPTOR_FILE --dataflow-max-runtime $MAX_RUNTIME

$SHELL dataflow wait-for-status --dataflow-id log-splitter-dataflow --max-wait-time $MAX_RUNTIME --status TERMINATED
$SHELL dataflow wait-for-status --dataflow-id log-persister-dataflow-info  --status TERMINATED

$SHELL dataflow wait-for-status --dataflow-id log-persister-dataflow-warn  --status TERMINATED
$SHELL dataflow wait-for-status --dataflow-id log-persister-dataflow-error --status TERMINATED
DATAFLOW_CHAIN_ELAPSED_TIME=$(($SECONDS - $START_DATAFLOW_CHAIN_TIME))
echo "Dataflow Chain ELAPSED TIME: $DATAFLOW_CHAIN_ELAPSED_TIME" 

#########################################################################################################################
# Launch Validator                                                                                                      #
#########################################################################################################################
START_MESSAGE_VALIDATION_TIME=$SECONDS
$SHELL vm submit  \
  --dfs-app-home /applications/log-sample \
  --registry-connect zookeeper-1:2181  --registry-db-domain /NeverwinterDP --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl \
  --name vm-log-validator-1 --role log-validator  --vm-application com.neverwinterdp.dataflow.logsample.vm.VMLogMessageValidatorApp \
  --prop:report-path=/applications/log-sample/reports \
  --prop:num-of-message-per-partition=$NUM_OF_MESSAGE \
  --prop:wait-for-termination=3600000 \
  $LOG_VALIDATOR_VALIDATE_OPT

$SHELL vm wait-for-vm-status --vm-id vm-log-validator-1 --vm-status TERMINATED --max-wait-time 3600000

MESSAGE_VALIDATION_ELAPSED_TIME=$(($SECONDS - $START_MESSAGE_VALIDATION_TIME))
echo "MESSAGE VALIDATION TIME: $MESSAGE_VALIDATION_ELAPSED_TIME" 
#########################################################################################################################
# Dump the vm and registry info                                                                                         #
#########################################################################################################################
$SHELL vm info
$SHELL registry dump --path /applications/log-sample

echo "MESSAGE GENERATION TIME    : $MESSAGE_GENERATION_ELAPSED_TIME" 
echo "Dataflow Chain ELAPSED TIME: $DATAFLOW_CHAIN_ELAPSED_TIME" 
echo "MESSAGE VALIDATION TIME    : $MESSAGE_VALIDATION_ELAPSED_TIME" 
``````````

