#How to develop a Scribengin Dataflow#

This howto will show you how to create, configure, and run a single, reliable dataflow

##Sample code##
[You can find lots of sample code here](https://github.com/Nventdata/NeverwinterDP/tree/master/scribengin/dataflow/log-sample).  The code comes complete with unit tests, a release script, and a run script that allows you to deploy and run the sample in a single command.

##Overview##
In order to create a dataflow (or chain of dataflows) you need to prepare your sources and sinks, have data in the source, understand your data structure, and decide how to transform the data.

1. Implement the log splitter dataflow
  - Create a dataflow that reads in logs from Kafka
  - Based on the log level, write that log to a corresponding kafka topic - info, warn, and error
1. Implement the log splitter dataflow chain
  - Create a persister dataflow
  - The persister should read in logs from the kafka topics info, warn, and error 
  - The persister will then move the logs to permanent storage - kafka, hdfs or s3
1. Configure, deploy and run
  - Create a configuration for the single dataflow and dataflow chain
  - Deploy the dataflow or dataflow chain to Scribengin
  - Run the dataflow or the dataflow chain



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

A Scribe is the class that allows you to customize the transformation of your data.  It lets your dataflow choose how and where to store the transformed data.

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

DataflowMessage:     the object that holds your data in byte format.
DataflowTaskContext: the object that hold the dataflow running environment and resources 


##Configure The Dataflow Log Splitter##

The Log Splitter configuration.  
This is required for Scribengin to parse and create the dataflow


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

* id                        : The unique identifier for the dataflow. Every dataflow running on Scribengin MUST have a unique ID.
* name                      : The name of the dataflow.  This does not need to be unique.
* numberOfWorkers           : The number of vm/containers Scribengin should allocate for the dataflow
* numberOfExecutorsPerWorker: The number of executors (threads) to run per worker
* taskSwitchingPeriod       : How long an executor should work on a task before it switches to the other one
* maxRunTime                : How long the dataflow should run. Setting to -1 will make the dataflow run forever.
* scribe                    : The scribe class to use. Controls how to transform your data and where to output the transformed data
* sourceDescriptor          : The configuration for the data source. Depending on the source type, there are different configurations and parameters
 * Kafka                    :
 * HDFS                     :
 * S3                       :
* sinkDescriptors           : The list of the configurations for the data sink. Depending on the sink type you have different configurations and parameters.  Multiple sinks can easily be configured.
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

* conf: Dataflow configuration, dataflow chain configuration, log4j configuration ... should be placed in this directory
* lib:  The lib directory should contain the library of your scribe class and its dependencies
* bin:  This is an optional directory. In order to a dataflow, you may need many extra steps such launch the data generator, deploy the dataflow package... and then run. You may want to write a script to simplify the launch process and the script should be placed in the bin directory

To automatically build the [log-sample](https://github.com/Nventdata/NeverwinterDP/tree/master/scribengin/dataflow/log-sample):
```
cd NeverwinterDP
gradle clean build install release -x test

cd NeverwinterDP/scribengin/dataflow/log-sample/
gradle clean build install release -x test

#Your $APP_DIR will now be set up here:
#NeverwinterDP/scribengin/dataflow/log-sample/build/release/dataflow/log-sample/
```

##Deploy And Run The Dataflow Log Splitter##

In order to deploy and run a dataflow, you need to setup a scribengin cluster. See scribengin-cluster-setup-quickstart.md for more detail instruction.

Make sure that your scribengin cluster is running:

1. Make sure zookeeper is running
2. Make sure hadoop-master and hadoop-worker are running
3. The command ```$path/scribengin/bin/shell.sh scribengin info``` should give a good report and healthy status

To run a dataflow , you will need to upload the dataflow app to dfs and then submit the dataflow to the scribengin. The scribengin service will be responsible to init and launch the dataflow master according to the dataflow configuration. The dataflow master will continue to init the registry and launch the dataflow tasl worker.


Commands to upload and run the dataflow

````````
SHELL=NeverwinterDP/release/build/release/neverwinterdp/scribengin/bin/shell.sh
APP_DIR=../dataflow/log-sample/

#########################################################################################################################
# Upload The App                                                                                                        #
#########################################################################################################################

$SHELL vm upload-app --local $APP_DIR --dfs /applications/log-sample

#########################################################################################################################
# Launch A Single Dataflow                                                                                              #
#########################################################################################################################
#Choose any of the splitter dataflows (or any dataflow json configuration)
DATAFLOW_DESCRIPTOR_FILE=NeverwinterDP/scribengin/dataflow/log-sample/build/release/dataflow/log-sample/conf/splitter/kafka-to-kafka-log-splitter-dataflow.json

$SHELL dataflow submit \
  --dfs-app-home /applications/log-sample \
  --dataflow-config $DATAFLOW_DESCRIPTOR_FILE \
  --dataflow-id log-splitter-dataflow-1 --max-run-time 180000
````````

In the [sample code](https://github.com/Nventdata/NeverwinterDP/tree/master/scribengin/dataflow/log-sample) , You can find the log splitter automation script log-sample/src/app/bin/run-splitter.sh. The script does:

1. Upload the dataflow package
2. Generate messages into Kafka via the log generator app
 * Submit the log generator app to YARN
 * Wait for the log generator app to terminate
3. Run the log splitter dataflow
 * Submit the log splitter dataflow
 * Wait for the dataflow termination
 * Print the dataflow splitter info
4. Run the log message validator
 * Submit the log validator app to YARN
 * Wait for the log validator app terminates.
5. Print scribengin , registry and dataflow information

````````
#run-splitter.sh
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
2. Implement a log persister that will save the log message to a different location according to the sink configuration.

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

The dataflow chain configuration is similar to the dataflow configuration. The difference in configuration is in the chain configuration; There is a list of dataflow configurations and you have to make sure that the output (sink) of one dataflow is the input (source) of the other dataflow. 

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

* submitter: allows you to select the submitter type , either order or parallel
  * order - the dataflows will submit in the order; each dataflow must successfully submit and start running before the next dataflow is submitted.  For a chain, it is recommended to submit in order
  * parallel - allow all the dataflows are submitted at the same time by a isolated thread.
* descriptors: The list of the dataflow descriptor

##Package The Dataflow Chain##

Same as the dataflow package.  You can package multiple dataflows or dataflow chains into one package and deploy them all at once.


##Run The Dataflow Chain##

While running the dataflow, you have to make sure that your scribengin cluster is running:

* Make sure zookeeper is running
* Make sure hadoop-master and hadoop-worker are running
* The command ```$path/scribengin/bin/shell.sh scribengin info``` should give a good report and healthy status

The following script is taken from log-sample/src/app/bin/run-dataflow-chain.sh. It allows you to deploy the dataflow package, run the message generator, run the dataflow chain, and then run the validator

``````````
#run-dataflow-chain.sh
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

