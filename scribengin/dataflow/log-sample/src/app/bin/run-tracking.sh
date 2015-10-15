#!/bin/bash

if [ "x$JAVA_HOME" == "x" ] ; then 
  echo "WARNING JAVA_HOME is not set"
fi

export HADOOP_USER_NAME="neverwinterdp"

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
APP_DIR=`cd $SCRIPT_DIR/..; pwd; cd $SCRIPT_DIR`
NEVERWINTERDP_BUILD_DIR=`cd $APP_DIR/../..; pwd; cd $SCRIPT_DIR`

JAVACMD=$JAVA_HOME/bin/java

function get_opt() {
  OPT_NAME=$1
  DEFAULT_VALUE=$2
  shift
  
  #Par the parameters
  for i in "$@"; do
    index=$(($index+1))
    if [[ $i == $OPT_NAME* ]] ; then
      value="${i#*=}"
      echo "$value"
      return
    fi
  done
  echo $DEFAULT_VALUE
}

function has_opt() {
  OPT_NAME=$1
  shift
  #Par the parameters
  for i in "$@"; do
    if [[ $i == $OPT_NAME ]] ; then
      echo "true"
      return
    fi
  done
  echo "false"
}


#########################################################################################################################
# Setup environment and collect the setup parameters                                                                    #
#########################################################################################################################
SCRIBENGIN_HOME="/opt/neverwinterdp/scribengin"
JAVA_OPTS="-Xshare:auto -Xms128m -Xmx1024m -XX:-UseSplitVerifier" 
APP_OPT="-Dapp.dir=$APP_DIR -Duser.dir=$APP_DIR"
APP_OPT="$APP_OPT -Dshell.zk-connect=zookeeper-1:2181 -Dshell.hadoop-master=hadoop-master"


DFS_APP_HOME="/applications/tracking-sample"
TRACKING_REPORT_PATH="/applications/tracking-sample/reports"

GENERATOR_NUM_OF_CHUNK=$(get_opt --generator-num-of-chunk '10' $@)
GENERATOR_NUM_OF_MESSAGE_PER_CHUNK=$(get_opt --generator-num-of-message-per-chunk '100000' $@)
GENERATOR_NUM_OF_WRITER=$(get_opt --generator-num-of-writer '3' $@)
GENERATOR_BREAK_IN_PERIOD=$(get_opt --generator-break-in-period '50' $@)
GENERATOR_MESSAGE_SIZE=$(get_opt  --generator-message-size '512' $@)
GENERATOR_NUM_OF_KAFKA_PARTITION=$(get_opt --generator-num-of-kafka-partition '8' $@)
GENERATOR_NUM_OF_KAFKA_REPLICATION=$(get_opt --generator-num-of-kafka-replication '2' $@)
GENERATOR_MAX_WAIT_TIME=$(get_opt --generator-max-wait-time '15000' $@)

DATAFLOW_STORAGE=$(get_opt --dataflow-storage 'kafka' $@)

DATAFLOW_NUM_OF_WORKER=$(get_opt --dataflow-num-of-worker '2' $@)
DATAFLOW_NUM_OF_EXECUTOR_PER_WORKER=$(get_opt --dataflow-num-of-executor-per-worker '2' $@)

DATAFLOW_KILL_WORKER_RANDOM=$(get_opt --kill-worker-random 'false' $@)
DATAFLOW_KILL_WORKER_MAX=$(get_opt --kill-worker-max '5' $@)
DATAFLOW_KILL_WORKER_PERIOD=$(get_opt --kill-worker-period '60000' $@)


DATAFLOW_DESCRIPTOR_FILE=""
VALIDATOR_SOURCE_OPT=""
if [ "$DATAFLOW_STORAGE" = "hdfs" ] ; then
  DATAFLOW_DESCRIPTOR_FILE="$APP_DIR/conf/chain/hdfs-tracking-dataflow-chain.json"
  LOG_VALIDATOR_VALIDATE_OPT="--prop:validate-hdfs=/tracking-sample/hdfs/info,/tracking-sample/hdfs/warn,/tracking-sample/hdfs/error"
elif [ "$DATAFLOW_STORAGE" = "s3" ] ; then
  DATAFLOW_DESCRIPTOR_FILE="$APP_DIR/conf/chain/s3-tracking-dataflow-chain.json"
  LOG_VALIDATOR_VALIDATE_OPT="--prop:validate-s3=test-tracking-sample:info,test-tracking-sample:warn,test-tracking-sample:error" 
else
  DATAFLOW_DESCRIPTOR_FILE="$APP_DIR/conf/chain/kafka-tracking-dataflow-chain.json"
  VALIDATOR_SOURCE_OPT="--prop:kafka.zk-connects=zookeeper-1:2181  --prop:kafka.topic=tracking.aggregate  --prop:kafka.message-wait-timeout=1200000"
fi

DATAFLOW_DEFAULT_RUNTIME=$(( 180000 + (($GENERATOR_NUM_OF_CHUNK * $GENERATOR_NUM_OF_MESSAGE_PER_CHUNK) / 3) ))
DATAFLOW_MAX_RUNTIME=$(get_opt --max-run-time $DATAFLOW_DEFAULT_RUNTIME $@)

VALIDATOR_DISABLE=$(has_opt "--validator-disable" $@ )
VALIDATOR_NUM_OF_READER=$(get_opt --validator-num-of-reader '3' $@)

MONITOR_MAX_RUNTIME=$(get_opt --monitor-max-runtime '60000' $@)


SHELL=$NEVERWINTERDP_BUILD_DIR/scribengin/bin/shell.sh


#########################################################################################################################
# Upload The App                                                                                                        #
#########################################################################################################################
$SHELL vm upload-app --local $APP_DIR --dfs $DFS_APP_HOME

#########################################################################################################################
# Launch The Message Generator                                                                                          #
#########################################################################################################################
$SHELL vm submit \
  --dfs-app-home $DFS_APP_HOME \
  --registry-connect zookeeper-1:2181  --registry-db-domain /NeverwinterDP  --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl \
  --name vm-tracking-generator-1 --role vm-tracking-generator --vm-application  com.neverwinterdp.scribengin.dataflow.tool.tracking.VMTMGeneratorKafkaApp \
  --prop:tracking.report-path=$TRACKING_REPORT_PATH \
  --prop:tracking.num-of-writer=$GENERATOR_NUM_OF_WRITER \
  --prop:tracking.num-of-chunk=$GENERATOR_NUM_OF_CHUNK \
  --prop:tracking.num-of-message-per-chunk=$GENERATOR_NUM_OF_MESSAGE_PER_CHUNK \
  --prop:tracking.break-in-period=$GENERATOR_BREAK_IN_PERIOD \
  --prop:tracking.message-size=$GENERATOR_MESSAGE_SIZE \
  --prop:kafka.zk-connects=zookeeper-1:2181 \
  --prop:kafka.topic=tracking.input \
  --prop:kafka.num-of-partition=$GENERATOR_NUM_OF_KAFKA_PARTITION \
  --prop:kafka.replication=$GENERATOR_NUM_OF_KAFKA_REPLICATION

$SHELL vm wait-for-vm-status --vm-id vm-tracking-generator-1 --vm-status TERMINATED --max-wait-time $GENERATOR_MAX_WAIT_TIME

#########################################################################################################################
# Launch A Dataflow Chain                                                                                               #
#########################################################################################################################
$SHELL dataflow submit-chain \
  --dfs-app-home $DFS_APP_HOME \
  --dataflow-chain-config $DATAFLOW_DESCRIPTOR_FILE \
  --dataflow-max-runtime $DATAFLOW_MAX_RUNTIME  --dataflow-num-of-worker $DATAFLOW_NUM_OF_WORKER --dataflow-num-of-executor-per-worker $DATAFLOW_NUM_OF_EXECUTOR_PER_WORKER \
  --wait-for-running-timeout 180000 

if [ "$DATAFLOW_KILL_WORKER_RANDOM" = "true" ] ; then
  $SHELL dataflow kill-worker-random \
    --dataflow-id tracking-splitter-dataflow,tracking-persister-dataflow-info,tracking-persister-dataflow-warn,tracking-persister-dataflow-error \
    --wait-before-simulate-failure 60000 --failure-period $DATAFLOW_KILL_WORKER_PERIOD --max-kill $DATAFLOW_KILL_WORKER_MAX &
fi

#########################################################################################################################
# Launch Validator                                                                                                      #
#########################################################################################################################
if [ $VALIDATOR_DISABLE == "false" ] ; then
  $SHELL vm submit  \
    --dfs-app-home $DFS_APP_HOME \
    --registry-connect zookeeper-1:2181  --registry-db-domain /NeverwinterDP --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl \
    --name vm-tracking-validator-1 --role tracking-validator --vm-application  com.neverwinterdp.scribengin.dataflow.tool.tracking.VMTMValidatorKafkaApp \
    --prop:tracking.report-path=$TRACKING_REPORT_PATH \
    --prop:tracking.num-of-reader=$VALIDATOR_NUM_OF_READER \
    --prop:tracking.expect-num-of-message-per-chunk=$GENERATOR_NUM_OF_MESSAGE_PER_CHUNK \
    --prop:tracking.max-runtime=$(( 180000 + $DATAFLOW_MAX_RUNTIME ))\
    $VALIDATOR_SOURCE_OPT

  $SHELL vm wait-for-vm-status --vm-id vm-tracking-validator-1 --vm-status TERMINATED --max-wait-time 5000
fi
#########################################################################################################################
# MONITOR                                                                                                               #
#########################################################################################################################
MONITOR_COMMAND="\
$SHELL plugin com.neverwinterdp.scribengin.dataflow.tool.tracking.TrackingMonitor \
  --dataflow-id tracking-splitter-dataflow,tracking-persister-dataflow-info,tracking-persister-dataflow-warn,tracking-persister-dataflow-error \
  --report-path $TRACKING_REPORT_PATH --max-runtime $MONITOR_MAX_RUNTIME --print-period 15000"

echo -e "\n\n"
echo "##To Tracking The Dataflow Progress##"
echo "-------------------------------------"
echo "$MONITOR_COMMAND"
echo -e "\n\n"

$MONITOR_COMMAND

echo -e "\n\n"
echo "##To Tracking The Dataflow Progress##"
echo "-------------------------------------"
echo "$MONITOR_COMMAND"
echo -e "\n\n"
