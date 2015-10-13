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

GENERATOR_SEND_PERIOD=$(get_opt --generator-send-period '-1' $@)
GENERATOR_MAX_WAIT_TIME=$(get_opt --generator-max-wait-time '180000' $@)

STORAGE=$(get_opt --storage 'kafka' $@)

NUM_OF_WORKER=$(get_opt --num-of-worker '2' $@)
NUM_OF_EXECUTOR_PER_WORKER=$(get_opt --num-of-executor-per-worker '2' $@)
NUM_OF_STREAM=$(get_opt --num-of-stream '8' $@)
MESSAGE_SIZE=$(get_opt  --message-size '128' $@)
NUM_OF_MESSAGE=$(get_opt --num-of-message '100000' $@)

KILL_WORKER_RANDOM=$(get_opt --kill-worker-random 'false' $@)
KILL_WORKER_MAX=$(get_opt --kill-worker-max '5' $@)
KILL_WORKER_PERIOD=$(get_opt --kill-worker-period '60000' $@)

DUMP_PERIOD=$(get_opt --dump-period '15000' $@)
VALIDATOR_DISABLE=$(has_opt "--validator-disable" $@ )

TRACKING_REPORT_PATH="/applications/log-sample/reports"

DATAFLOW_DESCRIPTOR_FILE=""
LOG_VALIDATOR_VALIDATE=""
if [ "$STORAGE" = "hdfs" ] ; then
  DATAFLOW_DESCRIPTOR_FILE="$APP_DIR/conf/chain/hdfs-log-dataflow-chain.json"
  LOG_VALIDATOR_VALIDATE_OPT="--prop:validate-hdfs=/log-sample/hdfs/info,/log-sample/hdfs/warn,/log-sample/hdfs/error"
elif [ "$STORAGE" = "s3" ] ; then
  DATAFLOW_DESCRIPTOR_FILE="$APP_DIR/conf/chain/s3-log-dataflow-chain.json"
  LOG_VALIDATOR_VALIDATE_OPT="--prop:validate-s3=test-log-sample:info,test-log-sample:warn,test-log-sample:error" 
else
  DATAFLOW_DESCRIPTOR_FILE="$APP_DIR/conf/chain/kafka-log-dataflow-chain.json"
  LOG_VALIDATOR_VALIDATE_OPT="--prop:validate-kafka=log4j.aggregate"
fi

DEFAULT_RUNTIME=$(( 180000 + ($NUM_OF_MESSAGE / 3) ))
MAX_RUNTIME=$(get_opt --max-run-time $DEFAULT_RUNTIME $@)

SHELL=$NEVERWINTERDP_BUILD_DIR/scribengin/bin/shell.sh


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
   --prop:report-path=$TRACKING_REPORT_PATH \
   --prop:num-of-message=$NUM_OF_MESSAGE --prop:message-size=$MESSAGE_SIZE \
   --prop:num-of-stream=$NUM_OF_STREAM \
   --prop:send-period=$GENERATOR_SEND_PERIOD

$SHELL vm wait-for-vm-status --vm-id vm-log-generator-1 --vm-status TERMINATED --max-wait-time $GENERATOR_MAX_WAIT_TIME
#########################################################################################################################
# Launch A Dataflow Chain                                                                                               #
#########################################################################################################################
$SHELL dataflow submit-chain \
  --dfs-app-home /applications/log-sample \
  --dataflow-chain-config $DATAFLOW_DESCRIPTOR_FILE --wait-for-running-timeout 180000 --dataflow-max-runtime $MAX_RUNTIME \
  --dataflow-num-of-worker $NUM_OF_WORKER --dataflow-num-of-executor-per-worker $NUM_OF_EXECUTOR_PER_WORKER 

if [ "$KILL_WORKER_RANDOM" = "true" ] ; then
  $SHELL dataflow kill-worker-random \
    --dataflow-id log-splitter-dataflow,log-persister-dataflow-info,log-persister-dataflow-warn,log-persister-dataflow-error \
    --wait-before-simulate-failure 60000 --failure-period $KILL_WORKER_PERIOD --max-kill $KILL_WORKER_MAX &
fi

#########################################################################################################################
# Launch Validator                                                                                                      #
#########################################################################################################################
if [ $VALIDATOR_DISABLE == "false" ] ; then
  $SHELL vm submit  \
    --dfs-app-home /applications/log-sample \
    --registry-connect zookeeper-1:2181  --registry-db-domain /NeverwinterDP --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl \
    --name vm-log-validator-1 --role log-validator  --vm-application com.neverwinterdp.dataflow.logsample.vm.VMLogMessageValidatorApp \
    --prop:report-path=$TRACKING_REPORT_PATH \
    --prop:num-of-message-per-partition=$NUM_OF_MESSAGE \
    --prop:wait-for-termination=3600000 \
    $LOG_VALIDATOR_VALIDATE_OPT

  $SHELL vm wait-for-vm-status --vm-id vm-log-validator-1 --vm-status TERMINATED --max-wait-time 3600000
fi
#########################################################################################################################
# MONITOR                                                                                                               #
#########################################################################################################################
MONITOR_COMMAND="\
$SHELL plugin com.neverwinterdp.scribengin.dataflow.tool.tracking.TrackingMonitor \
  --dataflow-id log-splitter-dataflow,log-persister-dataflow-info,log-persister-dataflow-warn,log-persister-dataflow-error \
  --report-path $TRACKING_REPORT_PATH --max-runtime 60000 --print-period 10000"

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
