#!/bin/bash

cygwin=false
ismac=false
case "`uname`" in
  CYGWIN*) cygwin=true;;
  Darwin) ismac=true;;
esac

if [ "x$JAVA_HOME" == "x" ] ; then 
  echo "WARNING JAVA_HOME is not set"
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

export HADOOP_USER_NAME="neverwinterdp"

APP_DIR=`cd $bin/..; pwd; cd $bin`
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


#########################################################################################################################
# Setup environment and collect the setup parameters                                                                    #
#########################################################################################################################
SCRIBENGIN_HOME="/opt/neverwinterdp/scribengin"
JAVA_OPTS="-Xshare:auto -Xms128m -Xmx1024m -XX:-UseSplitVerifier" 
APP_OPT="-Dapp.dir=$APP_DIR -Duser.dir=$APP_DIR"
APP_OPT="$APP_OPT -Dshell.zk-connect=zookeeper-1:2181 -Dshell.hadoop-master=hadoop-master"

PROFILE=$(get_opt --profile 'kafka-to-kafka' $@)
STORAGE=$(get_opt --storage 'kafka' $@)
MESSAGE_SIZE=$(get_opt --message-size '128' $@)
NUM_OF_MESSAGE=$(get_opt --num-of-message '100000' $@)


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

MAX_RUNTIME=$(( 180000 + ($NUM_OF_MESSAGE / 5) ))

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
echo "Dataflow Chain ElAPSED TIME: $DATAFLOW_CHAIN_ELAPSED_TIME" 

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
echo "MESSAGE GENERATION TIME: $MESSAGE_VALIDATION_ELAPSED_TIME" 
#########################################################################################################################
# Dump the vm and registry info                                                                                         #
#########################################################################################################################
$SHELL vm info
$SHELL registry dump --path /applications/log-sample
