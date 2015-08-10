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


SCRIBENGIN_HOME="/opt/neverwinterdp/scribengin"
JAVA_OPTS="-Xshare:auto -Xms128m -Xmx1024m -XX:-UseSplitVerifier" 
APP_OPT="-Dapp.dir=$APP_DIR -Duser.dir=$APP_DIR"
APP_OPT="$APP_OPT -Dshell.zk-connect=zookeeper-1:2181 -Dshell.hadoop-master=hadoop-master"

MAIN_CLASS="com.neverwinterdp.dataflow.logsample.chain.LogSampleChainClient"

PROFILE=$(get_opt --profile 'performance' $@)
MESSAGE_SIZE=$(get_opt --message-size '128' $@)
NUM_OF_MESSAGE=$(get_opt --num-of-message '100000' $@)

STORAGE=$(get_opt --storage 'kafka' $@)


DATAFLOW_DESCRIPTOR_FILE=""
LOG_VALIDATOR_VALIDATE=""
if [ "$STORAGE" = "hdfs" ] ; then
  DATAFLOW_DESCRIPTOR_FILE="$APP_DIR/conf/chain/hdfs-log-dataflow-chain.json"
  LOG_VALIDATOR_VALIDATE_OPT="--log-validator-validate-hdfs /log-sample/hdfs/info,/log-sample//hdfs/warn,/log-sample/hdfs/error"
elif [ "$STORAGE" = "s3" ] ; then
  DATAFLOW_DESCRIPTOR_FILE="$APP_DIR/conf/chain/s3-log-dataflow-chain.json"
  LOG_VALIDATOR_VALIDATE_OPT="--log-validator-validate-s3 test-log-sample:info,test-log-sample:warn,test-log-sample:error" 
else
  DATAFLOW_DESCRIPTOR_FILE="$APP_DIR/conf/chain/kafka-log-dataflow-chain.json"
  LOG_VALIDATOR_VALIDATE_OPT="--log-validator-validate-kafka log4j.aggregate"
fi


SHELL=./scribengin/bin/shell.sh


$SHELL vm upload-app --local $APP_DIR --dfs /applications/log-sample

$SHELL vm submit \
   --dfs-app-home /applications/log-sample \
   --registry-connect zookeeper-1:2181 --registry-db-domain /NeverwinterDP --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl \
   --name vm-log-generator-1  --role vm-log-generator --vm-application  com.neverwinterdp.dataflow.logsample.vm.VMToKafkaLogMessageGeneratorApp \
   --prop:report-path=/applications/log-sample/reports --prop:num-of-message=$NUM_OF_MESSAGE --prop:message-size=$MESSAGE_SIZE


$SHELL dataflow submit \
  --dfs-app-home /applications/log-sample \
  --dataflow-config $APP_DIR/conf/splitter/kafka-log-splitter-dataflow.json \
  --dataflow-id log-splitter-dataflow-1 --max-run-time 180000

$SHELL vm wait-for-vm-status --vm-id vm-log-generator-1 --vm-status TERMINATED --max-wait-time 45000
$SHELL registry dump --path /applications/log-sample

$SHELL dataflow wait-for-status --dataflow-id log-splitter-dataflow-1 --status TERMINATED
$SHELL dataflow info --dataflow-id log-splitter-dataflow-1 --show-all


$SHELL vm submit  \
  --dfs-app-home /applications/log-sample \
  --registry-connect zookeeper-1:2181  --registry-db-domain /NeverwinterDP --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl \
  --name vm-log-validator-1 --role log-validator  --vm-application com.neverwinterdp.dataflow.logsample.vm.VMLogMessageValidatorApp \
  --prop:report-path=/applications/log-sample/reports \
  --prop:num-of-message-per-partition=5000 \
  --prop:wait-for-termination=300000 \
  --prop:validate-kafka=log4j.info,log4j.warn,log4j.error

$SHELL vm wait-for-vm-status --vm-id vm-log-validator-1 --vm-status TERMINATED --max-wait-time 60000

$SHELL vm info
$SHELL registry dump --path /applications/log-sample