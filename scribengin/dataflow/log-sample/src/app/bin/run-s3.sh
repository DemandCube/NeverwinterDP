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

MAIN_CLASS="com.neverwinterdp.dataflow.logsample.LogSampleClient"

PROFILE=$(get_opt --profile 'unknown' $@)
MESSAGE_SIZE=$(get_opt --message-size '128' $@)
NUM_OF_MESSAGE=$(get_opt --num-of-message '100000' $@)
DEDICATED_EXECUTOR=$(get_opt --dedicated-executor 'false' $@)

if [ "$PROFILE" = "performance" ] ; then
  MAX_RUN_TIME=$(( 180000 + ($NUM_OF_MESSAGE / 5) ))
  $JAVACMD -Djava.ext.dirs=$APP_DIR/libs:$SCRIBENGIN_HOME/libs:$JAVA_HOME/jre/lib/ext $JAVA_OPTS $APP_OPT $LOG_OPT $MAIN_CLASS \
    --registry-connect zookeeper-1:2181 \
    --registry-db-domain /NeverwinterDP \
    --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl \
    --upload-app $APP_DIR --dfs-app-home /applications/dataflow/log-sample \
    --log-generator-num-of-vm 1 --log-generator-wait-for-ready 30000 \
    --log-generator-num-of-message $NUM_OF_MESSAGE --log-generator-message-size $MESSAGE_SIZE \
    --log-validator-wait-for-termination 1800000 --log-validator-validate-s3 test-log-sample:info,test-log-sample:warn,test-log-sample:error \
    --dataflow-descriptor $APP_DIR/conf/s3-log-dataflow-chain.json  \
    --dataflow-wait-for-submit-timeout 210000 --dataflow-wait-for-termination-timeout $MAX_RUN_TIME \
    --dataflow-task-dedicated-executor $DEDICATED_EXECUTOR \
    --dataflow-task-debug
elif [ "$PROFILE" = "dataflow-worker-failure" ] ; then
  MAX_RUN_TIME=$(( 180000 + ($NUM_OF_MESSAGE / 2) ))
  $JAVACMD -Djava.ext.dirs=$APP_DIR/libs:$SCRIBENGIN_HOME/libs:$JAVA_HOME/jre/lib/ext $JAVA_OPTS $APP_OPT $LOG_OPT $MAIN_CLASS \
    --registry-connect zookeeper-1:2181 \
    --registry-db-domain /NeverwinterDP \
    --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl \
    --upload-app $APP_DIR --dfs-app-home /applications/dataflow/log-sample \
    --log-generator-num-of-vm 1  --log-generator-wait-for-ready 30000 \
    --log-generator-num-of-message $NUM_OF_MESSAGE --log-generator-message-size $MESSAGE_SIZE \
    --log-validator-wait-for-termination 1800000 --log-validator-validate-s3 test-log-sample:info,test-log-sample:warn,test-log-sample:error \
    --dataflow-descriptor $APP_DIR/conf/s3-log-dataflow-chain.json  \
    --dataflow-wait-for-submit-timeout 210000 --dataflow-wait-for-termination-timeout $MAX_RUN_TIME \
    --dataflow-task-dedicated-executor $DEDICATED_EXECUTOR \
    --dataflow-failure-simulation-worker  \
    --dataflow-failure-simulation-wait-before-start 210000 \
    --dataflow-failure-simulation-max-kill 5 \
    --dataflow-failure-simulation-period 180000 \
    --dataflow-task-debug
else
  echo "Usage: "
  echo "  run-s3.sh --profile=[performance, dataflow-worker-failure] --message-size=[128, 256, 512....]"
fi


