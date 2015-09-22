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

NUM_OF_WORKER=$(get_opt --num-of-worker '2' $@)
NUM_OF_EXECUTOR_PER_WORKER=$(get_opt --num-of-executor-per-worker '2' $@)
NUM_OF_STREAM=$(get_opt --num-of-stream '8' $@)
MESSAGE_SIZE=$(get_opt  --message-size '128' $@)
NUM_OF_MESSAGE=$(get_opt --num-of-message '100000' $@)

SHELL=./scribengin/bin/shell.sh


#########################################################################################################################
# Upload The App                                                                                                        #
#########################################################################################################################
$SHELL vm upload-app --local $APP_DIR --dfs /applications/log-sample

#########################################################################################################################
# Launch The Message Generator                                                                                          #
#########################################################################################################################
id=1
while :
do
  ((id++))
  START_MESSAGE_GENERATION_TIME=$SECONDS
  $SHELL vm submit \
     --dfs-app-home /applications/log-sample \
     --registry-connect zookeeper-1:2181 --registry-db-domain /NeverwinterDP --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl \
     --name vm-log-generator-$id  --role vm-log-generator --vm-application  com.neverwinterdp.dataflow.logsample.vm.VMToKafkaLogMessageGeneratorApp \
     --prop:report-path=/applications/log-sample/reports \
     --prop:num-of-message=$NUM_OF_MESSAGE --prop:message-size=$MESSAGE_SIZE \
     --prop:num-of-stream=$NUM_OF_STREAM \

  $SHELL vm wait-for-vm-status --vm-id vm-log-generator-$id --vm-status TERMINATED --max-wait-time 60000
  MESSAGE_GENERATION_ELAPSED_TIME=$(($SECONDS - $START_MESSAGE_GENERATION_TIME))
  echo "MESSAGE GENERATION TIME: $MESSAGE_GENERATION_ELAPSED_TIME"
done