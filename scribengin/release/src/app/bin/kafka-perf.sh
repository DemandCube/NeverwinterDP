#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

export HADOOP_USER_NAME="neverwinterdp"

APP_DIR=`cd $bin/..; pwd; cd $bin`
JAVACMD=$JAVA_HOME/bin/java

if [ "x$JAVA_HOME" == "x" ] ; then 
  echo "WARNING JAVA_HOME is not set"
fi

JAVA_OPTS="-Xshare:auto -Xms128m -Xmx1536m -XX:-UseSplitVerifier" 
APP_OPT="-Dapp.dir=$APP_DIR -Duser.dir=$APP_DIR"

#--topics tracking.input,tracking.info,tracking.warn,tracking.error,tracking.aggregate \
ARGS="\
  --zk-connect zookeeper-1:2181 \
  \
  --topics tracking.input,tracking.info,tracking.warn,tracking.error,tracking.aggregate \
  --topic-num-of-message 30000000 \
  --topic-num-of-partition 8 --topic-num-of-replication 2 \
  \
  --writer-write-per-writer 50000  --writer-write-break-in-period 100 \
  \
  --reader-read-per-reader 50000 --reader-run-delay 30000 \
  \
  --max-runtime 7200000"

MAIN_CLASS="com.neverwinterdp.scribengin.storage.kafka.perftest.KafkaPerfTest"

$JAVACMD -Djava.ext.dirs=$APP_DIR/libs:$JAVA_HOME/jre/lib/ext $JAVA_OPTS $APP_OPT $LOG_OPT $MAIN_CLASS $ARGS "$@"
