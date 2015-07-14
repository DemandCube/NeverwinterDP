#!/bin/bash

cygwin=false
ismac=false
case "`uname`" in
  CYGWIN*) cygwin=true;;
  Darwin) ismac=true;;
esac

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

export HADOOP_USER_NAME="neverwinterdp"

APP_DIR=`cd $bin/..; pwd; cd $bin`
JAVACMD=$JAVA_HOME/bin/java

if [ "x$JAVA_HOME" == "x" ] ; then 
  echo "WARNING JAVA_HOME is not set"
fi

(which $JAVACMD)
isjava=$?

if $ismac && [ $isjava -ne 0 ] ; then
  which java
  if [ $? -eq 0 ] ; then
    JAVACMD=`which java`
    echo "Defaulting to java: $JAVACMD"
  else 
    echo "JAVA Command (java) Not Found Exiting"
    exit 1
  fi
fi

if $cygwin; then
  APP_DIR=`cygpath --absolute --windows "$APP_DIR"`
fi

SCRIBENGIN_HOME="/opt/neverwinterdp/scribengin"
JAVA_OPTS="-Xshare:auto -Xms128m -Xmx1536m -XX:-UseSplitVerifier" 
APP_OPT="-Dapp.dir=$APP_DIR -Duser.dir=$APP_DIR"
APP_OPT="$APP_OPT -Dshell.zk-connect=zookeeper-1:2181 -Dshell.hadoop-master=hadoop-master"

MAIN_CLASS="com.neverwinterdp.dataflow.logsample.LogSampleClient"
$JAVACMD -Djava.ext.dirs=$APP_DIR/libs:$SCRIBENGIN_HOME/libs:$JAVA_HOME/jre/lib/ext $JAVA_OPTS $APP_OPT $LOG_OPT $MAIN_CLASS \
  --registry-connect zookeeper-1:2181 \
  --registry-db-domain /NeverwinterDP \
  --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl \
  --upload-app $APP_DIR --dfs-app-home /applications/dataflow/log-sample \
  --log-generator-num-of-vm 1 --log-generator-num-of-executor-per-vm 4 --log-generator-num-of-message-per-executor 2500000 --log-generator-message-size 512 \
  --log-validator-num-of-executor-per-vm 3 --log-validator-wait-for-message-timeout 15000 --log-validator-wait-for-termination 600000 \
  --dataflow-descriptor $APP_DIR/conf/kafka-log-dataflow-chain.json  \
  --dataflow-wait-for-submit-timeout 150000 --dataflow-wait-for-termination-timeout 5400000 \
  --dataflow-task-debug

