#!/bin/bash

if [ "x$JAVA_HOME" == "x" ] ; then 
  echo "WARNING JAVA_HOME is not set"
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
APP_DIR=`cd $bin/..; pwd; cd $bin`
SCRIBENGIN_APP_DIR="$APP_DIR/../../scribengin"

export HADOOP_USER_NAME="neverwinterdp"

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

JAVACMD=$JAVA_HOME/bin/java
JAVA_OPTS="-Xshare:auto -Xms128m -Xmx512m -XX:-UseSplitVerifier" 

MAIN_CLASS="com.neverwinterdp.scribengin.dataflow.example.simple.SimpleDataflowExample"

echo "Command: $JAVACMD -Djava.ext.dirs=$SCRIBENGIN_APP_DIR/libs:$APP_DIR/libs:$JAVA_HOME/jre/lib/ext $JAVA_OPTS $MAIN_CLASS $@"

$JAVACMD -Djava.ext.dirs=$SCRIBENGIN_APP_DIR/libs:$APP_DIR/libs:$JAVA_HOME/jre/lib/ext $JAVA_OPTS $MAIN_CLASS "$@"
