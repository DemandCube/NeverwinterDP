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

if [ "x$SCRIBENGIN_REGISTRY_CONNECT" == "x" ] ; then 
  SCRIBENGIN_REGISTRY_CONNECT="zookeeper-1:2181"
fi
SCRIBENGIN_HADOOP_MASTER="hadoop-master"

JAVA_OPTS="-Xshare:auto -Xms128m -Xmx1024m -XX:-UseSplitVerifier" 
APP_OPT="-Dapp.dir=$APP_DIR -Duser.dir=$APP_DIR"
APP_OPT="$APP_OPT -Dshell.zk-connect=$SCRIBENGIN_REGISTRY_CONNECT -Dshell.hadoop-master=$SCRIBENGIN_HADOOP_MASTER"

echo "***********************************************************************************************************"
echo "Run the shell command with the settings:"
echo "  SCRIBENGIN_REGISTRY_CONNECT = $SCRIBENGIN_REGISTRY_CONNECT"
echo "  SCRIBENGIN_HADOOP_MASTER    = $SCRIBENGIN_HADOOP_MASTER"
echo "  JAVA_OPTS                   = $JAVA_OPTS"
echo "  APP_OPT                     = $APP_OPT"
echo "***********************************************************************************************************"

MAIN_CLASS="com.neverwinterdp.scribengin.ShellMain"
$JAVACMD -Djava.ext.dirs=$APP_DIR/libs:$JAVA_HOME/jre/lib/ext $JAVA_OPTS $APP_OPT $LOG_OPT $MAIN_CLASS "$@"
