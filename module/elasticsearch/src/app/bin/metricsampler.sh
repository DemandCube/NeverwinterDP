#!/bin/bash

cygwin=false
ismac=false
case "`uname`" in
  CYGWIN*) cygwin=true;;
  Darwin) ismac=true;;
esac

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

APP_DIR=`cd $bin/..; pwd; cd $bin`
JAVACMD=$JAVA_HOME/bin/java

LOG_OPT="-Dlog4j.configuration=file:$APP_DIR/config/log4j.properties"
APP_OPT="-Dapp.dir=$APP_DIR -Duser.dir=$APP_DIR"

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

CLASSPATH=$CLASSPATH:$APP_DIR/libs/*

MAIN_CLASS="com.neverwinterdp.es.log.sampler.MetricSamplerRunner"
$JAVACMD -Djava.ext.dirs=$APP_DIR/libs:$JAVA_HOME/jre/lib/ext -cp $CLASSPATH $APP_OPT $LOG_OPT $MAIN_CLASS --app-dir $APP_DIR "$@"
