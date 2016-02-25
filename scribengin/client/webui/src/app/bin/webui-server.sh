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

JAVA_OPTS="-Xshare:auto -Xms128m -Xmx512m -XX:-UseSplitVerifier" 
APP_OPT="-Dapp.dir=$APP_DIR -Duser.dir=$APP_DIR"

echo "***********************************************************************************************************"
echo "Run the shell command with the settings:"
echo "  JAVA_OPTS                   = $JAVA_OPTS"
echo "  APP_OPT                     = $APP_OPT"
echo "***********************************************************************************************************"

MAIN_CLASS="com.neverwinterdp.scribengin.webui.WebuiServer"
$JAVACMD -Djava.ext.dirs=$APP_DIR/libs:$JAVA_HOME/jre/lib/ext $JAVA_OPTS $APP_OPT $MAIN_CLASS  --www-dir $APP_DIR/webapp --zk-connects 127.0.0.1;2181 "$@"
