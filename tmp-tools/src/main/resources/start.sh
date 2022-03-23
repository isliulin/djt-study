#!/bin/bash

DEPLOY_DIR=$(pwd)
CONF_DIR=$DEPLOY_DIR/config
JAR=${artifactId}-${version}.${packaging}
LOCATION=$DEPLOY_DIR/lib/$JAR

LOG=-Dlog4j.configurationFile=$CONF_DIR/log4j2.xml
ARGS=$1

nohup java -Xms512m -Xmx1g -Xmn256m -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+CMSClassUnloadingEnabled -XX:SurvivorRatio=8 -XX:+DisableExplicitGC -verbose:gc -Xloggc:$DEPLOY_DIR/gc.log -XX:+PrintGCDetails -XX:-OmitStackTraceInFastThrow $LOG -jar $LOCATION $ARGS > /dev/null 2>&1 &

echo "Start successful..."