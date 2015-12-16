#!/bin/bash

mkdir $(dirname $0)/../logs

LOGFILE=$(dirname $0)/../logs/hippo_server.log

nohup sh $(dirname $0)/runclass.sh com.pinganfu.hippo.bootstrap.Main $@ 2>&1 >>$LOGFILE &

sleep 1

tail -f $LOGFILE
