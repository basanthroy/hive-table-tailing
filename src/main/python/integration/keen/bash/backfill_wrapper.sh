#!/bin/bash


APP_HOME=/opt/dwradiumone/r1-dw-connect-app/dev/tracker_event_stj
PYTHON=/opt/python2.7/bin/python
SCRIPT_HOME=$APP_HOME/scripts/src/main/python
SCRIPT=integration/keen/backfill/backfill_wrapper.py

function log
{
        now="`date +'%Y%m%d %H:%M:%S'`"
        echo $* | sed "s/^/$now:$$:   /"  >> ${APP_HOME}/log/rt/job.log
}

function logerror
{
   log "ERROR: $*"
}


log "Executing backfill_wrapper.sh"
export PYTHONPATH="$SCRIPT_HOME"
$PYTHON $SCRIPT_HOME/$SCRIPT

exit 0
