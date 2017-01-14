#!/bin/bash


APP_HOME=/opt/dwradiumone/r1-dw-connect-app/dev/bid_overlap_media_types
PYTHON=/opt/python2.7/bin/python
SCRIPT_HOME=$APP_HOME/scripts/src/main/python
SCRIPT=integration/keen/real_time/real_time_process.py
ENTITY_NAME=bid_overlap_media_types

function log
{
        now="`date +'%Y%m%d %H:%M:%S'`"
        echo $* | sed "s/^/$now:$$:   /"  >> ${APP_HOME}/log/rt/job.log
}

function logerror
{
   log "ERROR: $*"
}

# called by data_triggers framework
export PYTHONPATH="$SCRIPT_HOME"
dt="`date +%Y%m%d -d "yesterday"`"
hr="`date +%Y%m%d%H -d "yesterday"`"
log "Called from cron, dt = $dt , hr=$hr"
current_date_time="`date +%Y%m%d%H%M%S`"
log "current date time = $current_date_time , dt = $dt, hr = $hr"
process_name=$dt'_'$hr'_'$current_date_time
log "process_name = $process_name"
$PYTHON $SCRIPT_HOME/$SCRIPT $dt $hr $process_name $ENTITY_NAME 2> >(grep -v 'Warning: Duplicate entry'| grep -v 'report_db_connect.execute(insert_filename)'| grep -v 'INFO lzo.GPLNativeCodeLoader: Loaded native gpl library'| grep -v 'INFO lzo.LzoCodec: Successfully loaded & initialized native-lzo library' >&2)

exit 0
