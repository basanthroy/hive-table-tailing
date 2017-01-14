#!/bin/bash


#nohup /opt/python2.7/bin/python /opt/dwradiumone/r1-dw-connect-app/prod/tracker_event_stj/scripts/integration/keen/real_time/real_time_process.py 20161021 2016102123 basanth_repair_2016102123 tracker_event 2> >(grep -v 'Warning: Duplicate entry'| grep -v 'report_db_connect.execute(insert_filename)' >&2)

APP_HOME=/opt/dwradiumone/r1-dw-connect-app/prod/tracker_event_stj
PYTHON=/opt/python2.7/bin/python
SCRIPT_HOME=$APP_HOME/scripts/src/main/python
SCRIPT=integration/keen/real_time/real_time_process.py
ENTITY_NAME=tracker_event_backfill

function log
{
        now="`date +'%Y%m%d %H:%M:%S'`"
        echo $* | sed "s/^/$now:$$:   /"  >> ${APP_HOME}/log/rt/historic.log
}

function logerror
{
   log "ERROR: $*"
}

dt=$1
export PYTHONPATH="$SCRIPT_HOME"
current_date_time="`date +%Y%m%d%H%M%S`"
log "current date time = $current_date_time , dt = $dt, hr = $hr"

if [[ -n "$dt" ]]; then

    COUNTER=0
    until [  $COUNTER -gt 11 ]; do
        #echo COUNTER $COUNTER
        if [ $COUNTER -lt 10 ]; then
          hr=$dt"0$COUNTER"
        else
          hr=$dt$COUNTER
        fi
        let COUNTER+=1
        #echo hr $hr
        process_name=$dt'_'$hr'_'$current_date_time
        log "process_name = $process_name"
        cmd="nohup $PYTHON $SCRIPT_HOME/$SCRIPT $dt $hr $process_name $ENTITY_NAME 2> >(grep -v 'Warning: Duplicate entry'| grep -v 'report_db_connect.execute(insert_filename)'| grep -v 'INFO lzo.GPLNativeCodeLoader: Loaded native gpl library'| grep -v 'INFO lzo.LzoCodec: Successfully loaded & initialized native-lzo library' >&2) &"
        log "cmd="$cmd
        eval $cmd
    done

else
    echo "argument error"
fi
