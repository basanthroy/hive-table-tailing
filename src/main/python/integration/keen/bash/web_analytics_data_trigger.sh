#!/bin/bash


APP_HOME=/opt/dwradiumone/r1-dw-mobile-lowlatency-app/test/web_analytics
PYTHON=/opt/python2.7/bin/python
SCRIPT_HOME=$APP_HOME/scripts/src/main/python
SCRIPT=integration/keen/real_time/real_time_process.py
ENTITY_NAME=web_analytics

function log
{
        now="`date +'%Y%m%d %H:%M:%S'`"
        echo $* | sed "s/^/$now:$$:   /"  >> ${APP_HOME}/log/rt/job.log
}

function logerror
{
   log "ERROR: $*"
}


hr=''
if [ -z "${R1DT_TIMESLOT_START_HR}" ]
then
  # called without data_triggers framework
  export hr=$1
  export dt=`echo $hr | sed 's/..$//'`
  export job_run_id=23
  log "Called without data_triggers framework. hr=$hr"
else
  # called by data_triggers framework
  export dt=`echo ${R1DT_TIMESLOT_START_HR} | sed 's/..$//'`
  export hr=${R1DT_TIMESLOT_START_HR}
  export job_run_id=${R1DT_JOBRUN_ID}
  log "Called from data_triggers framework. hr=$hr"
  export PYTHONPATH="$SCRIPT_HOME"
  current_date_time="`date +%Y%m%d%H%M%S`"
  log "current date time = $current_date_time , dt = $dt, hr = $hr"
  process_name=$dt'_'$hr'_'$current_date_time
  log "process_name = $process_name"
  $PYTHON $SCRIPT_HOME/$SCRIPT $dt $hr $process_name $ENTITY_NAME 2> >(grep -v 'Warning: Duplicate entry'| grep -v 'report_db_connect.execute(insert_filename)'| grep -v 'INFO lzo.GPLNativeCodeLoader: Loaded native gpl library'| grep -v 'INFO lzo.LzoCodec: Successfully loaded & initialized native-lzo library' >&2)
fi


exit 0
