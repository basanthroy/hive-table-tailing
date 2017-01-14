#!/bin/bash

APP_HOME=/opt/dwradiumone/kochava
PYTHON=/opt/python2.7/bin/python
RT_SCRIPT_HOME=/opt/dwradiumone/r1-dw-connect-app/prod/tracker_event_stj/scripts/src/main/python/integration/kochava/real_time
PYTHONLIBDIR=/opt/dwradiumone/r1-dw-connect-app/prod/tracker_event_stj/scripts/src/main/python
SCRIPT=${RT_SCRIPT_HOME}/real_time_process.py

ENTITY_NAME=kochava_tracker_event

function log
{
        now="`date +'%Y%m%d %H:%M:%S'`"
        echo $* | sed "s/^/$now:$$:   /"  >> ${APP_HOME}/job.log
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
fi

export PYTHONPATH="$RT_SCRIPT_HOME:${PYTHONLIBDIR}:${APP_HOME}:${PYTHONPATH}"
current_date_time="`date +%Y%m%d%H%M%S`"
log "current date time = $current_date_time , dt = $dt, hr = $hr"
process_name=$dt'_'$hr'_'$current_date_time
log "process_name = $process_name"

echo PYTHONPATH=$PYTHONPATH
env | egrep 'R1DT|R1RF'
echo "$PYTHON $SCRIPT $dt $hr $process_name $ENTITY_NAME 2> >(grep -v 'Warning: Duplicate entry'| grep -v 'report_db_connect.execute(insert_filename)'| grep -v 'INFO lzo.GPLNativeCodeLoader: Loaded native gpl library'| grep -v 'INFO lzo.LzoCodec: Successfully loaded & initialized native-lzo library' >&2)"
$PYTHON $SCRIPT $dt $hr $process_name $ENTITY_NAME 2> >(grep -v 'Warning: Duplicate entry'| grep -v 'report_db_connect.execute(insert_filename)'| grep -v 'INFO lzo.GPLNativeCodeLoader: Loaded native gpl library'| grep -v 'INFO lzo.LzoCodec: Successfully loaded & initialized native-lzo library' >&2)
rc=$?


exit ${rc}

