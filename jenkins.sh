#!/usr/bin/env bash

PATH="/opt/python2.7/bin:/opt/python2.7/site-packages:$PATH"
export R1DT_TIMESLOT_START_HR=2016100400
echo 'The R1DT_TIMESLOT_START_HR environment variable is: '  ${R1DT_TIMESLOT_START_HR}

#touch /Users/tmitchell/r1-dw-mobile-lowlatency-app/src/main/python/integration/config/settings.yaml
#python /Users/tmitchell/r1-dw-mobile-lowlatency-app/myEnv/bin/config_gen -e test -d /Users/tmitchell/r1-dw-mobile-lowlatency-app/src/main/python/integration/config
pyb clean install_dependencies publish
