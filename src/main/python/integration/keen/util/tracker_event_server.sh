#!/bin/sh

if ps ax | grep -v grep | grep "tracker_event_server.py" > /dev/null
then
    date >> /opt/dwradiumone/arte/programmable_bidder/r1-dw-arte-app/application/src/main/python/com/radiumone/arte/programmable_bidder/nqdq/log/cron.log
    echo "previous pb dequeue run for prod not done yet. exit ..." >> /opt/dwradiumone/arte/programmable_bidder/r1-dw-arte-app/application/src/main/python/com/radiumone/arte/programmable_bidder/nqdq/log/cron.log
else
    date >> /opt/dwradiumone/arte/programmable_bidder/r1-dw-arte-app/application/src/main/python/com/radiumone/arte/programmable_bidder/nqdq/log/cron.log
    echo "previous pb dequeue run for prod completed. Starting a new one ..." >> /opt/dwradiumone/arte/programmable_bidder/r1-dw-arte-app/application/src/main/python/com/radiumone/arte/programmable_bidder/nqdq/log/cron.log
    export PYTHONPATH="/opt/dwradiumone/arte/programmable_bidder/r1-dw-arte-app"
    /opt/python2.7/bin/python /opt/dwradiumone/arte/programmable_bidder/r1-dw-arte-app/application/src/main/python/com/radiumone/arte/programmable_bidder/nqdq/pb_dq.py
fi
