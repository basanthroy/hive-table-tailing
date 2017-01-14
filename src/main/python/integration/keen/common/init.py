__author__ = 'broy'

import logging
import time
import sys

from integration.keen.common import config
from integration.keen.util.util import Util

processing_start_time = int(time.time())

class Init(object):

    def __init__(self, _check_processname_uniqueness=True):

        logging.basicConfig(filename=config.ROOT_DIR + '/keen.log',
                            level=config.loglevel,
                            format='%(asctime)s %(name)s (%(levelname)s): %(message)s')
        logging.info('\n\n\n Beginning keen integration pipeline main method ')

        logging.info('Starting connect keen pipeline, time={}, formatted time = {}'.format(str(processing_start_time), (
            time.strftime("%Y %m %e %H %M %S", time.localtime(processing_start_time)))))

        metrics = logging.getLogger("METRICS")

        handler = logging.FileHandler(filename=(config.ROOT_DIR + '/metrics.log'))
        handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        metrics.addHandler(handler)

        util = Util()
        if _check_processname_uniqueness:
            if util.is_process_name_unique(sys.argv[3]) is not True:
                self.print_usage()
                raise ValueError("Expected Unique process name. Received process name = {}".format(sys.argv[3]))

    def print_usage(self):
        print """Usage =
        python integration/keen/real_time/real_time_process.py start_date end_date unique_process_name table_name_alias
        For example
        python integration/keen/real_time/real_time_process.py 20160710 2016071012 rt_test tracker_event
        """