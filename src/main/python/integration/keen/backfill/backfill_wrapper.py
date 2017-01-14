__author__ = 'broy'

import logging
import MySQLdb
import traceback
import os

from integration.keen.common.init import Init
from integration.keen.common import config

class BackfillWrapper(Init):

    def __init__(self):
        super(BackfillWrapper, self).__init__(_check_processname_uniqueness=False)

    def _isEligibleToRun(self):

        logging.info("\nisEligibleToRun invoked...")

        with MySQLdb.connect(config.report_db_connect_host,
                                         config.report_db_connect_user,
                                         config.report_db_connect_password,
                                         config.report_db_connect_db) as report_db_connect:
            logging.info("insert_file_names_to_state, report_db_connect={}".format(report_db_connect))

            try:

                _is_process_running = """
                        select count(*)
                        from connect_rt_file_process
                        where status <> 'processed'
                        and dt < 20160915
                        """
                logging.info("_is_process_running={}".format(_is_process_running))

                report_db_connect.execute(_is_process_running)

                _files_being_processed = [row for row in report_db_connect]
                logging.info("_files_being_processed={}".format(_files_being_processed))
                if _files_being_processed[0][0] > 0:
                    return False, "NA", "NA"
                else:
                    _num_hours_processed = """
                                            select dt, count(distinct hr)
                                            from connect_rt_file_process
                                            where dt = (select min(dt)
                                                        from connect_rt_file_process)
                                            """
                    logging.info("_num_hours_processed={}".format(_is_process_running))

                    report_db_connect.execute(_num_hours_processed)

                    _hours_processed = [row for row in report_db_connect]
                    _min_dt = _hours_processed[0][0]
                    _hours_processed = _hours_processed[0][1]

                    return True, _hours_processed, _min_dt

            except:
                logging.error('exception encountered when insert_file_names_to_state...')
                logging.error(traceback.format_exc())
            # app_db_connect.commit()

        logging.info("\ninsert_file_names_to_state invocation completed...")

    def invoke_runner(self):

        logging.info("In _invoke_runner method")

        _status, _hours_processed, _min_dt = self._isEligibleToRun()
        #_status, _hours_processed, _min_dt = True, 24, 20160403

        logging.info("_status={}, _hours_processed={}, _min_dt = {}"
                     .format(_status, _hours_processed, _min_dt))

        if _status == True:
            if _hours_processed == 12:
                os.system(config.SCRIPT_DIR + "/bash/historic_tracker_event_12_23.sh {}"
                          .format(_min_dt))
            else:
                _next_date = BackfillWrapper.get_day_older_date(_min_dt)
                if _next_date == 20151231:
                    logging.info("Beginning reached. {}".format(_next_date))
                    return
                os.system(config.SCRIPT_DIR + "/bash/historic_tracker_event_00_11.sh {}"
                          .format(_next_date))
                os.system(config.SCRIPT_DIR + "/bash/historic_tracker_event_12_23.sh {}"
                          .format(_next_date))

    @staticmethod
    def get_day_older_date(_min_dt):
        _new_date = _min_dt - 1
        if _new_date % 100 == 0:
            _new_date = (100 * (_new_date/100 - 1))
            _new_date = _new_date + BackfillWrapper.getLastDayOfMonth(_new_date)
        logging.info("_min_dt = {}, _new_date = {}".format(_min_dt, _new_date))
        return _new_date

    @staticmethod
    def getLastDayOfMonth(_date):
        month = (_date - 20160000) / 100
        if month == 2:
            return 29
        if month in [1,3,5,7,8,10,12]:
            return 31
        return 30

if __name__ == "__main__":
    _backfillWrapper = BackfillWrapper()
    _backfillWrapper.invoke_runner()

    # _backfillWrapper.get_day_older_date(20161114)
