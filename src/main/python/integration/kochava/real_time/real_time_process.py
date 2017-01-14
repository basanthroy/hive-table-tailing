__author__ = 'broy'

import random
import string
import sys
import logging
import os
import re
import time
import traceback
import MySQLdb
import pdb

from integration.keen.common import config
from integration.keen.common.init import Init
from integration.keen.common.app_metadata import AppMetadata
from integration.keen.common.metaclass_grant import metaclass_info
from integration.keen.historic.common.job_report import JobReport
from integration.keen.historic.common.rest_controller import RestController
from integration.keen.common.config import file_list_retriever, \
    entity_base_dir_template, entity_filepath_template, hive_line_parser

from subprocess import Popen, PIPE
import send_installs_datatech_6122

class RealTimeProcess(Init):

    def process(self, _dt, _hr):

        files = _file_list_retriever_class.get_file_names_in_dthr(_dt, _hr)
        self.__insert_file_names_to_state(files, _dt, _hr)

        _num_existing_processes = int(os.popen("ps -eaf | grep 'kochava/real_time/real_time_process.py' | grep -v grep | wc -l").read())

        if (_num_existing_processes > config.max_concurrent_python_processes_on_server):
            logging.info("Exiting this run since more than {} (exact count = {}) real_time processes are already running."
                         .format(config.max_concurrent_python_processes_on_server, _num_existing_processes))
            return

        logging.info("Continuing with this run since less than 5 (exact count = {}) real_time processes are already running."
            .format(_num_existing_processes))

        _unprocessed_file = self._get_oldest_file_to_process()

        while _unprocessed_file is not None:
            logging.info("unprocessed_files={}".format(_unprocessed_file))

            try:
                self._process_new_file(_unprocessed_file)
            finally:
                logging.info("Completed process for file = {}".format(_unprocessed_file))

            _unprocessed_file = self._get_oldest_file_to_process()

        logging.info("Completed processing all files in new status. Exiting...")

    def __insert_file_names_to_state(self, files_arg, dt, hr):

        logging.info("\ninsert_file_names_to_state invoked...")

        with MySQLdb.connect(config.report_db_connect_host,
                                         config.report_db_connect_user,
                                         config.report_db_connect_password,
                                         config.report_db_connect_db) as report_db_connect:
            logging.info("insert_file_names_to_state, report_db_connect={}".format(report_db_connect))
            logging.info("insert_file_names_to_state, files = {}".format(files_arg))

            try:

                for file in files_arg:

                    insert_filename = """
                        INSERT IGNORE INTO `connect_rt_file_process`
                        SET `dt`       = {},
                            `hr`       = {},
                            `filename` = '{}',
                            `status`   = 'new',
                            `process_name` = '{}',
                            `entity_name`  = '{}'
                        """.format(dt, hr, file, sys.argv[3], sys.argv[4])

                    # insert_filename = """
                    #     INSERT INTO
                    #     connect_rt_file_process(dt, hr, filename, status, process_name, entity_name)
                    #     values({}, {}, '{}', 'new', '{}', '{}')
                    #     ON DUPLICATE KEY UPDATE
                    #     process_name = concat(coalesce(process_name, ''), '{}')
                    # """.format(dt, hr, file, sys.argv[3], sys.argv[4], sys.argv[3])

                    logging.info("insert_filename={}".format(insert_filename))

                    report_db_connect.execute(insert_filename)

            except:
                logging.error('exception encountered when insert_file_names_to_state...')
                logging.error(traceback.format_exc())
            # app_db_connect.commit()

        logging.info("\ninsert_file_names_to_state invocation completed...")


    def _get_oldest_file_to_process(self):
        logging.info("\nget_list_of_files_to_process invoked...")

        oldest_file = None

        _generated_unique_name = ''.join(random.SystemRandom()
                                         .choice(string.ascii_uppercase + string.digits) for _ in range(20))

        with MySQLdb.connect(config.report_db_connect_host,
                             config.report_db_connect_user,
                             config.report_db_connect_password,
                             config.report_db_connect_db) as report_db_connect:

            logging.info("__get_list_of_files_to_process, report_db_connect={}".format(report_db_connect))

            try:

                _update_stmt = """
                    UPDATE connect_rt_file_process
                    SET status = '{}'
                    WHERE status = 'new'
                    AND entity_name = '{}'
                    ORDER BY date_new ASC LIMIT 1
                """.format(_generated_unique_name, sys.argv[4])

                logging.info("_update_stmt={}".format(_update_stmt))

                report_db_connect.execute(_update_stmt)

                _select_stmt = """
                    select dt, hr, filename, status, date_new, dt, id
                    from connect_rt_file_process
                    where status = '{}'
                    and entity_name = '{}'
                    """.format(_generated_unique_name, sys.argv[4])

                logging.info("_select_stmt={}".format(_select_stmt))

                report_db_connect.execute(_select_stmt)

                new_files = [row for row in report_db_connect]
                if len(new_files) > 0:
                    oldest_file = new_files[0]

                    _reset_stmt = """
                                    UPDATE connect_rt_file_process
                                    SET status = 'in_process'
                                    WHERE id = {}
                                """.format(oldest_file[6])
                    logging.info("_reset_stmt={}".format(_reset_stmt))
                    report_db_connect.execute(_reset_stmt)

                # _dt = oldest_file[0]
                # _hr = oldest_file[1]
                # _filename = oldest_file[2]
                # self._set_file_status(_dt, _hr, _filename, 'in_process', 'date_in_process')

                # update_stmt = """
                #     update connect_rt_file_process
                #     set status = 'in_process'
                #     where dt = {} AND
                #     hr = {} AND
                #     filename = {}
                # """.format(_dt, _hr, _filename)
                # report_db_connect.execute(update_stmt)

                logging.info("__get_list_of_files_to_process, new_files={}".format(new_files))

            except:
                logging.error('exception encountered when get_list_of_files_to_process...')
                logging.error(traceback.format_exc())
                # report_db_connect.rollback()

        logging.info("\nget_list_of_files_to_process invocation completed...")

        return oldest_file

    def _process_new_file(self, _unprocessed_files):

        _dt = _unprocessed_files[0]
        _hr = _unprocessed_files[1]
        _filename = _unprocessed_files[2]

        logging.info("read_local_file invoked..., unprocessed_files = {}".format(_unprocessed_files))

        _unprocessed_file = _file_list_retriever_class.construct_full_file_name(_dt, _hr, _filename)
        # _unprocessed_file = self.__construct_full_file_name(_dt, _hr, _filename)

        _tick = int(time.time())
        try:
            logging.info("In process_new_file, processing file = {}".format(_unprocessed_file))

            self._set_file_status(_dt, _hr, _filename, 'in_process', 'date_in_process',
                                  _rowcount=0, _update_rowcount=False)

            hdfs_path = '/'+'/'.join(_unprocessed_file.split('/')[3:])
            send_installs_datatech_6122.process_hdfs_file(hdfs_path)
            _tock = int(time.time())
            self._set_file_status(_dt, _hr, _filename, 'processed', 'date_processed', _tock - _tick)

        except:
            logging.error("Processing file - {}, threw exception...".format(_unprocessed_file))
            logging.error(traceback.format_exc())
            _tock = int(time.time())
            _exc_meg = str(sys.exc_info()[0])[:245]
            self._set_file_status(_dt, _hr, _filename, 'error-{}'.format(re.sub(r'\W+', ' ', _exc_meg)),
                                  'date_error',
                                  _tock - _tick)
        finally:
            _output = []

    def _process_local_file(self, _dt, _hr, _output):

        logging.info("read_local_file invoked...")

        _component_start_time = int(time.time())
        _total_number_of_batches = 0

        _app_md = AppMetadata()
        _app_md_data = _app_md.get_app_metadata_dict()

        _rows_mapped_by_tracking_id = dict()
        _jobreport = JobReport()

        logging.info("len(output) = {}".format(len(_output)))
        for _line in filter(lambda raw_line : len(raw_line) > 0,  _output.split('\n')):
            logging.debug("line={}".format(_line[:40]))
            logging.debug("len(line)={}".format(len(_line)))
            _line_as_dict = metaclass_info[sys.argv[4]][hive_line_parser](_line)
            _tracking_id = _line_as_dict["tracking_id"]

            if _app_md_data.has_key(_tracking_id):
                if not _rows_mapped_by_tracking_id.has_key(_line_as_dict["tracking_id"]):
                    _rows_mapped_by_tracking_id[_line_as_dict["tracking_id"]] = []
                _rows_mapped_by_tracking_id[_line_as_dict["tracking_id"]].append(_line_as_dict)

                if len(_rows_mapped_by_tracking_id[
                           _line_as_dict["tracking_id"]]) == config.keen_payload_record_batch_size:
                    logging.info(
                        "About to invoke rest endpoint for tracking_id = {}".format(_line_as_dict["tracking_id"]))

                    _app_metadata_row = _app_md_data[_line_as_dict["tracking_id"]]
                    _jobreport.record_data_source_metrics(_dt, _hr, _app_metadata_row[0], _app_metadata_row[1],
                                                         config.keen_payload_record_batch_size)

                    _data_sink_logger_tuple = _dt, _hr, _app_metadata_row[0], _app_metadata_row[1], \
                                             _app_metadata_row[3]

                    _rest_control = RestController(_app_metadata_row[0], _app_metadata_row[2])
                    _rest_control.invoke_keen_endpoint_for_transformed_rows(
                        _rows_mapped_by_tracking_id[_line_as_dict["tracking_id"]],
                        self.processing_start_time,
                        _component_start_time,
                        _total_number_of_batches,
                        _data_sink_logger_tuple)

                    logging.info("Completed invocation of rest endpoint for tracking_id = {}".format(
                        _line_as_dict["tracking_id"]))

                    _total_number_of_batches += 1
                    _rows_mapped_by_tracking_id[_line_as_dict["tracking_id"]] = []
            else:
                logging.debug("tracking id - {}, is not one of the apps being monitored. Ignoring this record".format(_tracking_id))

        for k, v in _rows_mapped_by_tracking_id.iteritems():
            if len(v) > 0:
                _app_metadata_row = _app_md_data[k]
                _jobreport.record_data_source_metrics(_dt, _hr, _app_metadata_row[0], _app_metadata_row[1],
                                                     len(v))

                _data_sink_logger_tuple = _dt, _hr, _app_metadata_row[0], _app_metadata_row[1], \
                                             _app_metadata_row[3]

                logging.info("About to invoke rest endpoint for tracking_id = {}".format(k))
                _rest_control = RestController(_app_metadata_row[0], _app_metadata_row[2])
                _rest_control.invoke_keen_endpoint_for_transformed_rows(v,
                                                                       self.processing_start_time,
                                                                       _component_start_time,
                                                                       _total_number_of_batches,
                                                                       _data_sink_logger_tuple)
                logging.info("Completed invocation of rest endpoint for tracking_id = {}".format(k))

    def __get_filename_without_extension(self, _filename):
        return str(_filename).replace(".gz", "")

    def _set_file_status(self, _dt, _hr, _filename, _status, _date_col_name, _processing_time=0, _rowcount=0, _update_rowcount=False):

        logging.info("__set_file_status invoked with dt = {}, hr = {}, filename = {}, status = {}, _date_col_name {}, _processing_time={}, _rowcount={}, _update_rowcount={}"
                     .format(_dt, _hr, _filename, _status, _date_col_name, _processing_time, _rowcount, _update_rowcount))

        with MySQLdb.connect(config.report_db_connect_host,
                             config.report_db_connect_user,
                             config.report_db_connect_password,
                             config.report_db_connect_db) as report_db_connect:

            logging.info("_set_file_status, report_db_connect={}".format(report_db_connect))

            try:
                if _update_rowcount:
                    _update_rowcount_sql = " , rowcount = {}".format(_rowcount)
                else:
                    _update_rowcount_sql = " "

                __update_stmt = """
                    update
                    connect_rt_file_process
                    set status = '{}',
                    {} = now(),
                    processing_time = {}
                    {}
                    where dt = {}
                    and hr = {}
                    and filename = '{}'
                    and entity_name  = '{}'
                    """.format(_status, _date_col_name, _processing_time, _update_rowcount_sql, _dt, _hr, _filename, sys.argv[4])

                logging.info("__update_stmt={}".format(__update_stmt))

                report_db_connect.execute(__update_stmt)

                logging.info("_set_file_status completed, dt={}, hr = {}, filename = {}".format(_dt, _hr, _filename))

            except:
                logging.error('exception encountered when __set_file_status...')
                logging.error(traceback.format_exc())
                # report_db_connect.rollback()

    def __init__(self):
        super(RealTimeProcess, self).__init__()
        self.processing_start_time = int(time.time())

    def tester(self, _dt, _hr):
        return self.__get_file_names_in_dthr(_dt, _hr)

if __name__ == "__main__":

    _process_dt = sys.argv[1]
    _process_hr = sys.argv[2]

    _entity_base_dir_template = metaclass_info[sys.argv[4]][entity_base_dir_template]
    _entity_filepath_template = metaclass_info[sys.argv[4]][entity_filepath_template]
    _file_list_retriever_class = metaclass_info[sys.argv[4]][file_list_retriever](_entity_base_dir_template,
                                                                                 _entity_filepath_template)

    real_time_processor = RealTimeProcess()
    real_time_processor.process(_process_dt, _process_hr)


    # _entity_base_dir_template = metaclass_info["tracker_event"][entity_base_dir_template]
    # _entity_filepath_template = metaclass_info["tracker_event"][entity_filepath_template]
    # file_list_retriever_class = metaclass_info["tracker_event"][file_list_retriever](_entity_base_dir_template,
    #                                                                              _entity_filepath_template)
    # files = file_list_retriever_class.get_file_names_in_dthr(20160910, 2016091010)

    # _entity_base_dir_template = metaclass_info["bid_overlap_categories"][entity_base_dir_template]
    # _entity_filepath_template = metaclass_info["bid_overlap_categories"][entity_filepath_template]
    # file_list_retriever_class = metaclass_info["bid_overlap_categories"][file_list_retriever](_entity_base_dir_template,
    #                                                                                  _entity_filepath_template)
    # files = file_list_retriever_class.get_file_names_in_dthr(20160910, 2016091010)

    # real_time_processor.tester(_process_dt, _process_hr)

