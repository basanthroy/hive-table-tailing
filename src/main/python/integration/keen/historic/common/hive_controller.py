import integration.keen.common.metaclass

__author__ = 'broy'

import logging
import re
import time
import traceback
import sys
import pyhs2

from integration.keen.common import config
from integration.keen.util import epoch
from integration.keen.historic.common.job_report import JobReport
from integration.keen.historic.common.process_chunked import ProcessChunked
from integration.keen.historic.common.rest_controller import RestController
from integration.keen.util import epoch
from integration.keen.common.metaclass import metaclass_info

class HiveController:

    processing_start_time = 0

    def process_tracker_event_server_rows(self, app_metadata, start_date, end_date):

        subprocesses = []
        __batch_start_date = start_date
        __batch_end_date = __batch_start_date + config.hive_queries_date_range - 1
        if __batch_end_date % 100 > 31:
            __batch_end_date =  __batch_end_date - __batch_end_date % 100 + 31
        if __batch_end_date > end_date:
            __batch_end_date = end_date

        while __batch_start_date <= end_date:

            for app_metadata_row in app_metadata:
                p = ProcessChunked(target=self.process_tracker_event_server_rows_for_single_project,
                        args=(app_metadata_row, __batch_start_date, __batch_end_date))
                p.app_metadata_row = app_metadata_row
                p.start_date = __batch_start_date
                p.end_date = __batch_end_date
                subprocesses.append(p)

            __batch_start_date = self.get_next_valid_start_date(__batch_start_date)
            __batch_end_date = self.get_next_valid_end_date(__batch_start_date)
            if __batch_end_date > end_date:
                __batch_end_date = end_date

        logging.info("Number of subprocesses spawned = {}".format(len(subprocesses)))
        self.__metrics.info("Number of subprocesses spawned = {}".format(len(subprocesses)))
        for idx, subprocess in enumerate(subprocesses):
            logging.info("{}, subprocesses = {}".format(idx, str(subprocess)))
            self.__metrics.info("{}, subprocesses = {}".format(idx, str(subprocess)))

        begin_index = 0
        end_index = min(config.hive_queries_throttle_batch_size, len(subprocesses) - 1)
        batch_id = 0
        while begin_index <= len(subprocesses) - 1:
            logging.info("Processing throttled batch_id={}".format(batch_id))
            self.__metrics.info("Processing throttled batch_id={}".format(batch_id))
            batch_id += 1
            batched_subprocesses = subprocesses[begin_index : end_index]

            for subprocess in batched_subprocesses:
                subprocess.start()

            for subprocess in batched_subprocesses:
                subprocess.join()

            begin_index += config.hive_queries_throttle_batch_size
            end_index += config.hive_queries_throttle_batch_size
            end_index = min(end_index, len(subprocesses) - 1)

        return True

    def get_next_valid_start_date(self, current_start_date):
        next_start_date = current_start_date + config.hive_queries_date_range
        if next_start_date %100 <= 31:
            return next_start_date
        return next_start_date - next_start_date%100 + 101

    def get_next_valid_end_date(self, current_start_date):
        next_end_date = current_start_date + config.hive_queries_date_range - 1
        if next_end_date % 100 <= 31:
            return next_end_date
        return next_end_date - next_end_date%100 + 31

    def process_tracker_event_server_rows_for_single_project(self,
                                                             app_metadata_row,
                                                             start_date,
                                                             end_date):

        component_start_time = int(time.time())

        logging.info("process_tracker_event_server_rows_for_single_project method invoked")
        logging.info("start_date={}, end_date={}, app_metadata_row={}"
                     .format(start_date, end_date, app_metadata_row))
        self.__metrics.info("start_date={}, end_date={}, app_metadata_row={}"
                     .format(start_date, end_date, app_metadata_row))

        hive_select_tracker_event_server = """
            select * from {}.{}
            where (dt between {} and {})
            and tracking_id = '{}'
            """.format(metaclass_info[sys.argv[4]][config.hive_db],
                       metaclass_info[sys.argv[4]][config.hive_table],
                       start_date,
                       end_date,
                       app_metadata_row[1])

        with pyhs2.connect(host=config.hive_server_host,
                           user=config.hive_server_user,
                           password=config.hive_server_password,
                           port=config.hive_server_port,
                           authMechanism=config.hive_server_auth) as conn:
            with conn.cursor() as cur:

                logging.info('\n In host, radiumone hive db, executing the hql statement: {}'
                             .format(hive_select_tracker_event_server))

                try:
                    cur.execute("ADD JAR hdfs://server:8020/user/broy/hive_udfs_and_jars/hive-hcatalog-core.jar")
                    cur.execute(hive_select_tracker_event_server)
                except:
                    logging.info("Hive query threw exception...")
                    logging.info("hive_select_tracker_event_server={}".format(hive_select_tracker_event_server))
                    logging.info(traceback.format_exc())
                    raise

                schema = cur.getSchema()
                schema = [re.sub(integration.keen.common.metaclass.metaclass_info[sys.argv[4]][config.hive_table] + ".",
                                 "",
                                 str(column_name_detailed['columnName'])) for
                          column_name_detailed in schema]
                logging.info("Hive query schema = {}".format(str(schema)))

                rows_as_dicts = []
                row_count = 0

                total_rows_processed = 0
                total_number_of_batches = 0

                logging.info(
                    ' Completed hive query execution. Rows not yet fetched, cumulative elapsed time = {}, Method invocation time = {} '
                        .format(
                        epoch.get_time_delta(self.processing_start_time),
                        epoch.get_time_delta(component_start_time)))

                hive_query_finish_time = int(time.time())

                rest_control = RestController(app_metadata_row[0], app_metadata_row[2])

                jobreport = JobReport()
                data_sink_logger_tuple = start_date, end_date, app_metadata_row[0], app_metadata_row[1], app_metadata_row[3]

                while cur.hasMoreRows:
                    tic = int(time.time())

                    for row in cur.fetchSet():
                        row_count += 1
                        total_rows_processed += 1
                        logging.info("""row_count = {}, total_rows_processed = {}, total_number_of_batches = {}, speed (in rows/sec) = {}""".format(row_count,
                                                                                                                                                    total_rows_processed,
                                                                                                                                                    total_number_of_batches,
                                                                                                                                                    total_rows_processed / epoch.get_time_delta(hive_query_finish_time - 1)))
                        if config.loglevel == logging.DEBUG:
                            logging.debug("row_count = {}, total_rows_processed = {}, len = {}".format(row_count,
                                                                                                       total_rows_processed,
                                                                                                       len(
                                                                                                           rows_as_dicts)))

                        rows_as_dicts.append(
                            {column_name: re.sub(r'\W+', "", str(column_value)) for column_name, column_value
                             in zip(schema, row)})

                        if row_count == config.keen_payload_record_batch_size:
                            toc = int(time.time())
                            logging.info("time to fetch {} rows from hive = {}"
                                         .format(config.keen_payload_record_batch_size,
                                                 toc - tic))
                            tic = int(time.time())

                            jobreport.record_data_source_metrics(start_date, end_date, app_metadata_row[0], app_metadata_row[1], row_count)

                            rest_control.invoke_keen_endpoint_for_transformed_rows(rows_as_dicts, self.processing_start_time,
                                                                      component_start_time, total_number_of_batches, data_sink_logger_tuple)
                            rows_as_dicts = []
                            row_count = 0
                            total_number_of_batches += 1

                if len(rows_as_dicts) > 0:
                    jobreport.record_data_source_metrics(start_date, end_date, app_metadata_row[0], app_metadata_row[1],
                                                    len(rows_as_dicts))
                    rest_control.invoke_keen_endpoint_for_transformed_rows(rows_as_dicts, self.processing_start_time,
                                                                           component_start_time,
                                                                           total_number_of_batches,
                                                                           data_sink_logger_tuple)
                    total_number_of_batches += 1

        logging.info("total_rows_processed = {}, total_number_of_batches = {}"
                     .format(total_rows_processed, total_number_of_batches))



    def __init__(self, processing_start_time_arg):
        self.processing_start_time = processing_start_time_arg
        self.__metrics = logging.getLogger("METRICS")
