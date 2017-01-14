__author__ = 'broy'

import logging
import sys
import traceback

import json
import MySQLdb

from integration.keen.common import config
from integration.keen.util.util import Util


class JobReport:

    def record_data_source_metrics(self, start_date, end_date, keen_project_id, app_id, rowcount):

        report_db_connect = MySQLdb.connect(config.report_db_connect_host,
                                            config.report_db_connect_user,
                                            config.report_db_connect_password,
                                            config.report_db_connect_db)

        report_db_connect.autocommit(False)
        cursor = report_db_connect.cursor()

        try:
            select_app_metadata = """
                insert into data_metrics_source(start_date, end_date, keen_project_id, app_id, rowcount, process_name, entity_name)
                values ({}, {}, '{}', '{}', {}, '{}', '{}')
                """.format(start_date, end_date, keen_project_id, app_id, rowcount, sys.argv[3], sys.argv[4])
            cursor.execute(select_app_metadata)

            logging.info("start_date = {}, end_date = {}, keen_project_id = {}, app_id = {}, rowcount = {}, process_name = {}, entity_name = {}"
                         .format(start_date, end_date, keen_project_id, app_id, rowcount, sys.argv[3], sys.argv[4]))

        except:
            logging.error('exception encountered when record_data_source_metrics...')
            logging.error(traceback.format_exc())
            report_db_connect.rollback

        report_db_connect.commit()

    def __record_data_sink_metrics(self, start_date, end_date, keen_project_id, app_id, rowcount, status):

        report_db_connect = MySQLdb.connect(config.report_db_connect_host,
                                            config.report_db_connect_user,
                                            config.report_db_connect_password,
                                            config.report_db_connect_db)

        report_db_connect.autocommit(False)
        cursor = report_db_connect.cursor()

        try:
            select_app_metadata = """
                    insert into data_metrics_sink(start_date, end_date, keen_project_id, app_id, rowcount, status, process_name, entity_name)
                    values ({}, {}, '{}', '{}', {}, '{}', '{}', '{}')
                    """.format(start_date, end_date, keen_project_id, app_id, rowcount, status, sys.argv[3], sys.argv[4])
            cursor.execute(select_app_metadata)

            logging.info("start_date = {}, end_date = {}, keen_project_id = {}, app_id = {}, rowcount = {}, status = {}, process_name = {}, entity_name = {}"
                         .format(start_date, end_date, keen_project_id, app_id, rowcount, status, sys.argv[3], sys.argv[4]))

        except:
            logging.error('exception encountered when record_data_sink_metrics...')
            logging.error(traceback.format_exc())
            report_db_connect.rollback

        report_db_connect.commit()

    def record_data_sink_metrics(self, data_sink_logger_tuple, response_dict):

        row_count_unknown, status = -1, "unknown"
        row_count_success, row_count_failure = 0, 0
        start_date, end_date, keen_project_id, app_id, row_id = data_sink_logger_tuple

        logging.info("response_dict[:100] = {}".format(json.dumps(response_dict)[:100]))

        if data_sink_logger_tuple[3] in config.debug_app_id_list:
            logging.info("DEBUG APP_ID, app_id = {}, response_dict = {}".format(data_sink_logger_tuple[3], json.dumps(response_dict)))

        util = Util()

        if dict(response_dict).has_key(util.get_collection_name(data_sink_logger_tuple)):

            response_rows = response_dict[util.get_collection_name(data_sink_logger_tuple)]
            response_success_rows = [response_row for response_row in response_rows
                                     if (response_row["success"] == "true" or
                                         response_row["success"] == "True" or
                                         response_row["success"] == True)]
            row_count_success = len(response_success_rows)

            logging.info("Total number of response records = {}, num success = {}"
                         .format(len(response_rows),
                                 len(response_success_rows)))

            if row_count_success > 0:
                self.__record_data_sink_metrics(start_date, end_date, keen_project_id, app_id, row_count_success, "success")

            response_failure_rows = [response_row for response_row in response_rows
                                     if (response_row["success"] == "false" or
                                         response_row["success"] == "False" or
                                         response_row["success"] == False)]
            row_count_failure = len(response_failure_rows)

            if row_count_failure > 0:
                self.__record_data_sink_metrics(start_date, end_date, keen_project_id, app_id, row_count_failure, "failure")

            return

        self.__record_data_sink_metrics(start_date, end_date, keen_project_id, app_id, 1, "unknown")

        logging.info("record_data_sink_metrics, row_count_success={}, row_count_failure = {}, row_count_unknown = {}"
                     .format(row_count_success, row_count_failure, row_count_unknown))



