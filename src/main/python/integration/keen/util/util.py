import integration.keen.common.metaclass

__author__ = 'broy'

import logging
import traceback
import sys
import MySQLdb

from integration.keen.common import config
from integration.keen.common.metaclass import metaclass_info

class Util:

    def get_collection_name(self, data_sink_logger_tuple):
        coll = metaclass_info[sys.argv[4]][config.keen_collection]
        keen_coll_name_fn = metaclass_info[sys.argv[4]][config.keen_collection_name_function]
        return keen_coll_name_fn(coll, data_sink_logger_tuple)

    def is_process_name_unique(self, process_name):
        logging.info("\nget_process_name_unique invoked...")

        report_db_connect = MySQLdb.connect(config.report_db_connect_host,
                                         config.report_db_connect_user,
                                         config.report_db_connect_password,
                                         config.report_db_connect_db)

        report_db_connect.autocommit(False)
        cursor = report_db_connect.cursor()

        try:
            select_process_name = """
                        select process_name from data_metrics_source where process_name = '{}' limit 1
                        """.format(process_name)
            cursor.execute(select_process_name)

            for row in cursor:
                print "row={}".format(row)
                return False
            # if cursor.hasMoreRows():
            #     return False

            return True

        except:
            logging.error('exception encountered when selecting get_process_name_unique metadata...')
            logging.error(traceback.format_exc())
        finally:
            logging.info("\nget_process_name_unique invocation completed...")
