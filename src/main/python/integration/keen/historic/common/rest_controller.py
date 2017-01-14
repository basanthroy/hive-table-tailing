import integration.keen.common.metaclass

__author__ = 'broy'

import json
import logging
import time
import traceback
import sys
import requests

from integration.keen.common import config
from integration.keen.historic.common.job_report import JobReport
from integration.keen.util import epoch
from integration.keen.util.util import Util

# from integration.keen.historic.bid_overlap.bid_overlap_transformer import BidOverlapTransformer
# from integration.keen.historic.bid_overlap.bid_overlap_transformer import BidOverlapTransformer


class RestController:

    def invoke_keen_endpoint_for_transformed_rows(self, rows_as_dicts, processing_start_time, component_start_time,
                                                  total_number_of_batches, data_sink_logger_tuple):

        tic = int(time.time())

        transformed_rows = self.convert_hive_rows_to_keen_payload(rows_as_dicts)

        toc = int(time.time())
        logging.info("time to transform 1000 rows in json = {}".format(toc - tic))

        __t = self.invoke_keen_endpoint(transformed_rows, data_sink_logger_tuple)
        logging.info(
            ' Completed one more rest api call, cumulative elapsed time = {}, Method invocation time = {}, number of batches so far = {} '
                .format(
                epoch.get_time_delta(processing_start_time),
                epoch.get_time_delta(component_start_time),
                total_number_of_batches))
        return __t

    def convert_hive_rows_to_keen_payload(self, hive_rows):

        xformer = integration.keen.common.metaclass.metaclass_info[sys.argv[4]][config.klass]

        if config.loglevel == logging.DEBUG:
            rows_for_logging = [json.dumps(row)[:100] for row in hive_rows]
            logging.debug("Invoked convert_hive_row_to_keen_payload ..., hive_rows={}".format(rows_for_logging))

        transformed_rows = []

        for hive_row in hive_rows:
            if config.loglevel == logging.DEBUG:
                logging.debug("converting hive row , {}".format(json.dumps(hive_row)[:100]))
            try:
                transformed_rows.append(xformer.convert_hive_row_to_keen_format(hive_row))
            except:
                logging.error("hive_row threw exceptionNNN")
                logging.error("exception={}".format(traceback.format_exc()))
                logging.error("hive_row={}".format(hive_row))
                raise

        if config.loglevel == logging.DEBUG:
            logging.debug("In convert_hive_row_to_keen_payload, transformed_rows={}".format(
                rows_for_logging=[str(row)[:300] for row in transformed_rows]))

        return transformed_rows

    def send_request_in_new_thread(self, url, json_payload, headers, data_sink_logger_tuple):

        tic = int(time.time())

        logging.info("keen endpoint invocation about to be initiated")

        jobreport = JobReport()
        # util = Util()
        # content = {util.get_collection_name(data_sink_logger_tuple) : []}
        try:

            # jobreport.record_data_sink_metrics(self.__start_date, end_date, self.__keen_project_id, app_id, rowcount)
            # jobreport.record_data_sink_metrics(0, 0, self.__keen_project_id, 0, 0)

            if data_sink_logger_tuple[3] in config.debug_app_id_list:
                logging.info("DEBUG APP_ID, app_id = {}, url = {}, headers={}, json_payload={}"
                             .format(data_sink_logger_tuple[3], url, headers, json_payload))

            r = requests.post(url,
                              json_payload,
                              headers=headers)
            __response_content = r.content
            # content = __response_content
            jobreport.record_data_sink_metrics(data_sink_logger_tuple, json.loads(__response_content))
        except:
            print "EXCEPTION encountered"
            print traceback.format_exc()
            logging.info("EXCEPTION encountered")
            logging.info(traceback.format_exc())
            # raise

        # print "KEEN ENDPOINT INVOKED"
        # logging.info("KEEN ENDPOINT INVOKED")

        if 'r' in locals():
            logging.info("keen endpoint invocation completed. Response status = {}".format(str(r.status_code)))
            # logging.debug("keen endpoint invocation completed. Response body = {}".format(str(r.content[:100])))
        else:
            logging.info("keen endpoint invocation completed. Response status undefined")

        # TODO : If there are any error responses, then need to do something for thows rows
        # TODO : This is post MVP

        toc = int(time.time())
        logging.info("rest api call time = {}".format(toc - tic))

    def invoke_keen_endpoint(self, rows, data_sink_logger_tuple):

        try:
            logging.info("invoke_keen_endpoint invocation initiated ...\n")

            if config.loglevel == logging.DEBUG:
                rows_for_logging = [str(row)[:200] for row in rows]
                logging.debug("In invoke_keen_endpoint, rows={}".format(rows_for_logging))

            row_with_envelope = dict()
            util = Util()
            row_with_envelope[util.get_collection_name(data_sink_logger_tuple)] = rows

            json_payload = json.dumps(row_with_envelope)

            if config.loglevel == logging.DEBUG:
                logging.debug("json_payload length = {}, json_payload={}".format(len(json_payload), json_payload[:300]))
            logging.info("url={}, headers={}".format
                         (self.__url,
                          self.__headers))

            self.send_request_in_new_thread(self.__url, json_payload, self.__headers, data_sink_logger_tuple)
            logging.info("invoke_keen_endpoint invocation completed ...\n")
            # __t = Thread(target=self.send_request_in_new_thread, args=(self.__url, json_payload, self.__headers))
            # __t.start()
            #
            # return __t
            # __t.join()
        except:
            print "EXCEPTION encountered"
            print traceback.format_exc()
            logging.info("EXCEPTION encountered")
            logging.info(traceback.format_exc())

    def __init__(self, keen_project_id, keen_custom_key):
        self.__keen_project_id = keen_project_id
        self.__keen_custom_key = keen_custom_key
        self.__url = config.keen_api_template.format(keen_project_id)
        # self.__headers = json.loads(re.sub("KEEN_CUSTOM_KEY", keen_custom_key, config.keen_api_event_header_template))
        hdr = config.keen_api_event_header_template
        hdr["Authorization"] = keen_custom_key
        self.__headers = hdr


