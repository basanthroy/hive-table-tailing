import integration.keen.common.config

__author__ = 'broy'

import json
import logging
import re
from time import gmtime, strftime

from integration.keen.common import config
from integration.keen.historic.common.transformer import Transformer


class TrackerEventTSTransformer(Transformer):

    def convert_hive_row_to_keen_format(self, hive_row):

        if hive_row["tracking_id"] in config.debug_app_id_list:
            logging.info("DEBUG APP_ID, app_id = {}, hive_row={}"
                         .format(hive_row["tracking_id"], hive_row))

        # logging.info("TrackerEventTSTransformer, hive_row = {}".format(hive_row))
        if config.loglevel == logging.DEBUG:
            logging.debug("\n\n\nhive_row = {}\n\n\n".format(json.dumps(hive_row)))

        if type(hive_row["message"]) == str:
            hive_row["message"] = json.loads(hive_row["message"])

        if config.loglevel == logging.DEBUG:
            logging.debug("hive_row = {}".format(json.dumps(hive_row)))

        formatted_hive_row = {str(key): str(value) for key, value in hive_row["message"].iteritems()
                              if type(value) != dict}

        for k, v in dict(hive_row).iteritems():
            if type(k) != dict and k != "message":
                formatted_hive_row[str(k)] = str(v)

        device_info = hive_row["message"]["device_info"]
        for k, v in dict(device_info).iteritems():
            if type(k) != dict and k != "id_info":
                formatted_hive_row[str(k)] = str(v)

        id_info = device_info["id_info"]

        for k, v in dict(id_info).iteritems():
            if (type(k) != dict):
                formatted_hive_row[str(k)] = str(v)

        if hive_row.has_key("message") and hive_row["message"].has_key("event") \
            and hive_row["message"]["event"].has_key("key_value"):
            event_kv = hive_row["message"]["event"]["key_value"]

            for kv in list(event_kv if event_kv is not None else []):
                if config.loglevel == logging.DEBUG:
                    logging.debug("key value={}".format(kv))
                if kv.has_key("key") and kv.has_key("string_value"):
                    formatted_hive_row[kv["key"]] = str(kv["string_value"])
                if kv.has_key("key") and kv.has_key("long_value"):
                    formatted_hive_row[kv["key"]] = str(kv["long_value"])
                if kv.has_key("key") and kv.has_key("double_value"):
                    formatted_hive_row[kv["key"]] = str(kv["double_value"])
                if kv.has_key("key") and kv.has_key("bool_value"):
                    formatted_hive_row[kv["key"]] = str(kv["bool_value"])

        formatted_hive_row.update({str(k): str(v) for k, v in dict(hive_row["message"]["event"]).iteritems() if
                                   (type(v) is not dict and type(v) is not list)})

        event_attr_dicts = [v for k, v in dict(hive_row["message"]["event"]).iteritems() if type(v) is dict]
        for kv_inner in list(event_attr_dicts):
            if config.loglevel == logging.DEBUG:
                logging.debug("kv_inner = {}".format(kv_inner))
            for inner_key, inner_value in kv_inner.iteritems():
                if (type(inner_value) is not dict and type(inner_value) is not list):
                    formatted_hive_row[str(inner_key)] = str(inner_value)

        if not formatted_hive_row.has_key("ip_address") or formatted_hive_row["ip_address"] == "127.0.0.1":
            if formatted_hive_row.has_key("x_forwarded_for") and not formatted_hive_row["x_forwarded_for"] == "127.0.0.1":
                formatted_hive_row["ip_address"] = formatted_hive_row["x_forwarded_for"]
        # formatted_hive_row["ip_address"] = ".".join(formatted_hive_row["ip_v4"].split(".")[:4])

        formatted_hive_row_cleaned = {k:v for k,v in formatted_hive_row.iteritems() if v is not None and v != "None"}

        if formatted_hive_row_cleaned.has_key("session_length"):
            formatted_hive_row_cleaned["session_length"] = int(formatted_hive_row_cleaned["session_length"])
        if formatted_hive_row_cleaned.has_key("subsession_length"):
            formatted_hive_row_cleaned["subsession_length"] = int(formatted_hive_row_cleaned["subsession_length"])

        if formatted_hive_row_cleaned.has_key("installed_app_info"):
            del formatted_hive_row_cleaned["installed_app_info"]

        timestamp = strftime("%Y-%m-%dT%H:%M:%S.{}Z".format(str(formatted_hive_row["timestamp"])[-3:]),
                             gmtime(int(str(formatted_hive_row["timestamp"])[:-3])))

        keen_md = json.loads(re.sub("TIMESTAMP_PLACEHOLDER", timestamp, integration.keen.common.config.keen_metadata))
        if not formatted_hive_row_cleaned.has_key("ip_address"):
            addons = keen_md["addons"]
            addons = [addon for addon in addons if addon["name"] != "keen:ip_to_geo"]
            keen_md["addons"] = addons

        formatted_hive_row_cleaned["keen"] = keen_md

        formatted_hive_row = formatted_hive_row_cleaned

        return super(TrackerEventTSTransformer, self).cleanup(formatted_hive_row)
