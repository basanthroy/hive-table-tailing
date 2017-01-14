__author__ = 'broy'

import json
import logging
import re
import sys

from integration.keen.historic.common.transformer import Transformer
from integration.keen.common import config


class KochavaPerformanceTransformer(Transformer):

    def convert_hive_row_to_keen_format(self, hive_row):

        if config.loglevel == logging.DEBUG:
            logging.debug("hive_row = {}".format(json.dumps(hive_row)))

        formatted_hive_row = {key: value for key, value in hive_row.iteritems()}

        for k,v in formatted_hive_row.iteritems():
            if v is None or v == 'None':
                v = 'NULL'
            formatted_hive_row[k] = v

        int_to_string = ["install_matched_to_imp_count",
                         "install_matched_to_click_count",
                         "installs",
                         "click_matched_to_imp_count",
                         "click_duplicates",
                         "impressions",
                         "impressions",
                         "click_spent",
                         "clicks",
                         "rank",
                         "bid_requests",
                         "overlap_devices"]
        for field in int_to_string:
            if formatted_hive_row.has_key(field):
                if formatted_hive_row[field] == "NULL":
                    formatted_hive_row[field] = 0
                formatted_hive_row[field] = int(float(formatted_hive_row[field]))

        hive_row["dt"] = sys.argv[1]
        dt = str(hive_row["dt"])
        timestamp = "{}-{}-{}T12:00:00.000Z".format(dt[0:4], dt[4:6], dt[6:8])

        keen_md = json.loads(re.sub("TIMESTAMP_PLACEHOLDER", timestamp, config.keen_metadata))
        addons = keen_md["addons"]
        addons = [addon for addon in addons if addon["name"] != "keen:ip_to_geo"]
        keen_md["addons"] = addons

        formatted_hive_row["keen"] = keen_md

        return super(KochavaPerformanceTransformer, self).cleanup(formatted_hive_row)