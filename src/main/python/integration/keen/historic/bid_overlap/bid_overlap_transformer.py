__author__ = 'broy'

import json
import logging
import re

from integration.keen.historic.common.transformer import Transformer
from integration.keen.common import config


class BidOverlapTransformer(Transformer):

    def convert_hive_row_to_keen_format(self, hive_row):

        if config.loglevel == logging.DEBUG:
            logging.debug("hive_row = {}".format(json.dumps(hive_row)))

        formatted_hive_row = {key: value for key, value in hive_row.iteritems()}

        for k,v in formatted_hive_row.iteritems():
            if v is None or v == 'None':
                v = 'NULL'
            formatted_hive_row[k] = v

        if formatted_hive_row.has_key("rank"):
            formatted_hive_row["rank"] = int(formatted_hive_row["rank"])
        if formatted_hive_row.has_key("bid_requests"):
            formatted_hive_row["bid_requests"] = int(formatted_hive_row["bid_requests"])
        if formatted_hive_row.has_key("overlap_devices"):
            formatted_hive_row["overlap_devices"] = int(formatted_hive_row["overlap_devices"])

        dt = str(hive_row["dt"])
        timestamp = "{}-{}-{}T12:00:00.000Z".format(dt[0:4], dt[4:6], dt[6:8])

        keen_md = json.loads(re.sub("TIMESTAMP_PLACEHOLDER", timestamp, config.keen_metadata))
        addons = keen_md["addons"]
        addons = [addon for addon in addons if addon["name"] != "keen:ip_to_geo"]
        keen_md["addons"] = addons

        formatted_hive_row["keen"] = keen_md

        return super(BidOverlapTransformer, self).cleanup(formatted_hive_row)