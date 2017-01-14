__author__ = 'broy'

import logging
import collections
import json

from integration.keen.common import config

class Transformer(object):

    def convert_hive_row_to_keen_format(self, hive_row):
        return hive_row

    def cleanup(self, formatted_hive_row):
        formatted_hive_row = {k.replace('.', '_'): v for k, v in formatted_hive_row.iteritems()}

        formatted_hive_row = {k: v for k, v in formatted_hive_row.iteritems() if k != ''}

        od = collections.OrderedDict(sorted(formatted_hive_row.iteritems(), key=lambda x: x[0].lower()))

        if config.loglevel == logging.DEBUG:
            logging.debug("\n\n\nOrdered Dict = {}\n\n\n".format(json.dumps(od)))

        return od
