__author__ = 'broy'

import logging
import sys

from integration.keen.common.init import Init
from integration.keen.historic.common.keen_controller import KeenController

class HistoricDataTransfer(Init):

    def process(self, start_date, end_date):
        logging.info("start_date={}".format(start_date))
        logging.info("end_date={}".format(end_date))

        keen_control = KeenController()

        logging.info("Processing of historic hive records for Connect/Keen pipeline initiated...")

        keen_control.process(start_date, end_date)


if __name__ == "__main__":
    __historic_dt = HistoricDataTransfer()

    logging.info("sys argv ={}".format(sys.argv))

    if len(sys.argv) != 5:
        raise ValueError("Expected 4 arguments for start and end date. Received {}, And they are = {}".format(len(sys.argv)-1, sys.argv[1:]))

    table_name = sys.argv[4]

    __start_date = int(sys.argv[1])
    __end_date = int(sys.argv[2])

    __historic_dt.process(__start_date, __end_date)

