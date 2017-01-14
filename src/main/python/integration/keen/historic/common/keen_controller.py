__author__ = 'broy'

import time

from integration.keen.common.app_metadata import AppMetadata
from integration.keen.common.init import Init
from integration.keen.historic.common.hive_controller import HiveController

processing_start_time = int(time.time())

class KeenController(Init):

    def process(self, start_date, end_date):

        app_md = AppMetadata()
        app_md_data = app_md.get_app_metadata()

        hive_control = HiveController(processing_start_time)
        hive_rows = hive_control.process_tracker_event_server_rows(app_md_data, start_date, end_date)
