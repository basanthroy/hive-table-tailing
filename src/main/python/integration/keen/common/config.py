__author__ = 'broy'

import logging

keen_api_event_header_template={"Authorization": "KEEN_CUSTOM_KEY", "Content-Type" : "application/json"}
keen_api_template = """https://api.keen.io/3.0/projects/{}/events"""

# personal mac
BASE_DIR='/Users/broy/keen'
# jobserver2/connu
# BASE_DIR='/opt/dwradiumone/r1-dw-connect-app/dev/tracker_event_stj'
ROOT_DIR=BASE_DIR + '/log/rt'
SCRIPT_DIR=BASE_DIR + '/scripts/src/main/python/integration/keen'

report_db_connect_host = '.com'
report_db_connect_user = ''
report_db_connect_password = ''
report_db_connect_db = ''

app_db_connect_host = '.com'
app_db_connect_user = ''
app_db_connect_password = ''
app_db_connect_db = ''

hive_server_host = ''
hive_server_user = ''
hive_server_password = ''
hive_server_port = 10000
hive_server_auth = 'PLAIN'

loglevel = logging.INFO

keen_payload_record_batch_size=2000
hive_queries_throttle_batch_size=20
hive_queries_date_range=31

keen_metadata = """
    {"timestamp": "TIMESTAMP_PLACEHOLDER",
     "addons" : [
         {"name" : "keen:ip_to_geo",
          "input" : {
              "ip" : "ip_address"
          },
          "output" : "ip_geo_info"
         }
     ]
    }
"""

klass = "class"
hive_db = "hive_db"
hive_table = "hive_table"
hive_line_parser="hive_line_parser"
keen_collection = "keen_collection"
keen_collection_name_function = "lamda"
file_list_retriever="file_list_retriever"
entity_base_dir_template="entity_base_dir_template"
entity_filepath_template="entity_filepath_template"

max_concurrent_python_processes_on_server=8

debug_app_id_list = []
