__author__ = 'broy'

import json
import logging

from integration.keen.common.config import klass, hive_db, hive_table, keen_collection, keen_collection_name_function, \
                                           entity_base_dir_template, entity_filepath_template, file_list_retriever, \
                                           hive_line_parser
from integration.keen.historic.bid_overlap.bid_overlap_transformer import BidOverlapTransformer
from integration.keen.historic.tracker_event_server_ts.tracker_event_ts_transformer import TrackerEventTSTransformer
from integration.keen.historic.kochava_performance.kochava_performance_transformer import KochavaPerformanceTransformer
from integration.keen.historic.tracker_event_server_ts.file_list_retriever_tracker_ev_json import FileListRetrieverTrackerEventJson
from integration.keen.historic.bid_overlap.file_list_retriever_bid_overlap import FileListRetrieverBidOverlap

_bid_overlap_schema = ["tracking_id", "ad_exchange", "label", "rank", "overlap_devices", "bid_requests", "dt"]
_kochava_perf_schema = ["tracking_id", "kochava_app_id", "campaign_id", "campaign_name", "clicks", "click_duplicates", "installs", \
                        "impressions", "click_matched_to_imp_count", "install_matched_to_click_count", \
                        "install_matched_to_imp_count", "click_spent", "impression_spent", "network", \
                        "period", "period_timestamp", "date", "dt", "hr"]

def _bid_overlap_line_parser(line):
    return _generic_line_parser(line, _bid_overlap_schema)

def _kochava_perf_line_parser(line):
    return _generic_line_parser(line, _kochava_perf_schema)

def _generic_line_parser(line, _schema):
    _parts = line.split("\x01")
    _line_as_dict = dict()
    for _col_name, _col_value in zip(_schema, _parts):
        _line_as_dict[_col_name] = _col_value
    logging.debug("_generic_line_parser, line = {}, line_as_dict={}".format(line, _line_as_dict))
    return _line_as_dict

metaclass_info = {
                  "tracker_event"           : {klass : TrackerEventTSTransformer(),
                                               hive_db : "radiumone",
                                               hive_table : "tracker_event_server_ts_json",
                                               hive_line_parser: (lambda line : json.loads(line)),
                                               keen_collection : "unenriched_data_collection",
                                               keen_collection_name_function : (lambda coll, data_sink_logger_tuple :
                                                        coll + "_" + str(data_sink_logger_tuple[4])
                                                        # "zz_basanth_test_" + str(data_sink_logger_tuple[4])
                                                                                ),
                                               entity_base_dir_template : "hdfs://server:8020/data/radiumone/tracker-event-server-ts-json/dt={}/hr={}/",
                                               entity_filepath_template : "hdfs://server:8020/data/radiumone/tracker-event-server-ts-json/dt={}/hr={}/{}",
                                               file_list_retriever : FileListRetrieverTrackerEventJson
                                               },
                  "kochava_tracker_event"    : {klass : TrackerEventTSTransformer(),
                                               hive_db : "radiumone",
                                               hive_table : "tracker_event_server_ts_json",
                                               hive_line_parser: (lambda line : json.loads(line)),
                                               entity_base_dir_template : "hdfs://server:8020/data/radiumone/tracker-event-server-ts-json/dt={}/hr={}/",
                                               entity_filepath_template : "hdfs://server:8020/data/radiumone/tracker-event-server-ts-json/dt={}/hr={}/{}",
                                               file_list_retriever : FileListRetrieverTrackerEventJson
                                               },
                  "tracker_event_backfill"  : {klass : TrackerEventTSTransformer(),
                                               hive_db : "radiumone",
                                               hive_table : "tracker_event",
                                               hive_line_parser: (lambda line : json.loads(line)),
                                               keen_collection : "unenriched_data_collection",
                                               keen_collection_name_function : (lambda coll, data_sink_logger_tuple :
                                                        coll + "_" + str(data_sink_logger_tuple[4])
                                                        #"zz_basanth_test_" + str(data_sink_logger_tuple[4])
                                                                                ),
                                               entity_base_dir_template : "hdfs://server:8020/data/radiumone/tracker-event/dt={}/hr={}/",
                                               entity_filepath_template : "hdfs://server:8020/data/radiumone/tracker-event/dt={}/hr={}/{}",
                                               file_list_retriever : FileListRetrieverTrackerEventJson
                                               },
                  "tracker_event_kochava"   : {klass: TrackerEventTSTransformer(),
                                               hive_db: "radiumone",
                                               hive_table: "tracker_event_kochava_json",
                                               hive_line_parser: (lambda line: json.loads(line)),
                                               keen_collection: "enriched_data_collection_test",
                                               keen_collection_name_function: (lambda coll, data_sink_logger_tuple:
                                                                    coll + "_" + str(data_sink_logger_tuple[4]))
                                               },
                  "bid_overlap_cities"      : {klass : BidOverlapTransformer(),
                                               hive_db : "radiumone",
                                               hive_table : "connect_overlap_cities",
                                               hive_line_parser: (lambda line: _bid_overlap_line_parser(line)),
                                               keen_collection: "connect_overlap_cities3",
                                               keen_collection_name_function : (lambda coll, data_sink_logger_tuple :
                                                        coll),
                                               entity_base_dir_template : "hdfs://server:8020/data/radiumone/connect/overlap/top_cities/dt={}/",
                                               entity_filepath_template : "hdfs://server:8020/data/radiumone/connect/overlap/top_cities/dt={}/{}",
                                               file_list_retriever : FileListRetrieverBidOverlap
                                               },
                  "bid_overlap_categories"  : {klass: BidOverlapTransformer(),
                                               hive_db : "radiumone",
                                               hive_table: "connect_overlap_categories",
                                               hive_line_parser: (lambda line: _bid_overlap_line_parser(line)),
                                               keen_collection: "connect_overlap_categories3",
                                               keen_collection_name_function: (lambda coll, data_sink_logger_tuple:
                                                       coll),
                                               entity_base_dir_template : "hdfs://server:8020/data/radiumone/connect/overlap/top_content_categories/dt={}/",
                                               entity_filepath_template : "hdfs://server:8020/data/radiumone/connect/overlap/top_content_categories/dt={}/{}",
                                               file_list_retriever : FileListRetrieverBidOverlap
                                               },
                  "bid_overlap_domains"     : {klass: BidOverlapTransformer(),
                                               hive_db : "radiumone",
                                               hive_table: "connect_overlap_domains",
                                               hive_line_parser: (lambda line: _bid_overlap_line_parser(line)),
                                               keen_collection: "connect_overlap_domains3",
                                               keen_collection_name_function: (lambda coll, data_sink_logger_tuple:
                                                       coll),
                                               entity_base_dir_template : "hdfs://server:8020/data/radiumone/connect/overlap/top_domains/dt={}/",
                                               entity_filepath_template : "hdfs://server:8020/data/radiumone/connect/overlap/top_domains/dt={}/{}",
                                               file_list_retriever : FileListRetrieverBidOverlap
                                               },
                  "bid_overlap_media_types" : {klass: BidOverlapTransformer(),
                                               hive_db : "radiumone",
                                               hive_table: "connect_overlap_media_types",
                                               hive_line_parser: (lambda line: _bid_overlap_line_parser(line)),
                                               keen_collection: "connect_overlap_media_types3",
                                               keen_collection_name_function: (lambda coll, data_sink_logger_tuple:
                                                       coll),
                                               entity_base_dir_template : "hdfs://server:8020/data/radiumone/connect/overlap/top_media_types/dt={}/",
                                               entity_filepath_template : "hdfs://server:8020/data/radiumone/connect/overlap/top_media_types/dt={}/{}",
                                               file_list_retriever : FileListRetrieverBidOverlap
                                               },
                  "kochava_performance"     : {klass: KochavaPerformanceTransformer(),
                                               hive_db : "kochava",
                                               hive_table: "campaign2",
                                               hive_line_parser: (lambda line: json.loads(line)),
                                               keen_collection: "kochava_performance4",
                                               keen_collection_name_function: (lambda coll, data_sink_logger_tuple:
                                                       coll),
                                               entity_base_dir_template : "hdfs://server:8020/data/partner/kochava/app_campaign2/dt={}/hr={}/",
                                               entity_filepath_template : "hdfs://server:8020/data/partner/kochava/app_campaign2/dt={}/hr={}/{}",
                                               file_list_retriever : FileListRetrieverTrackerEventJson
                                               }
                  }

