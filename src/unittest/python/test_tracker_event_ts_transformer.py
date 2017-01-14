import logging
import sys
import unittest
import os
import collections
import json
import re

# fix this for final version
currentUser = os.path.expanduser('~')

sys.path.append(currentUser + '/r1-dw-mobile-lowlatency-app/')

from integration.keen.historic.tracker_event_server_ts import tracker_event_ts_transformer
from integration.keen.historic.tracker_event_server_ts.tracker_event_ts_transformer import TrackerEventTSTransformer
from integration.keen.common import config

class TestTrackerEventTSTransformer(unittest.TestCase):

	partitionData = []
	resourceDir = currentUser + '/workspace/r1-dw-mobile-lowlatency-app_feature_build/src/main/python/integration/resources/'

	with open(resourceDir+'radium1_tracker-event-server-ts-json_222259999_2016101000_0.json') as f:
			for line in f:
				line = re.sub(r'false',r'"false"',line)
				line = re.sub(r'}}, "ip_address":',r'}, "ip_address":',line)
				partitionData.append("{"+line[line.find('"carrier_country"'):len(line)])

	def setUp(self):
		print "in setUp"

	def tearDown(self):
		print "in tearDown"

	def test_transformEmptyDictionary(self):
		result = False
		testTarget = TrackerEventTSTransformer()
		hiveRow = dict()
		try:
			od = testTarget.convert_hive_row_to_keen_format(hiveRow)
		except KeyError:
			result = True
		self.assertEqual(True,result)

	def test_transformEmptyDeviceInfo(self):
		result = False
		testTarget = TrackerEventTSTransformer()
		hiveRow = dict()
		hiveRow = {'message': {'event':{'foo':'bar'}},'timestamp':'1475881135'}

		try:
			od = testTarget.convert_hive_row_to_keen_format(hiveRow)
		except KeyError,ValueError:
			result = True
			self.assertEqual(True,result)

	def test_transformEmptyIDInfoInDeviceInfo(self):
		result = False
		testTarget = TrackerEventTSTransformer()
		hiveRow = dict()
		hiveRow = {'message': {'device_info':{},'event':{'foo':'bar'}},'timestamp':'1475881135'}
		try:
			od = testTarget.convert_hive_row_to_keen_format(hiveRow)
		except KeyError:
			result = True
		self.assertEqual(True,result)

	def test_transformEmptyEvent(self):
		result = False
		testTarget = TrackerEventTSTransformer()
		hiveRow = dict()
		hiveRow = {'message': {'device_info':{'id_info':{'foo':'bar'}},'event':{}},'timestamp':'1475881135'}
		try:
			od = testTarget.convert_hive_row_to_keen_format(hiveRow)
		except KeyError:
			result = True
		self.assertEqual(True,result)

	def test_transformNoIpAddress(self):
		testTarget = TrackerEventTSTransformer()
		hiveRow = dict()
		hiveRow = {"message": {"carrier_country": "ca", "screen_resolution": "640x1136", "application_id": "com.biggu.shopsavvyapp", "viewport_size": "320x568", "device_info": {"id_info": {"idfv": "ADEBBBE7-CF5C-4DD8-9238-25506D877543", "opt_out": "false", "r1_daid": "B817AF40-FB61-460C-B997-55F2594B928A", "idfa": "16F30910-D606-400D-8BE7-AC2C553A66BC", "dpid_md5": "b85caad2443c0d85999c199afd1038ad", "dpid": "16F30910-D606-400D-8BE7-AC2C553A66BC", "dpid_sha1": "2f0178dd6e7ff6726aa00bb3d5ae8ac5eb960c99"}, "country": "CA", "make": "Apple", "os_language": "en-CA", "os_version": "9.3.2", "ip_v4": "92.117.48.48.48.97.92.117.48.48.57.100.92.117.48.48.98.100.92.117.48.48.99.53", "model": "iPhone5,3", "os": "iPhone OS", "timezone_offset": -10800}, "conn_type": "CARRIER", "source": "ADVERTISER_SDK", "sdk_version": "3.3.0", "version": "2", "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13F69", "device_type": "HANDHELD", "user_language": "en", "bundle_name": "com.biggu.shopsavvyapp", "carrier_name": "Koodo", "application_version": "6", "event": {"event_name": "Launch", "timestamp": 1476057596865, "key_value": [{"long_value": 1475804891940, "key": "R1_Firstlaunchts"}], "transaction_id": "92.117.48.48.49.99.92.117.48.48.102.53.92.117.48.48.51.51.92.117.48.48.52.101.92.117.48.48.99.48.92.117.48.48.52.49.92.117.48.48.52.101.92.117.48.48.56.99.92.117.48.48.56.48.92.117.48.48.51.101.92.117.48.48.57.99.92.117.48.48.57.57.92.117.48.48.51.101.92.117.48.48.50.48.92.117.48.48.57.97.92.117.48.48.49.54", "session_id": "8A38D058-EEFC-43BB-BC55-C94A8D92C044"}}, "x_forwarded_for": "142.169.78.197", "fileid": 222259999}
		od = testTarget.convert_hive_row_to_keen_format(hiveRow)
		result='142.169.78.197'
		self.assertEqual(od['ip_address'],result)

	def test_transformIpAddressLocalhost(self):
		testTarget = TrackerEventTSTransformer()
		hiveRow = dict()
		hiveRow = {"message": {"carrier_country": "ca", "screen_resolution": "640x1136", "application_id": "com.biggu.shopsavvyapp", "viewport_size": "320x568", "device_info": {"id_info": {"idfv": "ADEBBBE7-CF5C-4DD8-9238-25506D877543", "opt_out": "false", "r1_daid": "B817AF40-FB61-460C-B997-55F2594B928A", "idfa": "16F30910-D606-400D-8BE7-AC2C553A66BC", "dpid_md5": "b85caad2443c0d85999c199afd1038ad", "dpid": "16F30910-D606-400D-8BE7-AC2C553A66BC", "dpid_sha1": "2f0178dd6e7ff6726aa00bb3d5ae8ac5eb960c99"}, "country": "CA", "make": "Apple", "os_language": "en-CA", "os_version": "9.3.2", "ip_v4": "92.117.48.48.48.97.92.117.48.48.57.100.92.117.48.48.98.100.92.117.48.48.99.53", "model": "iPhone5,3", "os": "iPhone OS", "timezone_offset": -10800}, "conn_type": "CARRIER", "source": "ADVERTISER_SDK", "sdk_version": "3.3.0", "version": "2", "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13F69", "device_type": "HANDHELD", "user_language": "en", "bundle_name": "com.biggu.shopsavvyapp", "carrier_name": "Koodo", "application_version": "6", "event": {"event_name": "Launch", "timestamp": 1476057596865, "key_value": [{"long_value": 1475804891940, "key": "R1_Firstlaunchts"}], "transaction_id": "92.117.48.48.49.99.92.117.48.48.102.53.92.117.48.48.51.51.92.117.48.48.52.101.92.117.48.48.99.48.92.117.48.48.52.49.92.117.48.48.52.101.92.117.48.48.56.99.92.117.48.48.56.48.92.117.48.48.51.101.92.117.48.48.57.99.92.117.48.48.57.57.92.117.48.48.51.101.92.117.48.48.50.48.92.117.48.48.57.97.92.117.48.48.49.54", "session_id": "8A38D058-EEFC-43BB-BC55-C94A8D92C044"}}, "ip_address": "127.0.0.1", "x_forwarded_for": "142.169.78.197", "fileid": 222259999}
		od = testTarget.convert_hive_row_to_keen_format(hiveRow)
		result='142.169.78.197'
		self.assertEqual(od['ip_address'],result)

	def test_transformSessionLength(self):
		testTarget = TrackerEventTSTransformer()
		hiveRow = dict()
		hiveRow = {'message': self.partitionData[4]}
		od = testTarget.convert_hive_row_to_keen_format(hiveRow)
		self.assertEqual(True,type(od['session_length']) == int)

	def test_transformSubSessionLength(self):
		result = False
		testTarget = TrackerEventTSTransformer()
		hiveRow = dict()
		hiveRow = {'message': self.partitionData[23]}
		od = testTarget.convert_hive_row_to_keen_format(hiveRow)
		self.assertEqual(True,type(od['subsession_length']) == int)

	def test_transformNoCleanedIpAddress(self):
		result = False
		testTarget = TrackerEventTSTransformer()
		hiveRow = dict()
		hiveRow = {"message": {"carrier_country": "ca", "screen_resolution": "640x1136", "application_id": "com.biggu.shopsavvyapp", "viewport_size": "320x568", "device_info": {"id_info": {"idfv": "ADEBBBE7-CF5C-4DD8-9238-25506D877543", "opt_out": "false", "r1_daid": "B817AF40-FB61-460C-B997-55F2594B928A", "idfa": "16F30910-D606-400D-8BE7-AC2C553A66BC", "dpid_md5": "b85caad2443c0d85999c199afd1038ad", "dpid": "16F30910-D606-400D-8BE7-AC2C553A66BC", "dpid_sha1": "2f0178dd6e7ff6726aa00bb3d5ae8ac5eb960c99"}, "country": "CA", "make": "Apple", "os_language": "en-CA", "os_version": "9.3.2", "ip_v4": "92.117.48.48.48.97.92.117.48.48.57.100.92.117.48.48.98.100.92.117.48.48.99.53", "model": "iPhone5,3", "os": "iPhone OS", "timezone_offset": -10800}, "conn_type": "CARRIER", "source": "ADVERTISER_SDK", "sdk_version": "3.3.0", "version": "2", "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13F69", "device_type": "HANDHELD", "user_language": "en", "bundle_name": "com.biggu.shopsavvyapp", "carrier_name": "Koodo", "application_version": "6", "event": {"event_name": "Launch", "timestamp": 1476057596865, "key_value": [{"long_value": 1475804891940, "key": "R1_Firstlaunchts"}], "transaction_id": "92.117.48.48.49.99.92.117.48.48.102.53.92.117.48.48.51.51.92.117.48.48.52.101.92.117.48.48.99.48.92.117.48.48.52.49.92.117.48.48.52.101.92.117.48.48.56.99.92.117.48.48.56.48.92.117.48.48.51.101.92.117.48.48.57.99.92.117.48.48.57.57.92.117.48.48.51.101.92.117.48.48.50.48.92.117.48.48.57.97.92.117.48.48.49.54", "session_id": "8A38D058-EEFC-43BB-BC55-C94A8D92C044"}}, "fileid": 222259999}
		od = testTarget.convert_hive_row_to_keen_format(hiveRow)
		result = od.has_key('keen')
		self.assertEqual(True,result)

	def test_transformWrongStructure(self):
		result = False
		testTarget = TrackerEventTSTransformer()
		hiveRow = dict()
		hiveRow = {"server_dt": 20161010, "uuid": "200.50.241.247.125.170.77.228.166.86.30.203.6.95.34.251", "timestamp": 1476057600322, "device_hr": 2016100923, "tracking_id": "0305A750-0DB1-4B1A-979D-9D860353A899", "server_hr": 2016101000, "device_dt": 20161009, "message": {"carrier_country": "ca", "screen_resolution": "640x1136", "application_id": "com.biggu.shopsavvyapp", "viewport_size": "320x568", "device_info": {"id_info": {"idfv": "ADEBBBE7-CF5C-4DD8-9238-25506D877543", "opt_out": "false", "r1_daid": "B817AF40-FB61-460C-B997-55F2594B928A", "idfa": "16F30910-D606-400D-8BE7-AC2C553A66BC", "dpid_md5": "b85caad2443c0d85999c199afd1038ad", "dpid": "16F30910-D606-400D-8BE7-AC2C553A66BC", "dpid_sha1": "2f0178dd6e7ff6726aa00bb3d5ae8ac5eb960c99"}, "country": "CA", "make": "Apple", "os_language": "en-CA", "os_version": "9.3.2", "ip_v4": "92.117.48.48.48.97.92.117.48.48.57.100.92.117.48.48.98.100.92.117.48.48.99.53", "model": "iPhone5,3", "os": "iPhone OS", "timezone_offset": -10800}, "conn_type": "CARRIER", "source": "ADVERTISER_SDK", "sdk_version": "3.3.0", "version": "2", "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13F69", "device_type": "HANDHELD", "user_language": "en", "bundle_name": "com.biggu.shopsavvyapp", "carrier_name": "Koodo", "application_version": "6", "event": {"event_name": "Launch", "timestamp": 1476057596865, "key_value": [{"long_value": 1475804891940, "key": "R1_Firstlaunchts"}], "transaction_id": "92.117.48.48.49.99.92.117.48.48.102.53.92.117.48.48.51.51.92.117.48.48.52.101.92.117.48.48.99.48.92.117.48.48.52.49.92.117.48.48.52.101.92.117.48.48.56.99.92.117.48.48.56.48.92.117.48.48.51.101.92.117.48.48.57.99.92.117.48.48.57.57.92.117.48.48.51.101.92.117.48.48.50.48.92.117.48.48.57.97.92.117.48.48.49.54", "session_id": "8A38D058-EEFC-43BB-BC55-C94A8D92C044"}}, "ip_address": "127.0.0.1", "x_forwarded_for": "142.169.78.197", "fileid": 222259999}
		try:
			od = testTarget.convert_hive_row_to_keen_format(hiveRow)
		except KeyError:
			result = True
		self.assertEqual(True,result)

	def test_transformDummyData(self):
		testTarget = TrackerEventTSTransformer()
		hiveRow = dict()
		hiveRow = {'message': {'device_info':{'id_info':{'foo':'bar'}},'event':{'foo':'bar'}},'timestamp':'1475881135'}
		od = testTarget.convert_hive_row_to_keen_format(hiveRow)
		self.assertEqual(len(od.keys()),3)

	def test_transformRealData(self):
		testTarget = TrackerEventTSTransformer()
		hiveRow = dict()
		hiveRow = {"message": {"carrier_country": "ca", "screen_resolution": "640x1136", "application_id": "com.biggu.shopsavvyapp", "viewport_size": "320x568", "device_info": {"id_info": {"idfv": "ADEBBBE7-CF5C-4DD8-9238-25506D877543", "opt_out": "false", "r1_daid": "B817AF40-FB61-460C-B997-55F2594B928A", "idfa": "16F30910-D606-400D-8BE7-AC2C553A66BC", "dpid_md5": "b85caad2443c0d85999c199afd1038ad", "dpid": "16F30910-D606-400D-8BE7-AC2C553A66BC", "dpid_sha1": "2f0178dd6e7ff6726aa00bb3d5ae8ac5eb960c99"}, "country": "CA", "make": "Apple", "os_language": "en-CA", "os_version": "9.3.2", "ip_v4": "92.117.48.48.48.97.92.117.48.48.57.100.92.117.48.48.98.100.92.117.48.48.99.53", "model": "iPhone5,3", "os": "iPhone OS", "timezone_offset": -10800}, "conn_type": "CARRIER", "source": "ADVERTISER_SDK", "sdk_version": "3.3.0", "version": "2", "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13F69", "device_type": "HANDHELD", "user_language": "en", "bundle_name": "com.biggu.shopsavvyapp", "carrier_name": "Koodo", "application_version": "6", "event": {"event_name": "Launch", "timestamp": 1476057596865, "key_value": [{"long_value": 1475804891940, "key": "R1_Firstlaunchts"}], "transaction_id": "92.117.48.48.49.99.92.117.48.48.102.53.92.117.48.48.51.51.92.117.48.48.52.101.92.117.48.48.99.48.92.117.48.48.52.49.92.117.48.48.52.101.92.117.48.48.56.99.92.117.48.48.56.48.92.117.48.48.51.101.92.117.48.48.57.99.92.117.48.48.57.57.92.117.48.48.51.101.92.117.48.48.50.48.92.117.48.48.57.97.92.117.48.48.49.54", "session_id": "8A38D058-EEFC-43BB-BC55-C94A8D92C044"}}, "ip_address": "127.0.0.1", "x_forwarded_for": "142.169.78.197", "fileid": 222259999}
		od = testTarget.convert_hive_row_to_keen_format(hiveRow)
		self.assertEqual(len(od.keys()),38)

	def test_logging(self):
		testTarget = TrackerEventTSTransformer()
		config.loglevel = logging.DEBUG
		self.assertEqual(True,True)
