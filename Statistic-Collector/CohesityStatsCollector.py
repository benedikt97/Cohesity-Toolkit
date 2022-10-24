# Stats Collector for Cohesity Environment (C) fgn GmbH

from cohesity_management_sdk.cohesity_client import CohesityClient
from numpy import int64
import pandas as pd
import time
import os
import datetime


##Login
print('Welcome to Cohesity Stats Collector (C) fgn GmbH')
username = input('Enter Username: ')
password = input('Enter Password: ')
ts_days = int64(input('Days to Collect: '))
cluster_vip = '10.200.250.210'

## Get Nodes and First Information
cclient = CohesityClient(cluster_vip, username, password)
stats_controller = cclient.statistics
nodelist = stats_controller.get_entities('kSentryNodeStats', metric_names='kCpuUsagePct')
print(nodelist[1].attribute_vec[1].value.data.int_64_value)
print('Found Nodes: ')
print('######### Cohesity CPU Utilization ##########')
for node in nodelist:
    nodeid = node.attribute_vec[0].value.data.int_64_value
    cpuusage = node.latest_metric_vec[0].value.data.double_value      
    print('NodeID: ' + str(nodeid) + ' - CPU Usage: ' + str(cpuusage))


##Create Pandas Dataframe
temp_dict = {'NodeID' :[], 'Time' : [], 'CPU' :[], 'Memory' : []}
df = pd.DataFrame(temp_dict)

## Get Time Series Information
date = datetime.datetime.utcnow()
unix_timestamp_now = int64(datetime.datetime.timestamp(date)*1000)
unix_timestamp_start = (unix_timestamp_now - (ts_days * 24 * 60 *60 * 1000))
print('Datetime: ',date, ' Unit Timestamp: ', unix_timestamp_now, ' - ', unix_timestamp_start)

l_ts = []
l_cpu = []
l_id = []
l_nodectr = []
l_rx_raw = []
l_tx_raw = []

nodectr = 0

for node in nodelist:
    nodeid = node.attribute_vec[0].value.data.int_64_value
    print('Fetching Node: ', nodeid)

    ## CPU
    cpu_timeseries = stats_controller.get_time_series_stats('kSentryNodeStats', 'kCpuUsagePct', unix_timestamp_start, entity_id=nodeid, entity_id_list=None, end_time_msecs=None, rollup_function=None, rollup_interval_secs=None)
    nodectr += 1
    print('Processing CPU Data from Node: ', nodeid)
    for metric in cpu_timeseries.data_point_vec:
        l_ts.append(metric.timestamp_msecs*1000) 
        l_cpu.append(metric.data.double_value) 
        l_id.append(nodeid)
        l_nodectr.append(nodectr)
        
print(str(len(l_ts)), str(len(l_cpu)), str(len(l_id)), str(len(l_nodectr)))
temp_dict = {'time' : l_ts, 'nodectr' : l_nodectr, 'nodeid' : l_id, 'cpu' :l_cpu}
df = pd.DataFrame(temp_dict)
df.to_csv('cohesity_cpu.csv')
l_ts.clear()
l_nodectr.clear()
l_id.clear()
l_nodectr.clear()




##Interface Stats
interfaces = []
temp_interface_list = stats_controller.get_entities('kInterfaceStats', include_aggr_metric_sources=None, metric_names=None, max_entities=None)
for interface in temp_interface_list:
    ifn = interface.entity_id.entity_id.data.string_value
    if str(ifn).find('bond') != -1:
        interfaces.append(ifn)

for interface in interfaces:
    print('Processing Network RX Data from Node: ', interface)
    timeseries = stats_controller.get_time_series_stats('kInterfaceStats', 'kRxBytes', unix_timestamp_start, entity_id=interface, entity_id_list=None, end_time_msecs=None, rollup_function=None, rollup_interval_secs=None)
    for metric in timeseries.data_point_vec:
        l_ts.append(metric.timestamp_msecs*1000) 
        l_rx_raw.append(metric.data.double_value) 
        l_tx_raw.append(None)
        l_id.append(interface)
        l_nodectr.append(nodectr)

    timeseries = stats_controller.get_time_series_stats('kInterfaceStats', 'kTxBytes', unix_timestamp_start, entity_id=interface, entity_id_list=None, end_time_msecs=None, rollup_function=None, rollup_interval_secs=None)
    for metric in timeseries.data_point_vec:
        l_ts.append(metric.timestamp_msecs*1000) 
        l_tx_raw.append(metric.data.double_value)
        l_rx_raw.append(None) 
        l_id.append(interface)
        l_nodectr.append(nodectr)


print(str(len(l_ts)), str(len(l_ts)), str(len(l_id)), str(len(l_rx_raw)))
temp_dict = {'time' : l_ts, 'interface' : l_id, 'rx' :l_rx_raw, 'tx' :l_tx_raw}
df = pd.DataFrame(temp_dict)
df.to_csv('cohesity_intf.csv')








