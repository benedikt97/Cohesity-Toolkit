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

ts = []
cpu = []
nodeids = []
nodectrl = []

nodectr = 0

for node in nodelist:
    nodeid = node.attribute_vec[0].value.data.int_64_value
    print('Fetching Node: ', nodeid)

    ## CPU
    cpu_timeseries = stats_controller.get_time_series_stats('kSentryNodeStats', 'kCpuUsagePct', unix_timestamp_start, entity_id=nodeid, entity_id_list=None, end_time_msecs=None, rollup_function=None, rollup_interval_secs=None)
    nodectr += 1
    print('Processing CPU Data from Node: ', nodeid)
    for metric in cpu_timeseries.data_point_vec:
        ts.append(metric.timestamp_msecs*1000) 
        cpu.append(metric.data.double_value) 
        nodeids.append(nodeid)
        nodectrl.append(nodectr)



    
    temp_dict = {'time' : ts, 'nodectr' : nodectrl, 'nodeid' : nodeids, 'cpu' :cpu}
df = pd.DataFrame(temp_dict)
df.to_csv('cohesity_cpu.csv')


ts = []
interfacename = []
nodectrl = []
rx_raw = []
rxbs = []


##Interface Stats
interfaces = []
interface_list = stats_controller.get_entities('kInterfaceStats', include_aggr_metric_sources=None, metric_names=None, max_entities=None)
for interface in interface_list:
    ifn = interface.entity_id.entity_id.data.string_value
    if str(ifn).find('bond') != -1:
        interfaces.append(ifn)

for interface in interfaces:
    ts = []
    interfacename = []
    rxbs = []
    print('Processing Network RX Data from Node: ', interface)
    timeseries = stats_controller.get_time_series_stats('kInterfaceStats', 'kRxBytes', unix_timestamp_start, entity_id=interface, entity_id_list=None, end_time_msecs=None, rollup_function=None, rollup_interval_secs=None)
    for metric in timeseries.data_point_vec:
        ts.append(metric.timestamp_msecs*1000) 
        rx_raw.append(metric.data.double_value) 
        interfacename.append(interface)
        nodectrl.append(nodectr)

    for i in range(len(rx_raw)):
        if i == 0:
            p = 1
        else:
            p = i
        
        rx_delta = rx_raw[p] - rx_raw[p-1]
        rxbs.append(rx_delta)
    
    print(len(rxbs))
    print(len(rx_raw))


temp_dict = {'time' : ts, 'interface' : interfacename, 'rx' :rxbs}
df = pd.DataFrame(temp_dict)
df.to_csv('cohesity_intf.csv')








