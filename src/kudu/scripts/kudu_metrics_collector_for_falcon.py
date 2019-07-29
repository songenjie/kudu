#!/usr/bin/env python

import argparse
import commands
import gzip
import logging
from logging.handlers import RotatingFileHandler
import multiprocessing
import os
import signal
import StringIO
import threading
import time
import traceback
import types
import urllib2
import kudu_utils
try:
    import ujson
except ImportError:
    import json as ujson

g_metric_types_inited = False
g_metric_type = {'replica_count': 'GAUGE'}
g_last_metric_urls_updated_time = 0
g_is_running = True
g_args = {}
if os.path.exists(kudu_utils.g_script_path + '/kudu'):
    g_kudu_bin = kudu_utils.g_script_path + '/kudu'
elif os.path.exists(os.environ['KUDU_HOME'] + '/build/release/bin/kudu'):
    g_kudu_bin = os.environ['KUDU_HOME'] + '/build/release/bin/kudu'
else:
    kudu_utils.LOG.warn('Could not find kudu bin path, exit.')
    exit()

""" origin metrics are as follows, but we only use `percentile_99` now
"""
# {
#   'name': 'xxx',
#   'total_count': 0,
#   'min': 0,
#   'mean': 0,
#   'percentile_75': 0,
#   'percentile_95': 0,
#   'percentile_99': 0,
#   'percentile_99_9': 0,
#   'percentile_99_99': 0,
#   'max': 0,
#   'total_sum': 0
# }
g_percentiles = ['percentile_99']


def collector_assert(condition, msg):
    if not condition:
        kudu_utils.LOG.error(msg)


def interruptable_sleep(sec):
    while g_is_running and sec >= 1:
        time.sleep(1)
        sec -= 1
    if not g_is_running:
        return

    time.sleep(sec)


def get_hostname(host_port):
    return host_port.split(':')[0]


def update_metric_urls(host_urls, cur_time, role):
    global g_args
    global g_last_metric_urls_updated_time
    if cur_time - g_last_metric_urls_updated_time < 300:    # update_metric_urls every 5 minutes
        return host_urls
    kudu_utils.LOG.info('Start to update_metric_urls()')
    cmd = '%s %s list @%s -columns=http-addresses -format=json -timeout_ms=%d 2>/dev/null'\
          % (g_kudu_bin, role, g_args.cluster_name, 1000*g_args.get_metrics_timeout)
    kudu_utils.LOG.info('request server list: %s' % cmd)
    status, output = commands.getstatusoutput(cmd)
    host_urls = {}
    if status == 0:
        for entry in ujson.loads(output):
            url = 'http://' + entry['http-addresses'] + '/metrics?compact=1'
            if len(g_args.metrics):
                url += '&metrics=' + ','.join(g_args.metrics)
            if not g_args.local_stat:
                url += '&origin=false&merge=true'
            host_urls[get_hostname(entry['http-addresses'])] = url
    else:
        kudu_utils.LOG.warning('request server list failed, error: %d %s' % (status, output))
    g_last_metric_urls_updated_time = cur_time
    return host_urls


def push_to_falcon_agent(origin_data):
    global g_args
    if len(origin_data) == 0 or g_args.local_stat:
        return
    if g_args.falcon_url != '':
        data = ujson.dumps(origin_data)
        request = urllib2.Request(g_args.falcon_url, data)
        response = urllib2.urlopen(request).read()         # TODO async
        kudu_utils.LOG.info('Pushed %d data to %s. response=%s' % (len(data), g_args.falcon_url, response))
    else:
        kudu_utils.LOG.info(ujson.dumps(origin_data, indent=2))


def get_origin_metric(url):
    global g_args
    kudu_utils.LOG.info('Start to get_origin_metric()')
    request = urllib2.Request(url)
    request.add_header('Accept-encoding', 'gzip')
    try:
        response = urllib2.urlopen(request, timeout=g_args.get_metrics_timeout)
        if response.info().get('Content-Encoding') == 'gzip':
            buf = StringIO.StringIO(response.read())
            return gzip.GzipFile(fileobj=buf).read()
        else:
            return response.read()
    except Exception as e:
        kudu_utils.LOG.error('url: %s, error: %s' % (url, e))
        return '[]'


# TODO are master's and tserver's metrics are the same?
# TODO not needed to fetch all
def init_metric_types_once(host_urls, timeout):
    global g_args
    global g_metric_types_inited
    if g_metric_types_inited:
        return
    kudu_utils.LOG.info('Start to init_metric_types_once()')
    for host_url in host_urls.values():
        origin_metric = get_origin_metric(host_url + '&include_schema=1')
        for block in ujson.loads(origin_metric):
            for metric in block['metrics']:
                g_metric_type[metric['name']] = metric['type'].upper()
            if block['type'] in ('table', 'tablet'):
                g_metric_types_inited = True
                return      # init once is ok


def get_value(metric_name, metric_value):
    # metrics in {'state'} are type of string gauge, otherwise are in type of int
    if metric_name in {'state'}:
        collector_assert(isinstance(metric_value, (basestring, str)),
                         'type of metric %s:%s must be str' % (metric_name, metric_value))
        if metric_name == 'state':
            return 1 if len(metric_value.replace('RUNNING', '')) == 0 else 0
        collector_assert('Unhandled type of metric %s:%s' % (metric_name, metric_value))

    collector_assert(isinstance(metric_value, (int, types.LongType)),
                     'type of metric %s:%s must be int' % (metric_name, metric_value))
    return metric_value


def construct_falcon_item(endpoint, metric_name, level, timestamp,
                          metric_value, counter_type, extra_tags=''):
    global g_args
    return {'endpoint': endpoint,
            'metric': metric_name,
            'tags': 'service=kudu,cluster=%s,level=%s,v=3%s'
                    % (g_args.cluster_name, level, extra_tags),
            'timestamp': timestamp,
            'step': g_args.metric_period,
            'value': metric_value,
            'counterType': counter_type}


# [in]:  block
# [out]: host_tables_metrics
#        host_tables_metrics_histogram,
#        host_metrics
#        host_metrics_histogram
def parse_tablet_metrics(block,
                         tables_metrics,
                         tables_metrics_histogram,
                         host_metrics,
                         host_metrics_histogram):
    global g_args
    for metric in block['metrics']:
        metric_name = metric['name']
        if metric_name not in g_metric_type:
            kudu_utils.LOG.error('metric %s not in g_metric_type %s' % (metric_name, str(g_metric_type)))
            continue
        if g_metric_type[metric_name] in ['GAUGE', 'COUNTER']:
            metric_value = metric['value']
            parsed_value = get_value(metric_name, metric_value)
            if g_args.local_stat:
                print('%s %s %s %s %d' % (block['attributes']['table_name'],
                                          block['attributes']['partition'],
                                          block['id'],
                                          metric['name'],
                                          parsed_value))

            # host table level metric
            table_name = block['attributes']['table_name']
            if table_name not in tables_metrics.keys():
                tables_metrics[table_name] = {'replica_count': 0}
            if metric_name not in tables_metrics[table_name].keys():
                tables_metrics[table_name][metric_name] = 0
            tables_metrics[table_name][metric_name] += parsed_value

            # host level metric
            if metric_name in host_metrics.keys():
                host_metrics[metric_name] += parsed_value
            else:
                host_metrics[metric_name] = parsed_value
        elif g_metric_type[metric_name] == 'HISTOGRAM':
            for percentile in g_percentiles:
                if g_args.local_stat:
                    print('%s %s %s %s %d' % (block['attributes']['table_name'],
                                              block['attributes']['partition'],
                                              block['id'],
                                              metric['name'],
                                              metric[percentile]))

                if percentile in metric.keys():
                    # host table level metric, histogram type
                    metric_name_histogram = metric_name + '_' + percentile
                    table_name = block['attributes']['table_name']
                    if table_name not in tables_metrics.keys():
                        tables_metrics[table_name] = {'replica_count': 0}
                    if table_name not in tables_metrics_histogram.keys():
                        tables_metrics_histogram[table_name] = {}
                    if metric_name_histogram not in tables_metrics_histogram[table_name].keys():
                        # origin data in list, calculate later
                        tables_metrics_histogram[table_name][metric_name_histogram] = []
                    tables_metrics_histogram[table_name][metric_name_histogram]\
                        .append((metric['total_count'], metric[percentile]))

                    # host level metric, histogram type
                    if metric_name_histogram not in host_metrics_histogram.keys():
                        # origin data in list, calculate later
                        host_metrics_histogram[metric_name_histogram] = []
                    host_metrics_histogram[metric_name_histogram].\
                        append((metric['total_count'], metric[percentile]))
        else:
            collector_assert(False, '%s not support' % g_metric_type[metric_name])


# [in]:  block
# [out]: host_tables_metrics
#        host_tables_metrics_histogram,
#        host_metrics
#        host_metrics_histogram
def parse_table_metrics(block,
                        tables_metrics,
                        tables_metrics_histogram,
                        host_metrics,
                        host_metrics_histogram):
    for metric in block['metrics']:
        metric_name = metric['name']
        if metric_name not in g_metric_type:
            kudu_utils.LOG.error('metric %s not in g_metric_type %s' % (metric_name, str(g_metric_type)))
            continue
        if g_metric_type[metric_name] in ['GAUGE', 'COUNTER']:
            metric_value = metric['value']
            parsed_value = get_value(metric_name, metric_value)
            # host table level metric
            table_name = block['id']
            # TODO how to calculate replica count on a host
            if table_name not in tables_metrics.keys():
                tables_metrics[table_name] = {}
            collector_assert(metric_name not in tables_metrics[table_name].keys(),
                             '%s duplicated' % metric_name)
            tables_metrics[table_name][metric_name] = parsed_value

            # host level metric
            if metric_name in host_metrics.keys():
                host_metrics[metric_name] += parsed_value
            else:
                host_metrics[metric_name] = parsed_value
        elif g_metric_type[metric_name] == 'HISTOGRAM':
            for percentile in g_percentiles:
                if percentile in metric.keys():
                    # host table level metric, histogram type
                    metric_name_histogram = metric_name + '_' + percentile
                    table_name = block['id']
                    if table_name not in tables_metrics_histogram.keys():
                        tables_metrics_histogram[table_name] = {}
                    collector_assert(
                        metric_name_histogram not in tables_metrics_histogram[table_name].keys(),
                        '%s duplicated' % metric_name_histogram)
                    tables_metrics_histogram[table_name][metric_name_histogram] = []
                    # origin data in list, calculate later. keep compatible with parse_table_metrics
                    tables_metrics_histogram[table_name][metric_name_histogram] \
                        .append((metric['total_count'], metric[percentile]))

                    # host level metric, histogram type
                    if metric_name_histogram not in host_metrics_histogram.keys():
                        # origin data in list, calculate later
                        host_metrics_histogram[metric_name_histogram] = []
                    host_metrics_histogram[metric_name_histogram].append((metric['total_count'],
                                                                          metric[percentile]))
        else:
            collector_assert(False, '%s not support' % g_metric_type[metric_name])


def get_disk_size(block,
                  on_disk_size,
                  on_disk_data_size):
    for metric in block['metrics']:
        if metric['name'] == 'on_disk_size':
            on_disk_size += metric['value']
        if metric['name'] == 'on_disk_data_size':
            on_disk_data_size += metric['value']
    return on_disk_size, on_disk_data_size


def get_host_metrics(hostname, url):
    global g_args
    kudu_utils.LOG.info('Start get metrics from [%s]' % url)
    metrics_json_str = get_origin_metric(url)

    host_metrics = {}
    host_metrics_histogram = {}
    tables_metrics = {}
    tables_metrics_histogram = {}
    on_disk_size = 0
    on_disk_data_size = 0
    for block in ujson.loads(metrics_json_str):
        if block['type'] == 'tablet':
            if (len(g_args.tables_set) == 0 or                                  # no table filter
                (block['attributes']
                 and block['attributes']['table_name']
                 and block['attributes']['table_name'] in g_args.tables_set)):  # table match
                parse_tablet_metrics(block,
                                     tables_metrics,
                                     tables_metrics_histogram,
                                     host_metrics,
                                     host_metrics_histogram)
                # 'replica_count' is an extra metric
                tables_metrics[block['attributes']['table_name']]['replica_count'] += 1
            on_disk_size, on_disk_data_size = get_disk_size(block, on_disk_size, on_disk_data_size)
        elif block['type'] == 'table':
            if (len(g_args.tables_set) == 0 or                                  # no table filter
                    block['id'] in g_args.tables_set):                          # table match
                parse_table_metrics(block,
                                    tables_metrics,
                                    tables_metrics_histogram,
                                    host_metrics,
                                    host_metrics_histogram)
            on_disk_size, on_disk_data_size = get_disk_size(block, on_disk_size, on_disk_data_size)
    host_falcon_data = []
    timestamp = int(time.time())

    # host_table level
    host_tables_metrics = {}
    for table, metrics in tables_metrics.iteritems():
        for metric, value in metrics.iteritems():
            # add the host metrics to table metrics
            if table not in host_tables_metrics.keys():
                host_tables_metrics[table] = {}
            if metric not in host_tables_metrics[table].keys():
                host_tables_metrics[table][metric] = 0
            host_tables_metrics[table][metric] += value

            host_falcon_data.append(
                construct_falcon_item(endpoint=hostname,
                                      metric_name=metric,
                                      level='host_table',
                                      extra_tags=',table=%s' % table,
                                      timestamp=timestamp,
                                      metric_value=value,
                                      counter_type=g_metric_type[metric]))

    # host_table level, histogram type
    host_tables_metrics_histogram = {}
    for table, metrics in tables_metrics_histogram.iteritems():
        for metric, hist_list in metrics.iteritems():
            # add the host histogram metrics to table metrics
            if table not in host_tables_metrics_histogram.keys():
                host_tables_metrics_histogram[table] = {}
            if metric not in host_tables_metrics_histogram[table].keys():
                host_tables_metrics_histogram[table][metric] = []
            host_tables_metrics_histogram[table][metric].append(hist_list)

            total_count = 0
            total_value = 0
            for hist in hist_list:
                total_count += hist[0]
                total_value += hist[0] * hist[1]

            value = int(total_value / total_count) if total_count != 0 else 0
            host_falcon_data.append(
                construct_falcon_item(endpoint=hostname,
                                      metric_name=metric,
                                      level='host_table',
                                      extra_tags=',table=%s' % table,
                                      timestamp=timestamp,
                                      metric_value=value,
                                      counter_type='GAUGE'))

    # host level
    for metric, value in host_metrics.iteritems():
        host_falcon_data.append(
            construct_falcon_item(endpoint=hostname,
                                  metric_name=metric,
                                  level='host',
                                  timestamp=timestamp,
                                  metric_value=value,
                                  counter_type=g_metric_type[metric]))

    # host level, histogram type
    for metric, hist_list in host_metrics_histogram.iteritems():
        total_count = 0
        total_value = 0
        for hist in hist_list:
            total_count += hist[0]
            total_value += hist[0]*hist[1]

        value = int(total_value / total_count) if total_count != 0 else 0
        host_falcon_data.append(
            construct_falcon_item(endpoint=hostname,
                                  metric_name=metric,
                                  level='host',
                                  timestamp=timestamp,
                                  metric_value=value,
                                  counter_type='GAUGE'))

    push_to_falcon_agent(host_falcon_data)
    return host_tables_metrics, host_tables_metrics_histogram, on_disk_size, on_disk_data_size


def get_server_health_status(status):
    if status == 'HEALTHY':
        return 0
    if status == 'UNAUTHORIZED':
        return 1
    if status == 'UNAVAILABLE':
        return 2
    if status == 'WRONG_SERVER_UUID':
        return 3
    return 999


def get_table_health_status(status):
    if status == 'HEALTHY':
        return 0
    if status == 'RECOVERING':
        return 1
    if status == 'UNDER_REPLICATED':
        return 2
    if status == 'UNAVAILABLE':
        return 3
    if status == 'CONSENSUS_MISMATCH':
        return 4
    return 999


def get_health_falcon_data():
    global g_args
    kudu_utils.LOG.info('Start to get_health_falcon_data()')
    ksck_falcon_data = []
    cmd = '%s cluster ksck @%s -consensus=false'\
          ' -ksck_format=json_compact -color=never'\
          ' -sections=MASTER_SUMMARIES,TSERVER_SUMMARIES,TABLE_SUMMARIES,TOTAL_COUNT'\
          ' -timeout_ms=%d 2>/dev/null'\
          % (g_kudu_bin, g_args.cluster_name, 1000*g_args.get_metrics_timeout)
    kudu_utils.LOG.info('request cluster ksck info: %s' % cmd)
    status, output = commands.getstatusoutput(cmd)
    if status == 0 or status == 256:
        timestamp = int(time.time())
        ksck_info = ujson.loads(output)
        healthy_table_count = 0
        for master in ksck_info['master_summaries']:
            ksck_falcon_data.append(
                construct_falcon_item(endpoint=get_hostname(master['address']),
                                      metric_name='kudu-master-health',
                                      level='host',
                                      timestamp=timestamp,
                                      metric_value=get_server_health_status(master['health']),
                                      counter_type='GAUGE'))
        for tserver in ksck_info['tserver_summaries']:
            ksck_falcon_data.append(
                construct_falcon_item(endpoint=get_hostname(tserver['address']),
                                      metric_name='kudu-tserver-health',
                                      level='host',
                                      timestamp=timestamp,
                                      metric_value=get_server_health_status(tserver['health']),
                                      counter_type='GAUGE'))
        if 'table_summaries' in ksck_info:
            healthy_table_count = 0
            table_count = len(ksck_info['table_summaries'])
            for table in ksck_info['table_summaries']:
                ksck_falcon_data.append(
                    construct_falcon_item(endpoint=table['name'],
                                          metric_name='kudu-table-health',
                                          level='table',
                                          timestamp=timestamp,
                                          metric_value=get_table_health_status(table['health']),
                                          counter_type='GAUGE'))
                if get_table_health_status(table['health']) == 0:
                    healthy_table_count += 1
            ksck_falcon_data.append(
                construct_falcon_item(endpoint=g_args.cluster_name,
                                      metric_name='healthy_table_proportion',
                                      level='cluster',
                                      timestamp=timestamp,
                                      metric_value=int(100*healthy_table_count/table_count),
                                      counter_type='GAUGE'))
        if len(ksck_info['count_summaries']) == 1:
            cluster_stat = ksck_info['count_summaries'][0]
            for key, value in cluster_stat.items():
                ksck_falcon_data.append(
                    construct_falcon_item(endpoint=g_args.cluster_name,
                                          metric_name=key+'_count',
                                          level='cluster',
                                          timestamp=timestamp,
                                          metric_value=value,
                                          counter_type='GAUGE'))
    else:
        kudu_utils.LOG.error('failed to fetch ksck info. status: %d, output: %s' % (status, output))

    push_to_falcon_agent(ksck_falcon_data)


def main_loop():
    global g_args
    global g_is_running
    kudu_utils.LOG.info('Start to main_loop()')
    host_urls = {}
    while g_is_running:
        start = time.time()

        host_urls = update_metric_urls(host_urls, start, 'tserver')
        init_metric_types_once(host_urls, g_args.get_metrics_timeout)
        if len(host_urls) == 0:
            kudu_utils.LOG.warn('Detect 0 hosts, try to fetch hosts later')
            interruptable_sleep(10)
            continue

        try:
            results = []
            count = len(host_urls)
            if g_args.local_stat:
                count = 1
            pool = multiprocessing.Pool(count)
            for host, url in host_urls.items():
                results.append(pool.apply_async(get_host_metrics, (host, url)))
            pool.close()
            pool.join()

            timestamp = int(time.time())
            table_falcon_data = []

            tables_metrics = {}
            tables_metrics_histogram = {}
            cluster_on_disk_size = 0
            cluster_on_disk_data_size = 0
            for result in results:
                try:
                    host_tables_metrics, host_tables_metrics_histogram, on_disk_size, on_disk_data_size = result.get()
                except Exception as e:
                    kudu_utils.LOG.error(traceback.format_exc())
                    continue

                # sum up table metrics
                for table, metrics in host_tables_metrics.iteritems():
                    for metric, value in metrics.iteritems():
                        if table not in tables_metrics.keys():
                            tables_metrics[table] = {}
                        if metric not in tables_metrics[table].keys():
                            tables_metrics[table][metric] = 0
                        tables_metrics[table][metric] += value

                # sum up table histogram metrics
                for table, metrics in host_tables_metrics_histogram.iteritems():
                    for metric, hist_list in metrics.iteritems():
                        if table not in tables_metrics_histogram.keys():
                            tables_metrics_histogram[table] = {}
                        if metric not in tables_metrics_histogram[table].keys():
                            tables_metrics_histogram[table][metric] = []
                        tables_metrics_histogram[table][metric].append(hist_list)

                # sum up disk size
                cluster_on_disk_size += on_disk_size
                cluster_on_disk_data_size += on_disk_data_size

            # table level
            for table, metrics in tables_metrics.iteritems():
                for metric, value in metrics.iteritems():
                    if g_args.local_stat:
                        print('%s %s %d' % (metric, table, value))
                    table_falcon_data.append(
                        construct_falcon_item(endpoint=table,
                                              metric_name=metric,
                                              level='table',
                                              timestamp=timestamp,
                                              metric_value=value,
                                              counter_type=g_metric_type[metric]))

            # table level, histogram type
            for table, metrics in tables_metrics_histogram.iteritems():
                for metric, hist_list in metrics.iteritems():
                    total_count = 0
                    total_value = 0
                    for hist_in_list in hist_list:
                        for hist in hist_in_list[0]:
                            total_count += hist[0]
                            total_value += hist[0] * hist[1]
                    value = int(total_value / total_count) if total_count != 0 else 0
                    if g_args.local_stat:
                        print('%s %s %d' % (metric, table, value))
                    table_falcon_data.append(
                        construct_falcon_item(endpoint=table,
                                              metric_name=metric,
                                              level='table',
                                              timestamp=timestamp,
                                              metric_value=value,
                                              counter_type='GAUGE'))

            #cluster_level
            push_to_falcon_agent([
                construct_falcon_item(endpoint=g_args.cluster_name,
                                      metric_name='on_disk_size',
                                      level='cluster',
                                      timestamp=timestamp,
                                      metric_value=cluster_on_disk_size,
                                      counter_type='GAUGE'),
                construct_falcon_item(endpoint=g_args.cluster_name,
                                      metric_name='on_disk_data_size',
                                      level='cluster',
                                      timestamp=timestamp,
                                      metric_value=cluster_on_disk_data_size,
                                      counter_type='GAUGE')])

            push_to_falcon_agent(table_falcon_data)

            if g_args.local_stat:
                return

            get_health_falcon_data()          # TODO put it into another process
        except Exception as e:
            kudu_utils.LOG.error(traceback.format_exc())

        end = time.time()
        used_time = end - start
        left_time = g_args.metric_period - used_time
        if left_time > 0:
            kudu_utils.LOG.info('Sleep for %s seconds' % left_time)
            interruptable_sleep(left_time)
        else:
            kudu_utils.LOG.warn('Collect timeout, cost %f secs this time' % used_time)
    kudu_utils.LOG.warn('main_loop exit')


def rebalance_cluster():
    global g_args
    global g_is_running
    if not g_args.rebalance_cluster:
        kudu_utils.LOG.warning('Cluster rebalance is disabled')
        return

    hour, minute = g_args.rebalance_time.split(':')
    if not int(hour) in range(0, 24) or not int(minute) in range(0, 60):
        kudu_utils.LOG.error('Cluster rebalance time is invalid')
        return

    kudu_utils.LOG.info('Start to rebalance_cluster()')
    # min interval of rebalance, 12 hours is large enough,
    # because there is only 1 rebalance task everyday
    rebalance_min_interval = 12 * 3600
    last_rebalance_time = 0
    while g_is_running:
        if time.strftime("%H:%M", time.localtime()) == g_args.rebalance_time \
                and time.time() - last_rebalance_time > rebalance_min_interval:
            start = time.time()
            cmd = '%s cluster rebalance @%s -tables=%s 2>/dev/null' \
                  % (g_kudu_bin, g_args.cluster_name, g_args.rebalance_tables)
            kudu_utils.LOG.info('request cluster rebalance info: %s' % cmd)
            status, output = commands.getstatusoutput(cmd)
            if status == 0 or status == 256:
                kudu_utils.LOG.info('rebalance output: %s' % output)
            else:
                kudu_utils.LOG.error('failed to perform rebalance. status: %d, output: %s' % (status, output))
            end = time.time()
            last_rebalance_time = end
            used_time = end - start
            kudu_utils.LOG.info('Cluster rebalance cost %s seconds' % used_time)
        else:
            interruptable_sleep(30)
    kudu_utils.LOG.info('rebalance_cluster exit')


def parse_command_line():
    kudu_utils.LOG.info('Start to parse_command_line()')
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='Collect apache kudu cluster metrics.')

    parser.add_argument('--cluster_name', type=str, help='Kudu cluster name',
                        required=True)

    parser.add_argument('--metric_period', type=int, help='The period to fetch metrics in seconds',
                        required=False, default=60)

    parser.add_argument('--local_stat', dest='local_stat',
                        help='Whether to calculate statistics on local host',
                        action='store_true')
    parser.set_defaults(local_stat=False)

    parser.add_argument('--get_metrics_timeout', type=int,
                        help='The timeout to fetch metrics from server in seconds',
                        required=False, default=3)

    parser.add_argument('--falcon_url', type=str, help='The falcon url to push metrics to',
                        required=False, default='http://127.0.0.1:1988/v1/push')

    parser.add_argument('--tables', type=str, help='Table names to collect metrics',
                        required=False, default='')

    parser.add_argument('--metrics', type=str, help='Metrics to collect',
                        required=False, default='')

    parser.add_argument('--rebalance_cluster', dest='rebalance_cluster',
                        help='Whether to rebalance cluster regularly',
                        action='store_true')
    parser.set_defaults(rebalance_cluster=False)

    parser.add_argument('--rebalance_tables', type=str, help='Table names to rebalance',
                        required=False, default='')

    parser.add_argument('--rebalance_time', type=str,
                        help='Time to perform cluster rebalance, format is HH:MM (e.g.00:00)',
                        required=False, default='00:00')

    global g_args
    g_args = parser.parse_args()
    g_args.cluster_name = g_args.cluster_name
    g_args.tables_set = set(filter(lambda x: x != '', g_args.tables.split(',')))
    g_args.metrics = filter(lambda x: x != '', g_args.metrics.split(','))


def term_sig_handler(signum, _):
    kudu_utils.LOG.warn('pid %d ppid %d catch signal: %d' % (os.getpid(), os.getppid(), signum))
    global g_is_running
    g_is_running = False


def main():
    signal.signal(signal.SIGTERM, term_sig_handler)
    signal.signal(signal.SIGINT, term_sig_handler)
    parse_command_line()
    try:
        # perform cluster rebalance in another process
        t = threading.Thread(target=rebalance_cluster, args=())
        t.start()
        main_loop()
    except KeyboardInterrupt:
        kudu_utils.LOG.warn('Collector terminated.')
        global g_is_running
        g_is_running = False


if __name__ == '__main__':
    main()
