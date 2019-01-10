#!/usr/bin/env python

import argparse
import commands
import gzip
import logging.config
import multiprocessing
import os
import signal
import sys
import StringIO
import threading
import time
import traceback
import types
import urllib2
try:
    import ujson
except ImportError:
    import json as ujson

reload(sys)
sys.setdefaultencoding('utf-8')

IS_RUNNING = True
args = {}

curr_dir = os.path.dirname(os.path.realpath(__file__))
logging.config.fileConfig(curr_dir + os.sep + 'logger.conf')
LOG = logging.getLogger('Collector')

g_metric_types_inited = False
g_metric_type = {'replica_count': 'GAUGE'}
g_last_metric_urls_updated_time = 0

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
HISTOGRAM_METRICS = ['percentile_99']


def interruptable_sleep(sec):
    while IS_RUNNING and sec >= 1:
        time.sleep(1)
        sec -= 1
    if not IS_RUNNING:
        return

    time.sleep(sec)


def get_hostname(host_port):
    return host_port.split(':')[0]


def update_metric_urls(host_urls, cur_time, kudu_master_rpcs, role, metrics):
    global g_last_metric_urls_updated_time
    if cur_time - g_last_metric_urls_updated_time < 300:    # update_metric_urls every 5 minutes
        return host_urls
    LOG.info('Start to update_metric_urls()')
    cmd = 'kudu %s list %s -columns=http-addresses -format=json'\
          % (role, kudu_master_rpcs)
    LOG.info('request server list: %s' % cmd)
    status, output = commands.getstatusoutput(cmd)
    host_urls = {}
    if status == 0:
        for entry in ujson.loads(output):
            url = 'http://' + entry['http-addresses'] + '/metrics?compact=1'
            if len(metrics):
                url += '&metrics=' + ','.join(metrics)
            host_urls[get_hostname(entry['http-addresses'])] = url
    g_last_metric_urls_updated_time = cur_time
    return host_urls


def push_to_falcon_agent(origin_data):
    if len(origin_data) == 0 or args.local_stat:
        return
    if args.falcon_url != '':
        data = ujson.dumps(origin_data)
        request = urllib2.Request(args.falcon_url, data)
        response = urllib2.urlopen(request).read()         # TODO async
        LOG.info('Pushed %d data to %s. response=%s' % (len(data), args.falcon_url, response))
    else:
        LOG.info(ujson.dumps(origin_data, indent=2))


def get_origin_metric(url):
    LOG.info('Start to get_origin_metric()')
    request = urllib2.Request(url)
    request.add_header('Accept-encoding', 'gzip')
    try:
        response = urllib2.urlopen(request, timeout=args.get_metrics_timeout)
        if response.info().get('Content-Encoding') == 'gzip':
            buf = StringIO.StringIO(response.read())
            return gzip.GzipFile(fileobj=buf).read()
        else:
            return response.read()
    except Exception as e:
        LOG.error('url: %s, error: %s' % (url, e))
        return {}


# TODO are master's and tserver's metrics are the same?
# TODO not needed to fetch all
def init_metric_types(host_urls):
    global g_metric_types_inited
    if g_metric_types_inited:
        return
    LOG.info('Start to init_metric_types()')
    for host_url in host_urls.values():
        origin_metric = get_origin_metric(host_url + '&include_schema=1')
        for block in ujson.loads(origin_metric):
            for metric in block['metrics']:
                g_metric_type[metric['name']] = metric['type'].upper()
            g_metric_types_inited = True
            return      # init once is ok


def get_value(metric_value):
    if isinstance(metric_value, (int, types.LongType)):
        return metric_value
    if metric_value == 'RUNNING':       # TODO support more status
        return 1
    return 0


def construct_falcon_item(endpoint, metric_name, level, timestamp, metric_value, counter_type, extra_tags=''):
    return {'endpoint': endpoint,
            'metric': metric_name,
            'tags': 'service=kudu,cluster=%s,level=%s,v=3%s'
                    % (args.cluster_name, level, extra_tags),
            'timestamp': timestamp,
            'step': args.metric_period,
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
    for metric in block['metrics']:
        metric_name = metric['name']
        if g_metric_type[metric_name] in ['GAUGE', 'COUNTER']:
            if args.local_stat:
                print('%s %s %s %s %d' % (block['attributes']['table_name'], block['attributes']['partition'], block['id'], metric['name'], metric['value']))

            metric_value = metric['value']
            # host table level metric
            table_name = block['attributes']['table_name']
            if table_name not in tables_metrics.keys():
                tables_metrics[table_name] = {'replica_count': 0}
            if metric_name not in tables_metrics[table_name].keys():
                tables_metrics[table_name][metric_name] = 0
            tables_metrics[table_name][metric_name] += get_value(metric_value)

            # host level metric
            if metric_name in host_metrics.keys():
                host_metrics[metric_name] += get_value(metric_value)
            else:
                host_metrics[metric_name] = get_value(metric_value)
        elif g_metric_type[metric_name] == 'HISTOGRAM':
            for hmetric in HISTOGRAM_METRICS:
                if args.local_stat:
                    print('%s %s %s %s %d' % (block['attributes']['table_name'], block['attributes']['partition'], block['id'], metric['name'], metric[hmetric]))

                if hmetric in metric.keys():
                    # host table level metric, histogram type
                    metric_name_histogram = metric_name + '_' + hmetric
                    table_name = block['attributes']['table_name']
                    if table_name not in tables_metrics.keys():
                        tables_metrics[table_name] = {'replica_count': 0}
                    if table_name not in tables_metrics_histogram.keys():
                        tables_metrics_histogram[table_name] = {}
                    if metric_name_histogram not in tables_metrics_histogram[table_name].keys():
                        # origin data in list, calculate later
                        tables_metrics_histogram[table_name][metric_name_histogram] = []
                    tables_metrics_histogram[table_name][metric_name_histogram].append((metric['total_count'],
                                                                                        metric[hmetric]))

                    # host level metric, histogram type
                    if metric_name_histogram not in host_metrics_histogram.keys():
                        # origin data in list, calculate later
                        host_metrics_histogram[metric_name_histogram] = []
                    host_metrics_histogram[metric_name_histogram].append((metric['total_count'], metric[hmetric]))
        else:
            assert g_metric_type[metric_name] == 'nonono'


def get_host_metrics(hostname, url):
    LOG.info('Start get metrics from [%s]' % url)
    metrics_json_str = get_origin_metric(url)

    host_metrics = {}
    host_metrics_histogram = {}
    tables_metrics = {}
    tables_metrics_histogram = {}
    for block in ujson.loads(metrics_json_str):
        if block['type'] == 'tablet':
            if (len(args.tables) == 0 or                                  # no table filter
                (block['attributes']
                 and block['attributes']['table_name']
                 and block['attributes']['table_name'] in args.tables)):  # table match
                parse_tablet_metrics(block,
                                     tables_metrics,
                                     tables_metrics_histogram,
                                     host_metrics,
                                     host_metrics_histogram)
                # 'replica_count' is an extra metric
                tables_metrics[block['attributes']['table_name']]['replica_count'] += 1

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
    return host_tables_metrics, host_tables_metrics_histogram


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
    LOG.info('Start to get_health_falcon_data()')
    ksck_falcon_data = []
    cmd = 'kudu cluster ksck %s -consensus=false'\
          ' -ksck_format=json_compact -color=never'\
          ' -sections=MASTER_SUMMARIES,TSERVER_SUMMARIES,TABLE_SUMMARIES'\
          ' 2>/dev/null' % (args.kudu_master_rpcs)
    LOG.info('request cluster ksck info: %s' % cmd)
    status, output = commands.getstatusoutput(cmd)
    if status == 0 or status == 256:
        timestamp = int(time.time())
        ksck_info = ujson.loads(output)
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
            for table in ksck_info['table_summaries']:
                ksck_falcon_data.append(
                    construct_falcon_item(endpoint=table['name'],
                                          metric_name='kudu-table-health',
                                          level='table',
                                          timestamp=timestamp,
                                          metric_value=get_table_health_status(table['health']),
                                          counter_type='GAUGE'))
    else:
        LOG.error('failed to fetch ksck info. status: %d, output: %s' % (status, output))

    push_to_falcon_agent(ksck_falcon_data)


def main_loop(args):
    LOG.info('Start to main_loop()')
    host_urls = {}
    global IS_RUNNING
    while IS_RUNNING:
        start = time.time()

        host_urls = update_metric_urls(host_urls, start, args.kudu_master_rpcs, 'tserver', args.metrics)
        init_metric_types(host_urls)
        if len(host_urls) == 0:
            LOG.warn('Detect 0 hosts, try fetch hosts later')
            interruptable_sleep(10)
            continue

        try:
            results = []
            count = len(host_urls)
            if args.local_stat:
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
            for result in results:
                try:
                    host_tables_metrics, host_tables_metrics_histogram = result.get()
                except Exception as e:
                    LOG.error(e)
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

            # table level
            for table, metrics in tables_metrics.iteritems():
                for metric, value in metrics.iteritems():
                    if args.local_stat:
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
                    if args.local_stat:
                        print('%s %s %d' % (metric, table, value))
                    table_falcon_data.append(
                        construct_falcon_item(endpoint=table,
                                              metric_name=metric,
                                              level='table',
                                              timestamp=timestamp,
                                              metric_value=value,
                                              counter_type='GAUGE'))

            push_to_falcon_agent(table_falcon_data)

            if args.local_stat:
                return

            get_health_falcon_data()          # TODO put it into another process
        except Exception as e:
            LOG.error(traceback.format_exc())

        end = time.time()
        used_time = end - start
        left_time = args.metric_period - used_time
        if left_time > 0:
            LOG.info('Sleep for %s seconds' % left_time)
            interruptable_sleep(left_time)
        else:
            LOG.warn('Collect timeout, cost %f secs this time' % used_time)


def rebalance_cluster(args):
    LOG.info('Start to rebalance_cluster()')
    global IS_RUNNING
    while IS_RUNNING:
        if time.strftime("%H:%M", time.localtime()) == args.rebalance_time:
            start = time.time()
            cmd = 'kudu cluster rebalance %s -tables=%s'\
              ' 2>/dev/null' % (args.kudu_master_rpcs, args.rebalance_tables)
            LOG.info('request cluster rebalance info: %s' % cmd)
            status, output = commands.getstatusoutput(cmd)
            if status == 0 or status == 256:
                LOG.info('rebalance output: %s' % (output))
            else:
                LOG.error('failed to perfome rebalance. status: %d, output: %s' % (status, output))
            end = time.time()
            used_time = end - start
            left_time = 3600*24 - used_time
            LOG.info('Sleep for %s seconds' % left_time)
            interruptable_sleep(left_time)


def parse_command_line():
    LOG.info('Start to parse_command_line()')
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='Collect apache kudu cluster metrics.')

    parser.add_argument('--cluster_name', type=str, help='Kudu cluster name',
                        required=True)

    parser.add_argument('--kudu_master_rpcs', type=str, help='Kudu masters rpc addresses',
                        required=True)

    parser.add_argument('--metric_period', type=int, help='The period to fetch metrics in seconds',
                        required=False, default=60)

    parser.add_argument('--local_stat', type=bool, help='Whether to calculate statistics on local host',
                        required=False, default=False)

    parser.add_argument('--get_metrics_timeout', type=int, help='The timeout to fetch metrics from server in seconds',
                        required=False, default=10)

    parser.add_argument('--falcon_url', type=str, help='The falcon url to push metrics to',
                        required=False, default='http://127.0.0.1:1988/v1/push')

    parser.add_argument('--tables', type=str, help='Table names to collect metrics',
                        required=False, default='')

    parser.add_argument('--metrics', type=str, help='Metrics to collect',
                        required=False, default='')
                        
    parser.add_argument('--rebalance_cluster', type=str, help='Whether to rebalance cluster regularly',
                        required=False, default=False)

    parser.add_argument('--rebalance_tables', type=str, help='Table names to rebalance',
                        required=False, default='')

    parser.add_argument('--rebalance_time', type=str, help='Time to perform cluster rebalance, format is HH:MM (e.g.00:00)',
                        required=False, default='00:00')

    args = parser.parse_args()
    args.cluster_name = args.cluster_name.replace(u'\ufeff', '')
    args.tables = set(filter(lambda x: x != '', args.tables.replace(u'\ufeff', '').split(',')))
    args.metrics = filter(lambda x: x != '', args.metrics.replace(u'\ufeff', '').split(','))
    return args


def term_sig_handler(signum, frame):
    LOG.warn('catched singal: %d' % signum)
    global IS_RUNNING
    IS_RUNNING = False


def main():
    signal.signal(signal.SIGTERM, term_sig_handler)
    signal.signal(signal.SIGINT, term_sig_handler)
    global args
    args = parse_command_line()
    try:
        if args.rebalance_cluster:
            hour,minute = args.rebalance_time.split(':')
            if int(hour) in range(0,24) and int(minute) in range(0,60):
                # perform cluster rebalance in another thread
                t = threading.Thread(target=rebalance_cluster, args=(args,))
                t.start()
        main_loop(args)
    except KeyboardInterrupt:
        LOG.warn('Collector terminated.')
        global IS_RUNNING
        IS_RUNNING = False


if __name__ == '__main__':
    main()
