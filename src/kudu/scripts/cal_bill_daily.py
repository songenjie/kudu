#! /usr/bin/env python
# coding=utf-8

import commands
import heapq
import logging
from logging.handlers import RotatingFileHandler
import json
import os
import re
import sys
import time
import kudu_utils


ignore_db_set = ('system', 'lcsbinlog', 'default', 'zhangxu_test_kudu')


def printtsr(level, table, size, reason):
    kudu_utils.LOG.log(level, 'table: ' + table + (', size: %fG' % (size/(1 << 30)) + ', reason: ') + reason)


class TopkHeap(object):
    def __init__(self, k):
        self.k = k
        self.data = []

    def Push(self, elem):
        if len(self.data) < self.k:
            heapq.heappush(self.data, elem)
        else:
            topk_small = self.data[0]
            if elem['size'] > topk_small['size']:
                heapq.heapreplace(self.data, elem)

    def TopK(self):
        return {x['table']:x['size'] for x in reversed([heapq.heappop(self.data) for x in xrange(len(self.data))])}


def add_org_size(dbtable, org, size, org_size_desc):
    if len(org) == 0:
        printtsr(logging.WARNING, dbtable, size, 'Org name is empty')
        return False

    if org not in org_size_desc.keys():
        org_size_desc[org] = {}
        org_size_desc[org]['size'] = 0
        org_size_desc[org]['desc'] = TopkHeap(10)
    org_size_desc[org]['size'] += size
    org_size_desc[org]['desc'].Push({'size':size, 'table': dbtable})
    return True


def get_org_size_desc_from_olap(master_rpcs, dbtable_size_dict, known_db_org_dict):
    db_org_dict = {}
    meta_table = 'system.kudu_table_owners'
    cmd = '%s/kudu table scan %s %s -show_value=true' \
          ' -columns=name,db,org 2>&1 | grep "(string name=\\\""' % (kudu_utils.script_path(), master_rpcs, meta_table)
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        kudu_utils.LOG.error('Scan table %s error, status %d, output \n%s' \
                  % (meta_table, status, output))
    else:
        for line in output.splitlines():
            match_obj = re.search(r'string name="(.*)", string db="(.*)", string org="(.*)"', line, re.M | re.I)
            if match_obj:
                db = match_obj.group(2)
                org = match_obj.group(3)
                db_org_dict[db] = org
            else:
                kudu_utils.LOG.error('Table %s value format error, line\n%s' % (meta_table, line))

    total_ignored_size = 0.0
    org_size_desc = {}
    for dbtable, size in dbtable_size_dict.iteritems():
        db_table_list = dbtable.split('.')
        if len(db_table_list) != 2:
            total_ignored_size += size
            printtsr(logging.WARNING, dbtable, size, 'Lack db')
            continue

        db, table = db_table_list[0], db_table_list[1]
        if db in ignore_db_set:
            total_ignored_size += size
            printtsr(logging.INFO, dbtable, size, 'Ignored table')
            continue

        org = ''
        if db in known_db_org_dict.keys():
            # 'org' from config file
            org = known_db_org_dict[db]
        elif db in db_org_dict.keys():
            # 'org' from syatem table
            org = db_org_dict[db]
        else:
            total_ignored_size += size
            printtsr(logging.WARNING, db, size, 'Lack org ID')
            continue

        if not add_org_size(dbtable, org, size, org_size_desc):
            total_ignored_size += size
            continue

    printtsr(logging.WARNING, 'TOTAL', total_ignored_size, 'Total ignored size')
    return org_size_desc


def stat_one_cluster(data_path, cluster_name, cluster_info):
    kudu_utils.LOG.info('Start to process cluster %s' % cluster_name)
    if cluster_info['charge_type'] == 'public_share':
        kudu_utils.LOG.warning('Ignore public_share cluster %s' % cluster_name)
        return ""

    # Output: db.table size
    cmd = '%s/kudu_metrics_collector_for_falcon.py --cluster_name=%s' \
          ' --local_stat --falcon_url="" --metrics=on_disk_size |' \
          ' egrep "^on_disk_size " | sort | awk \'{print $2, $3}\'' % (kudu_utils.g_script_path, cluster_info['master_rpcs'])
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        kudu_utils.LOG.fatal('Table stat error')
        return ""

    dbtable_size_dict = {}
    for dbtable_size_str in output.splitlines():
        dbtable_size_list = dbtable_size_str.split(' ')
        assert(len(dbtable_size_list) == 2)
        dbtable_size_dict[dbtable_size_list[0]] = float(dbtable_size_list[1])
    known_db_org_dict = {}
    if 'special_db_org' in cluster_info.keys():
        known_db_org_dict = cluster_info['special_db_org']
    org_size_desc = {}
    org_size_desc = get_org_size_desc_from_olap(cluster_info['master_rpcs'], dbtable_size_dict, known_db_org_dict)

    results = []
    date = time.strftime('%Y-%m-%d', time.localtime())
    for org, size_desc in org_size_desc.iteritems():
        result = {}
        result['date'] = date
        result['service'] = 'kudu'
        result['region'] = cluster_info['region']
        result['charge_type'] = cluster_info['charge_type']
        result['instance'] = cluster_info['instance']
        result['cluster'] = cluster_name
        result['user_type'] = 'org' if org.find('CL') != -1 else 'kerberos'
        result['user_id'] = org
        result['usage'] = size_desc['size']
        result['charge_object'] = size_desc['desc'].TopK()
        results.append(result)
    filename = data_path + '/' + date + '_' + cluster_name
    with open(filename, 'w') as f:
        json.dump(results, f)
        f.close()
        kudu_utils.LOG.info('Write to file finished')
        return filename


data_path = kudu_utils.prepare_pricing_data_path(False)
clusters_info = json.loads(open(kudu_utils.g_script_path + '/cluster_info.json').read())

filenames = []
if len(sys.argv) == 1:
    # Calculate all clusters
    for cluster_name, cluster_info in clusters_info.iteritems():
        filename = stat_one_cluster(data_path, cluster_name, cluster_info)
        if len(filename) != 0:
            filenames.append(filename)
elif len(sys.argv) == 2:
    # Calculate specified cluster
    cluster_name = sys.argv[1]
    if cluster_name not in clusters_info.keys():
        kudu_utils.LOG.fatal('Cluster %s not found' % cluster_name)
        exit(1)
    filename = stat_one_cluster(data_path, cluster_name, clusters_info[cluster_name])
    if len(filename) != 0:
        filenames.append(filename)
else:
    kudu_utils.LOG.fatal('Usage: $0 [cluster_name]')
    exit(1)

kudu_utils.push_file_to_repo(filenames)
