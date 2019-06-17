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


g_ignore_db_set = ('system', 'lcsbinlog', 'default', 'zhangxu_test_kudu')
g_month_path = kudu_utils.prepare_pricing_month_path()
g_month_data_path = g_month_path + 'data/'
g_usage_result_filename = g_month_path + 'tbl_usage.csv'
g_clusters_info = json.loads(open(kudu_utils.g_script_path + '/cluster_info.json').read())
g_commit_filenames = list()
g_commit_filenames.append(g_usage_result_filename)


def printtsr(level, table, size, reason):
    kudu_utils.LOG.log(level, 'table: ' + table + (', size: %fG' % (size/(1 << 30)) + ', reason: ') + reason)


class TopKHeap(object):
    def __init__(self, k):
        self.k = k
        self.data = []

    def push(self, elem):
        if len(self.data) < self.k:
            heapq.heappush(self.data, elem)
        else:
            top_k_small = self.data[0]
            if elem['size'] > top_k_small['size']:
                heapq.heapreplace(self.data, elem)

    def top_k(self):
        return {x['table']: x['size'] for x in reversed([heapq.heappop(self.data) for _ in xrange(len(self.data))])}


def add_org_size(dbtable, org, size, org_size_desc):
    if len(org) == 0:
        printtsr(logging.WARNING, dbtable, size, 'Org name is empty')
        return False

    if org not in org_size_desc.keys():
        org_size_desc[org] = {}
        org_size_desc[org]['size'] = 0
        org_size_desc[org]['desc'] = TopKHeap(10)
    org_size_desc[org]['size'] += size
    org_size_desc[org]['desc'].push({'size': size, 'table': dbtable})
    return True


def get_org_size_desc_from_olap(master_rpcs, dbtable_size_dict, known_db_org_dict):
    db_org_dict = {}
    meta_table = 'system.kudu_table_owners'
    cmd = '%s/kudu table scan %s %s -show_value=true' \
          ' -columns=name,db,org 2>&1 | grep "(string name=\\\""'\
          % (kudu_utils.script_path(), master_rpcs, meta_table)
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        kudu_utils.LOG.error('Scan table %s error, status %d, output \n%s' % (meta_table, status, output))
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
        if db in g_ignore_db_set:
            total_ignored_size += size
            printtsr(logging.INFO, dbtable, size, 'Ignored table')
            continue

        if db in known_db_org_dict.keys():
            # 'org' from config file
            org = known_db_org_dict[db]
        elif db in db_org_dict.keys():
            # 'org' from system table
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


def get_cluster_stat_filename(date, cluster_name):
    return g_month_data_path + date + '_' + cluster_name


def collect_origin_usage_for_cluster(cluster_name, cluster_info):
    kudu_utils.LOG.info('Start to collect usage info for cluster %s' % cluster_name)
    # Output: db.table size
    cmd = '%s/kudu_metrics_collector_for_falcon.py --cluster_name=%s' \
          ' --local_stat --falcon_url="" --metrics=on_disk_size |' \
          ' egrep "^on_disk_size " | sort | awk \'{print $2, $3}\''\
          % (kudu_utils.g_script_path, cluster_info['master_rpcs'])
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        kudu_utils.LOG.fatal('Table stat error')
        return

    dbtable_size_dict = {}
    for dbtable_size_str in output.splitlines():
        dbtable_size_list = dbtable_size_str.split(' ')
        assert(len(dbtable_size_list) == 2)
        dbtable_size_dict[dbtable_size_list[0]] = float(dbtable_size_list[1])
    known_db_org_dict = {}
    if 'special_db_org' in cluster_info.keys():
        known_db_org_dict = cluster_info['special_db_org']
    org_size_desc = get_org_size_desc_from_olap(cluster_info['master_rpcs'], dbtable_size_dict, known_db_org_dict)

    results = []
    date = time.strftime('%Y-%m-%d', time.localtime())
    for org, size_desc in org_size_desc.iteritems():
        result = dict()
        result['date'] = date
        result['service'] = 'kudu'
        result['region'] = cluster_info['region']
        result['charge_type'] = cluster_info['charge_type']
        result['instance'] = cluster_info['instance']
        result['cluster'] = cluster_name
        result['user_type'] = 'org' if org.find('CL') != -1 else 'kerberos'
        result['user_id'] = org
        result['usage'] = size_desc['size']
        result['charge_object'] = size_desc['desc'].top_k()
        results.append(result)
    origin_usage_filename = get_cluster_stat_filename(date, cluster_name)
    with open(origin_usage_filename, 'w') as origin_usage_file:
        json.dump(results, origin_usage_file)
        origin_usage_file.close()

        g_commit_filenames.append(origin_usage_filename)


def get_cluster_info(cluster_name):
    if cluster_name not in g_clusters_info.keys():
        kudu_utils.LOG.fatal('Cluster %s not found' % cluster_name)
        return None

    cluster_info = g_clusters_info[cluster_name]
    if cluster_info['charge_type'] == 'public_share':
        kudu_utils.LOG.warning('Ignore public_share cluster %s' % cluster_name)
        return None

    return cluster_info


def collect_origin_usage_for_clusters(cluster_name_list):
    for cluster_name in cluster_name_list:
        cluster_info = get_cluster_info(cluster_name)
        if not cluster_info:
            continue
        collect_origin_usage_for_cluster(cluster_name, cluster_info)


def calc_usage_result(origin_usage_filename, usage_result_file):
    kudu_utils.LOG.info('Start to process daily statistics file %s' % origin_usage_filename)
    if not os.path.exists(origin_usage_filename):
        kudu_utils.LOG.error('File not exist')
        return
    with open(origin_usage_filename, 'r') as origin_usage_file:
        users_usage = json.load(origin_usage_file)
        for user_usage in users_usage:
            usage_result_file.write('%s, %s, %s, %s, %s, %s, %s, %s, \'{"storage_bytes":%d}\', \'%s\'\n'
                                    % (user_usage['date'],
                                       user_usage['service'],
                                       user_usage['region'],
                                       user_usage['charge_type'],
                                       user_usage['instance'],
                                       user_usage['cluster'],
                                       user_usage['user_type'],
                                       user_usage['user_id'],
                                       user_usage['usage'],
                                       json.dumps(user_usage['charge_object'])))
        origin_usage_file.close()
    kudu_utils.LOG.info('Write to file finished')


def calc_usage_result_for_cluster(cluster_name, date):
    with open(g_usage_result_filename, 'a') as usage_result_file:
        origin_usage_filename = get_cluster_stat_filename(date, cluster_name)
        calc_usage_result(origin_usage_filename, usage_result_file)
        usage_result_file.close()


def calc_usage_result_for_clusters(cluster_name_list, date_list):
    for date in date_list:
        for cluster_name in cluster_name_list:
            cluster_info = get_cluster_info(cluster_name)
            if not cluster_info:
                continue
            calc_usage_result_for_cluster(cluster_name, date)


def create_usage_result_file_if_not_exist():
    if not os.path.exists(g_usage_result_filename):
        # Create new file
        with open(g_usage_result_filename, 'w') as usage_result_file:
            # Write header
            usage_result_file.write('date, service, region, charge_type, instance, '
                                    'cluster, user_type, user_id, usage, charge_object\n')
            usage_result_file.close()


def main(argv=None):
    if not os.path.exists(kudu_utils.g_git_repo_dir + '/.git'):
        kudu_utils.LOG.fatal('You must set `g_git_repo_dir` to a valid directory contains `.git`')
        return

    if argv is None:
        argv = sys.argv

    cluster_name_list = []
    if len(argv) == 1:
        # Calculate all clusters
        cluster_name_list = list(g_clusters_info.iterkeys())
    elif len(argv) == 2:
        # Calculate specified cluster
        cluster_name_list.append(argv[1])
    else:
        kudu_utils.LOG.fatal('Usage: $0 [cluster_name]')
        return

    collect_origin_usage_for_clusters(cluster_name_list)

    create_usage_result_file_if_not_exist()

    # date_list = kudu_utils.get_date_list('2019-06-01', kudu_utils.get_date())
    date_list = [kudu_utils.get_date()]
    calc_usage_result_for_clusters(cluster_name_list, date_list)

    kudu_utils.push_file_to_repo(g_commit_filenames)


if __name__ == "__main__":
    main()
