#! /usr/bin/env python
# coding=utf-8

import commands
import heapq
import json
import os
import re
import sys
import time
import types


def printc(msg):
    print('\033[1;31;40m WARNING: %s \033[0m' % msg)


def printtsr(table, size, reason):
    printc('table: ' + table + (', size: %fG' % (size/(1 << 30)) + ', reason: ') + reason)


if len(sys.argv) != 2:
    printc('Usage: $0 <cluster_name|all>')
    exit(1)

ignore_db_set = ('system')
test_org_dict = {'admin': '',
                 'default': '',
                 'resource_manager': 'CL29279',
                 'test_org': 'CL18599',
                 'yarn_admin': 'CL12641',
                 'computing': 'CL1219',
                }


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


def add_org_size(db_table, org, size, org_size_desc):
    if len(org) == 0:
        printtsr(db_table, size, 'Lack org ID')
        return False

    if org in test_org_dict:
        user = test_org_dict[org]
        if len(user) == 0:
            user = org
        printtsr(db_table, size, 'Test table(user: %s)' % user)
        return False

    if org not in org_size_desc.keys():
        org_size_desc[org] = {}
        org_size_desc[org]['size'] = 0
        org_size_desc[org]['desc'] = TopkHeap(10)
    org_size_desc[org]['size'] += size
    org_size_desc[org]['desc'].Push({'size':size, 'table': db_table})
    return True


def get_org_size_desc_from_olap_1(master_rpcs, db_table_size_dict, known_db_org):
    cmd = '${KUDU_HOME}kudu table scan %s system.db -show_value=true -columns=name,org 2>&1 |' \
          ' grep "(string name=\\\""' % master_rpcs
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        printc('Scan table error')
        return {}

    db_org_dict = {}
    for line in output.splitlines():
        match_obj = re.search(r'string name="(.*)", string org="(.*)"', line, re.M | re.I)
        if match_obj:
            db = match_obj.group(1)
            org = match_obj.group(2)
            db_org_dict[db] = org
        else:
            printc('Format error: %s' % line)
            exit(1)

    total_ignored_size = 0.0
    org_size_desc = {}
    for db_table, size in db_table_size_dict.items():
        if len(db_table.split('.')) != 2:
            total_ignored_size += size
            printtsr(db_table, size, 'Lack db')
            continue

        db, table = db_table.split('.')
        if db in ignore_db_set:
            total_ignored_size += size
            printtsr(db_table, size, 'System table')
            continue

        org = ''
        if db in known_db_org.keys():
            org = known_db_org[db]
        elif db in db_org_dict.keys():
            org = db_org_dict[db]
        else:
            total_ignored_size += size
            printtsr(db_table, size, 'Lack org ID')
            continue

        if not add_org_size(db_table, org, size, org_size_desc):
            total_ignored_size += size
            continue

    printtsr('TOTAL', total_ignored_size, 'Total ignored size')
    return org_size_desc


def get_org_size_desc_from_olap_2(master_rpcs, db_table_size_dict, known_db_org):
    cmd = '${KUDU_HOME}kudu table scan %s system.kudu_table_owners -show_value=true' \
          ' -columns=name,db,org 2>&1 | grep "(string name=\\\""' % master_rpcs
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        printc('Scan table error')

    db_org_dict = {}
    for line in output.splitlines():
        match_obj = re.search(r'string name="(.*)", string db="(.*)", string org="(.*)"', line, re.M | re.I)
        if match_obj:
            table = match_obj.group(1)
            db = match_obj.group(2)
            org = match_obj.group(3)
            db_org_dict[db] = org
        else:
            printc('Format error: %s' % line)
            exit(1)

    total_ignored_size = 0.0
    org_size_desc = {}
    for db_table, size in db_table_size_dict.items():
        if len(db_table.split('.')) != 2:
            total_ignored_size += size
            printtsr(db_table, size, 'Lack db')
            continue

        db, table = db_table.split('.')
        if db in ignore_db_set:
            total_ignored_size += size
            printtsr(db_table, size, 'System table')
            continue

        org = ''
        if db in known_db_org.keys():
            org = known_db_org[db]
        elif db in db_org_dict.keys():
            org = db_org_dict[db]
        else:
            total_ignored_size += size
            printtsr(db, size, 'Lack org ID')
            continue

        if not add_org_size(db_table, org, size, org_size_desc):
            total_ignored_size += size
            continue

    printtsr('TOTAL', total_ignored_size, 'Total ignored size')
    return org_size_desc


def stat_one_cluster(cluster_name, cluster_info):
    print('Start to process cluster %s' % cluster_name)
    if cluster_info['charge_type'] == 'public_share':
        printc('public_share cluster %s' % cluster_name)
        return {}

    cmd = 'python kudu_metrics_collector_for_falcon.py --cluster_name=%s' \
          ' --local_stat --falcon_url="" --metrics=on_disk_size |' \
          ' egrep "^on_disk_size " | sort | awk \'{print $2, $3}\'' % cluster_info['master_rpcs']
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        printc('Table stat error')
        return {}

    db_table_size_dict = {}
    for line in output.splitlines():
        key_value = line.split(' ')
        assert(len(key_value) == 2)
        db_table_size_dict[key_value[0]] = float(key_value[1])
    known_db_org = {}
    if 'special_db_org' in cluster_info.keys():
        known_db_org = cluster_info['special_db_org']
    org_size_desc = {}
    olap_version = cluster_info['olap_version']
    if olap_version == 1:
        org_size_desc = get_org_size_desc_from_olap_1(cluster_info['master_rpcs'], db_table_size_dict, known_db_org)
    elif olap_version == 2:
        org_size_desc = get_org_size_desc_from_olap_2(cluster_info['master_rpcs'], db_table_size_dict, known_db_org)
    else:
        printc('olap version is error: %d' % olap_version)
        exit(1)

    for org, size_desc in org_size_desc.iteritems():
        print('%s, kudu, %s, %s, %s, %s, %s, %s, \'{"storage_bytes":%d}\', \'%s\'' % (
                                              time.strftime("%Y-%m-%d", time.localtime()),
                                              cluster_info['region'],
                                              cluster_info['charge_type'],
                                              cluster_info['instance'],
                                              cluster_name,
                                              'org' if org.find('CL') != -1 else 'kerberos',
                                              org,
                                              size_desc['size'],
                                              json.dumps(size_desc['desc'].TopK())))


cluster_name = sys.argv[1]
clusters_info = json.loads(open(os.environ.get('KUDU_HOME') + '/cluster_info.json').read())
if cluster_name == 'all':
    for cluster_name, cluster_info in clusters_info.iteritems():
        stat_one_cluster(cluster_name, cluster_info)
else:
    if cluster_name not in clusters_info.keys():
        printc('Cluster %s not found' % cluster_name)
        exit(1)
    stat_one_cluster(cluster_name, clusters_info[cluster_name])
