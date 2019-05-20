#! /usr/bin/env python
# coding=utf-8

import logging
from logging.handlers import RotatingFileHandler
import json
import os
import re
import time
import kudu_utils


def stat_one_cluster(result_file, data_path, cluster_name, cluster_info):
    kudu_utils.LOG.info('Start to process cluster %s' % cluster_name)
    month = time.strftime('%Y-%m', time.localtime())
    monthly_user_usage = {}
    for filename in os.listdir(data_path):
        match_obj = re.search(r'%s-[0-9]{2}_%s' % (month, cluster_name), filename, re.M | re.I)
        if match_obj:
            with open(data_path + '/' + filename, 'r') as f:
                usage_list = json.load(f)
                kudu_utils.LOG.info('Start to process daily statistics file %s' % filename)
                for daily_user_usage in usage_list:
                    if 'user_id' not in daily_user_usage.keys():
                        kudu_utils.LOG.error('Lack `user_id` in %s' % filename)
                        continue

                    user_id = daily_user_usage['user_id']
                    if user_id not in monthly_user_usage.keys() \
                      or monthly_user_usage[user_id]['usage'] < daily_user_usage['usage']:
                        monthly_user_usage[user_id] = daily_user_usage
    for result in monthly_user_usage.itervalues():
        result_file.write('%s, %s, %s, %s, %s, %s, %s, %s, %d, %s\n' \
                % (result['date'],
                   result['service'],
                   result['region'],
                   result['charge_type'],
                   result['instance'],
                   result['cluster'],
                   result['user_type'],
                   result['user_id'],
                   result['usage'],
                   json.dumps(result['charge_object'])))
    kudu_utils.LOG.info('Write to file finished')


data_path = kudu_utils.prepare_pricing_data_path(True)
filename = data_path + '/tbl_usage.csv'
with open(filename, 'w') as f:
    # Write header
    f.write('date, service, region, charge_type, instance, cluster, user_type, user_id, usage, charge_object\n')

    clusters_info = json.loads(open(kudu_utils.g_script_path + '/cluster_info.json').read())
    for cluster_name, cluster_info in clusters_info.iteritems():
        stat_one_cluster(f, data_path, cluster_name, cluster_info)
    f.close()

    kudu_utils.push_file_to_repo([filename])
