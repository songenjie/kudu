#!/bin/bash

master_count=3
tserver_count=3
bin_path=./
# In release package, 'kudu-master' and 'kudu-tserver' are renamed to 'kudu_master' and 'kudu_tablet_server'
#master_bin=kudu-master
#tserver_bin=kudu-tserver
master_bin=kudu_master
tserver_bin=kudu_tablet_server
deploy_path=${HOME}/kudu_test/
local_ip=0.0.0.0
base_m_rpc_port=17050
base_m_webserver_port=18050
base_t_rpc_port=7050
base_t_webserver_port=8050

if [[ ! -f ${bin_path}${master_bin} ]]; then
  echo "ERROR: ${master_bin} not found in ${bin_path}"
  exit 1
fi

if [[ ! -f ${bin_path}${tserver_bin} ]]; then
  echo "ERROR: ${tserver_bin} not found in ${bin_path}"
  exit 1
fi

ps -ef | grep ${deploy_path} | grep kudu | grep -v grep | awk '{print $2}' | xargs kill

for i in `seq 1 ${master_count}`
do
  mkdir -p ${deploy_path}m${i}/wal
  mkdir -p ${deploy_path}m${i}/log

  rm ${deploy_path}m${i}/mlog -f
  rm ${deploy_path}m${i}/log/* -rf
  rm ${deploy_path}m${i}/wal/* -rf

  nohup ${bin_path}${master_bin} --fs_wal_dir=${deploy_path}m${i}/wal --log_dir=${deploy_path}m${i}/log --rpc_bind_addresses=${local_ip}:$[${base_m_rpc_port}+$i] --webserver_port=$[${base_m_webserver_port}+$i] --master_addresses=${local_ip}:$[${base_m_rpc_port}+1],${local_ip}:$[${base_m_rpc_port}+2],${local_ip}:$[${base_m_rpc_port}+3] > ${deploy_path}m${i}/mlog 2>&1 &
done

for i in `seq 1 ${tserver_count}`
do
  mkdir -p ${deploy_path}t${i}/wal
  mkdir -p ${deploy_path}t${i}/log
  mkdir -p ${deploy_path}t${i}/data
  
  rm ${deploy_path}t${i}/tlog -f
  rm ${deploy_path}t${i}/log/* -rf
  rm ${deploy_path}t${i}/wal/* -rf
  rm ${deploy_path}t${i}/data/* -rf

  nohup ${bin_path}${tserver_bin} --fs_data_dirs=${deploy_path}t${i}/data --fs_wal_dir=${deploy_path}t${i}/wal --log_dir=${deploy_path}t${i}/log --rpc_bind_addresses=${local_ip}:$[${base_t_rpc_port}+$i] --webserver_port=$[${base_t_webserver_port}+$i] --tserver_master_addrs=${local_ip}:$[${base_m_rpc_port}+1],${local_ip}:$[${base_m_rpc_port}+2],${local_ip}:$[${base_m_rpc_port}+3] > ${deploy_path}t${i}/tlog 2>&1 &
done


ps -ef | grep ${deploy_path} | grep kudu | grep -v grep
