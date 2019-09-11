#!/bin/bash

PID=$$
BASE_DIR="$( cd "$( dirname "$0" )" && pwd )"
KUDU=${KUDU_HOME}/kudu
COLLECTOR=${KUDU_HOME}/kudu_collector
if [[ ! -f ${KUDU} || ! -f ${COLLECTOR} ]]; then
  echo "ERROR: ${KUDU} or ${COLLECTOR} not found"
  exit 1
fi

if [[ $# -ne 3 ]]
then
  echo "This tool is for update falcon screen for specified kudu cluster."
  echo "USAGE: $0 <cluster_name> <masters> <table_count>"
  exit 1
fi

CLUSTER=$1
MASTERS=$2
TABLE_COUNT=$3

echo "UID: ${UID}"
echo "PID: ${PID}"
echo "cluster: ${CLUSTER}"
echo "masters: ${MASTERS}"
echo "top n table: ${TABLE_COUNT}"
echo "Start time: `date`"
ALL_START_TIME=$((`date +%s`))
echo

# get master list
${KUDU} master list @${CLUSTER} -format=space | awk -F' |:' '{print $2}' | sort -n &>/tmp/${UID}.${PID}.kudu.master.list
if [[ $? -ne 0 ]]; then
    echo "`kudu master list @${CLUSTER} -format=space` failed"
    exit $?
fi

MASTER_COUNT=`cat /tmp/${UID}.${PID}.kudu.master.list | wc -l`
if [[ ${MASTER_COUNT} -eq 0 ]]; then
    echo "ERROR: master list is empty, please check the cluster ${CLUSTER}"
    exit -1
fi

# get tserver list
${KUDU} tserver list @${CLUSTER} -format=space | awk -F' |:' '{print $2}' | sort -n &>/tmp/${UID}.${PID}.kudu.tserver.list
if [[ $? -ne 0 ]]; then
    echo "`kudu tserver list @${CLUSTER} -format=space` failed"
    exit $?
fi

TSERVER_COUNT=`cat /tmp/${UID}.${PID}.kudu.tserver.list | wc -l`
if [[ ${TSERVER_COUNT} -eq 0 ]]; then
    echo "ERROR: tserver list is empty, please check the cluster ${CLUSTER}"
    exit 1
fi

# get table list
${COLLECTOR} -collector_cluster_name=${CLUSTER} -collector_report_method=local -collector_metrics=bytes_flushed,on_disk_size,scanner_bytes_returned -log_dir=./log > /tmp/${UID}.${PID}.kudu.metric_table_value
if [[ $? -ne 0 ]]; then
    echo "ERROR: ${COLLECTOR} execute failed"
    exit 1
fi

cat /tmp/${UID}.${PID}.kudu.metric_table_value | egrep "^table bytes_flushed " | sort -rnk4 | head -n ${TABLE_COUNT} | awk '{print $3}' > /tmp/${UID}.${PID}.kudu.top.bytes_flushed
cat /tmp/${UID}.${PID}.kudu.metric_table_value | egrep "^table on_disk_size " | sort -rnk4 | head -n ${TABLE_COUNT} | awk '{print $3}' > /tmp/${UID}.${PID}.kudu.top.on_disk_size
cat /tmp/${UID}.${PID}.kudu.metric_table_value | egrep "^table scanner_bytes_returned " | sort -rnk4 | head -n ${TABLE_COUNT} | awk '{print $3}' > /tmp/${UID}.${PID}.kudu.top.scanner_bytes_returned
cat /tmp/${UID}.${PID}.kudu.top.* | sort -n | uniq > /tmp/${UID}.${PID}.kudu.table.list
echo "total `wc -l /tmp/${UID}.${PID}.kudu.table.list | awk '{print $1}'` tables to monitor"
echo -e "\033[32m Please set the following one line to the kudu collector's \`tables\` argument manually\033[0m"
awk BEGIN{RS=EOF}'{gsub(/\n/,",");print}' /tmp/${UID}.${PID}.kudu.table.list
echo ""

python ${BASE_DIR}/falcon_screen.py ${CLUSTER} ${BASE_DIR}/falcon_screen.json /tmp/${UID}.${PID}.kudu.master.list /tmp/${UID}.${PID}.kudu.tserver.list /tmp/${UID}.${PID}.kudu.table.list
if [[ $? -ne 0 ]]; then
    echo "ERROR: falcon screen operate failed"
    exit 1
fi

echo
echo "Finish time: `date`"
ALL_FINISH_TIME=$((`date +%s`))
echo "Falcon screen operate done, elapsed time is $((ALL_FINISH_TIME - ALL_START_TIME)) seconds."

rm -f /tmp/${UID}.${PID}.kudu.* &>/dev/null
