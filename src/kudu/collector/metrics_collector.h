// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#pragma once

#include <cstdint>
#include <list>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gtest/gtest_prod.h>
#include <rapidjson/document.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/status.h"

namespace kudu {
class JsonReader;
class Thread;
class ThreadPool;

namespace collector {
struct ItemBase;
}  // namespace collector
}  // namespace kudu

namespace kudu {

namespace collector {

class NodesChecker;
class ReporterBase;

class MetricsCollector : public RefCounted<MetricsCollector> {
 public:
  MetricsCollector(scoped_refptr<NodesChecker> nodes_checker,
                   scoped_refptr<ReporterBase> reporter);
  ~MetricsCollector();

  Status Init();
  Status Start();
  void Shutdown();

  std::string ToString() const;

 private:
  friend class RefCounted<MetricsCollector>;

  FRIEND_TEST(TestMetricsCollector, TestConvertStateToInt);
  FRIEND_TEST(TestMetricsCollector, TestGetHistValue);
  FRIEND_TEST(TestMetricsCollector, TestMergeToTableLevelMetrics);
  FRIEND_TEST(TestMetricsCollector, TestMergeToClusterLevelMetrics);
  FRIEND_TEST(TestMetricsCollector, TestParseMetrics);
  FRIEND_TEST(TestMetricsCollector, TestParseTypesOfMetrics);
  FRIEND_TEST(TestMetricsCollector, TestInitMetrics);
  FRIEND_TEST(TestMetricsCollector, TestInitFilters);
  FRIEND_TEST(TestMetricsCollector, TestInitMetricsUrlParameters);
  FRIEND_TEST(TestMetricsCollector, TestInitClusterLevelMetrics);

  // Metric name --> value, metric is in type of GAUGE or COUNTER.
  typedef std::unordered_map<std::string, int64_t> Metrics;
  // Table name --> metric name-value pairs.
  typedef std::unordered_map<std::string, Metrics> TablesMetrics;

  // Simple struct to collect histogram metrics.
  struct SimpleHistogram {
    // 'total_count' value in histogram metric.
    int64_t count;
    // 'percentile_xxx" value in histogram metric, percentile_xxx is specified
    // by kRegisterPercentiles.
    double value;
    SimpleHistogram(int64_t c, double v) : count(c), value(v) {
    }
    inline bool operator==(const SimpleHistogram& rhs) const {
      return count == rhs.count && value == rhs.value;
    }
  };
  // Metric name --> SimpleHistogram, metric is in type of HISTOGRAM.
  typedef std::unordered_map<std::string, std::vector<SimpleHistogram>> HistMetrics;
  // Table name --> metric name-struct pairs.
  typedef std::unordered_map<std::string, HistMetrics> TablesHistMetrics;

  // Metric name --> type, where type is in 'COUNTER', 'GAUGE' and 'HISTOGRAM'.
  typedef std::unordered_map<std::string, std::string> MetricTypes;

  Status InitMetrics();
  enum class NodeType {
    kMaster = 0,
    kTServer = 1,
  };
  Status InitMetricsFromNode(NodeType node_type, MetricTypes* metric_types) const;
  static Status ExtractMetricTypes(const JsonReader& r,
                                   const rapidjson::Value* entity,
                                   MetricTypes* metric_types);
  Status InitFilters();
  Status InitMetricsUrlParameters();
  Status InitHostTableLevelMetrics();
  Status InitClusterLevelMetrics();

  Status StartMetricCollectorThread();
  void MetricCollectorThread();
  Status CollectAndReportMasterMetrics();
  Status CollectAndReportTServerMetrics();

  Status UpdateThreadPool(int32_t thread_count);

  Status CollectAndReportHostLevelMetrics(NodeType node_type,
                                          const std::string& url,
                                          TablesMetrics* metrics_by_table_name,
                                          TablesHistMetrics* hist_metrics_by_table_name);

  static Status MergeToTableLevelMetrics(
      const std::vector<TablesMetrics>& hosts_metrics_by_table_name,
      const std::vector<TablesHistMetrics>& hosts_hist_metrics_by_table_name,
      TablesMetrics* metrics_by_table_name,
      TablesHistMetrics* hist_metrics_by_table_name);
  static Status MergeToClusterLevelMetrics(const TablesMetrics& metrics_by_table_name,
                                           const TablesHistMetrics& hist_metrics_by_table_name,
                                           Metrics* cluster_metrics);

  // Report metrics to third-party monitor system.
  void CollectMetrics(const std::string& endpoint,
                      const Metrics& metrics,
                      const std::string& level,
                      uint64_t timestamp,
                      const std::string& extra_tags,
                      std::list<scoped_refptr<ItemBase>>* items);
  void CollectMetrics(const std::string& endpoint,
                      const HistMetrics& metrics,
                      const std::string& level,
                      uint64_t timestamp,
                      const std::string& extra_tags,
                      std::list<scoped_refptr<ItemBase>>* items);

  Status ReportHostTableLevelMetrics(const std::string& host_name,
                                     uint64_t timestamp,
                                     const TablesMetrics& metrics_by_table_name,
                                     const TablesHistMetrics& hist_metrics_by_table_name);
  Status ReportHostLevelMetrics(const std::string& host_name,
                                uint64_t timestamp,
                                const Metrics& host_metrics,
                                const HistMetrics& host_hist_metrics);
  Status ReportTableLevelMetrics(uint64_t timestamp,
                                 const TablesMetrics& metrics_by_table_name,
                                 const TablesHistMetrics& hist_metrics_by_table_name);
  Status ReportClusterLevelMetrics(uint64_t timestamp,
                                   const Metrics& cluster_metrics);
  static int64_t GetHistValue(const std::vector<SimpleHistogram>& hist_values);

  // Get metrics from server by http method.
  static Status GetMetrics(const std::string& url, std::string* resp);

  // Parse metrics from http response, entities may be in different types.
  Status ParseMetrics(NodeType node_type,
                      const std::string& data,
                      TablesMetrics* metrics_by_table_name,
                      Metrics* host_metrics,
                      TablesHistMetrics* hist_metrics_by_table_name,
                      HistMetrics* host_hist_metrics);
  Status ParseServerMetrics(const JsonReader& r,
                            const rapidjson::Value* entity,
                            Metrics* host_metrics,
                            HistMetrics* host_hist_metrics) const;
  Status ParseTableMetrics(const JsonReader& r,
                           const rapidjson::Value* entity,
                           TablesMetrics* metrics_by_table_name,
                           Metrics* host_metrics,
                           TablesHistMetrics* hist_metrics_by_table_name,
                           HistMetrics* host_hist_metrics) const;
  Status ParseCatalogMetrics(const JsonReader& r,
                             const rapidjson::Value* entity,
                             Metrics* tablet_metrics,
                             HistMetrics* tablet_hist_metrics) const;
  Status ParseEntityMetrics(const JsonReader& r,
                            const rapidjson::Value* entity,
                            Metrics* kv_metrics,
                            Metrics* merged_kv_metrics,
                            HistMetrics* hist_metrics,
                            HistMetrics* merged_hist_metrics) const;

  Status GetNumberMetricValue(const rapidjson::Value* metric,
                              const std::string& metric_name,
                              int64_t* result) const;
  Status GetStringMetricValue(const rapidjson::Value* metric,
                              const std::string& metric_name,
                              int64_t* result) const;
  static Status ConvertStateToInt(const std::string& value, int64_t* result);

  static const std::set<std::string> kRegisterPercentiles;

  bool initialized_;

  scoped_refptr<NodesChecker> nodes_checker_;
  scoped_refptr<ReporterBase> reporter_;

  MetricTypes metric_types_;
  // Attribute filter, attributes not in this map will be filtered if it's not empty.
  // attribute name ---> attribute values
  std::unordered_map<std::string, std::set<std::string>> attributes_filter_;
  std::string metric_url_parameters_;
  std::unordered_set<std::string> hosttable_metrics_;
  Metrics cluster_metrics_;

  CountDownLatch stop_background_threads_latch_;
  scoped_refptr<Thread> metric_collector_thread_;
  std::unique_ptr<ThreadPool> host_metric_collector_thread_pool_;

  DISALLOW_COPY_AND_ASSIGN(MetricsCollector);
};
} // namespace collector
} // namespace kudu
