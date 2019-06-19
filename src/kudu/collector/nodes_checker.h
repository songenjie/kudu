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

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tools/ksck_results.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/status.h"

namespace kudu {
class Thread;
}  // namespace kudu

namespace kudu {

namespace collector {

class ReporterBase;

class NodesChecker {
 public:
  explicit NodesChecker(std::shared_ptr<ReporterBase> reporter);
  ~NodesChecker();

  Status Init();
  Status Start();
  void Shutdown();

  std::string ToString() const;

  std::vector<std::string> GetNodes();
  std::string GetFirstNode();

 private:
  FRIEND_TEST(TestNodesChecker, TestExtractServerHealthStatus);
  FRIEND_TEST(TestNodesChecker, TestExtractTableHealthStatus);

  Status StartNodesCheckerThread();
  void NodesCheckerThread();

  void UpdateAndCheckNodes();
  Status UpdateNodes();
  Status CheckNodes() const;
  Status ReportNodesMetrics(const std::string& data) const;

  static tools::KsckServerHealth ExtractServerHealthStatus(const std::string& health);
  static tools::KsckCheckResult ExtractTableHealthStatus(const std::string& health);

  bool initialized_;

  std::shared_ptr<ReporterBase> reporter_;

  CountDownLatch stop_background_threads_latch_;
  scoped_refptr<Thread> nodes_checker_thread_;

  mutable RWMutex tserver_http_addrs_lock_;
  std::vector<std::string> tserver_http_addrs_;

  DISALLOW_COPY_AND_ASSIGN(NodesChecker);
};
} // namespace collector
} // namespace kudu
