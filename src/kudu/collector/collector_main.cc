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

#include <iostream>
#include <string>

#include <glog/logging.h>

#include "kudu/collector/collector.h"
#include "kudu/collector/collector_util.h"
#include "kudu/util/flags.h"
#include "kudu/util/init.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/version_info.h"

namespace kudu {
namespace collector {

static int CollectorMain(int argc, char** argv) {
  InitKuduOrDie();

  GFlagsMap default_flags = GetFlagsMap();

  ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 1) {
    std::cerr << "usage: " << argv[0] << std::endl;
    return 1;
  }
  std::string nondefault_flags = GetNonDefaultFlags(default_flags);
  InitGoogleLoggingSafe(argv[0]);

  LOG(INFO) << "Collector non-default flags:\n"
            << nondefault_flags << '\n'
            << "Collector version:\n"
            << VersionInfo::GetAllVersionInfo();

  Collector collector;
  LOG(INFO) << "Initializing collector...";
  CHECK_OK(collector.Init());

  LOG(INFO) << "Starting collector...";
  CHECK_OK(collector.Start());

  LOG(INFO) << "Collector successfully started.";
  while (!RunOnceMode()) {
    SleepFor(MonoDelta::FromSeconds(60));
  }

  return 0;
}

} // namespace collector
} // namespace kudu

int main(int argc, char** argv) {
  return kudu::collector::CollectorMain(argc, argv);
}
