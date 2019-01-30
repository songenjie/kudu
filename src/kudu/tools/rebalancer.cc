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

#include "kudu/tools/rebalancer.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/ksck_remote.h"
#include "kudu/tools/ksck_results.h"
#include "kudu/tools/placement_policy_util.h"
#include "kudu/tools/rebalance_algo.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tools/tool_replica_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using std::accumulate;
using std::endl;
using std::back_inserter;
using std::inserter;
using std::ostream;
using std::map;
using std::multimap;
using std::numeric_limits;
using std::pair;
using std::set_difference;
using std::set;
using std::shared_ptr;
using std::sort;
using std::string;
using std::to_string;
using std::transform;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

Rebalancer::Config::Config(
    std::vector<std::string> ignored_tservers_param,
    std::vector<std::string> blacklist_tservers,
    std::vector<std::string> master_addresses,
    std::vector<std::string> table_filters,
    size_t max_moves_per_server,
    size_t max_staleness_interval_sec,
    int64_t max_run_time_sec,
    bool move_rf1_replicas,
    bool output_replica_distribution_details,
    bool run_policy_fixer,
    bool run_cross_location_rebalancing,
    bool run_intra_location_rebalancing,
    double load_imbalance_threshold)
    : ignored_tservers(ignored_tservers_param.begin(), ignored_tservers_param.end()),
      blacklist_tservers(std::move(blacklist_tservers)),
      master_addresses(std::move(master_addresses)),
      table_filters(std::move(table_filters)),
      max_moves_per_server(max_moves_per_server),
      max_staleness_interval_sec(max_staleness_interval_sec),
      max_run_time_sec(max_run_time_sec),
      move_rf1_replicas(move_rf1_replicas),
      output_replica_distribution_details(output_replica_distribution_details),
      run_policy_fixer(run_policy_fixer),
      run_cross_location_rebalancing(run_cross_location_rebalancing),
      run_intra_location_rebalancing(run_intra_location_rebalancing),
      load_imbalance_threshold(load_imbalance_threshold) {
  DCHECK_GE(max_moves_per_server, 0);
}

Rebalancer::Rebalancer(const Config& config)
    : config_(config) {
}

Status Rebalancer::PrintStats(ostream& out) {
  // First, report on the current balance state of the cluster.
  ClusterRawInfo raw_info;
  RETURN_NOT_OK(GetClusterRawInfo(boost::none, &raw_info));

  ClusterInfo ci;
  RETURN_NOT_OK(BuildClusterInfo(raw_info, MovesInProgress(), &ci));

  const auto& ts_id_by_location = ci.locality.servers_by_location;
  if (ts_id_by_location.empty()) {
    // Nothing to report about: there are no tablet servers reported.
    out << "an empty cluster" << endl;
    return Status::OK();
  }

  // Print information about replica count of blacklist tservers.
  RETURN_NOT_OK(PrintBlacklistTserversStats(ci, out));

  if (ts_id_by_location.size() == 1) {
    // That's about printing information about the whole cluster.
    return PrintLocationBalanceStats(ts_id_by_location.begin()->first,
                                     raw_info, ci, out);
  }

  // The stats are more detailed in the case of a multi-location cluster.
  DCHECK_GT(ts_id_by_location.size(), 1);

  // 1. Print information about cross-location balance.
  RETURN_NOT_OK(PrintCrossLocationBalanceStats(ci, out));

  // 2. Iterating over locations in the cluster, print per-location balance
  //    information. Since the ts_id_by_location is not sorted, let's first
  //    create a sorted list of locations so the ouput would be sorted by
  //    location.
  vector<string> locations;
  locations.reserve(ts_id_by_location.size());
  transform(ts_id_by_location.cbegin(), ts_id_by_location.cend(),
            back_inserter(locations),
            [](const unordered_map<string, set<string>>::value_type& elem) {
              return elem.first;
            });
  sort(locations.begin(), locations.end());

  for (const auto& location : locations) {
    ClusterRawInfo raw_info;
    RETURN_NOT_OK(KsckResultsToClusterRawInfo(location, ksck_->results(), &raw_info));
    ClusterInfo ci;
    RETURN_NOT_OK(BuildClusterInfo(raw_info, MovesInProgress(), &ci));
    RETURN_NOT_OK(PrintLocationBalanceStats(location, raw_info, ci, out));
  }

  // 3. Print information about placement policy violations.
  RETURN_NOT_OK(PrintPolicyViolationInfo(raw_info, out));

  return Status::OK();
}

Status Rebalancer::Run(RunStatus* result_status, size_t* moves_count) {
  DCHECK(result_status);
  *result_status = RunStatus::UNKNOWN;

  boost::optional<MonoTime> deadline;
  if (config_.max_run_time_sec != 0) {
    deadline = MonoTime::Now() + MonoDelta::FromSeconds(config_.max_run_time_sec);
  }

  ClusterRawInfo raw_info;
  RETURN_NOT_OK(
      KsckResultsToClusterRawInfo(boost::none, ksck_->results(), &raw_info));

  ClusterInfo ci;
  RETURN_NOT_OK(BuildClusterInfo(raw_info, MovesInProgress(), &ci));

  const auto& ts_id_by_location = ci.locality.servers_by_location;
  if (ts_id_by_location.empty()) {
    // Empty cluster: no tablet servers reported.
    if (moves_count != nullptr) {
      *moves_count = 0;
    }
    *result_status = RunStatus::CLUSTER_IS_BALANCED;
    LOG(INFO) << "no tablet servers are reported: nothing to balance";
    return Status::OK();
  }

  size_t moves_count_total = 0;
  if (ts_id_by_location.size() == 1) {
    const auto& location = ts_id_by_location.cbegin()->first;
    LOG(INFO) << "running whole-cluster rebalancing";
    IntraLocationRunner runner(
        this, config_.ignored_tservers, config_.max_moves_per_server, deadline, location);
    RETURN_NOT_OK(runner.Init(config_.master_addresses));
    RETURN_NOT_OK(RunWith(&runner, result_status));
    moves_count_total += runner.moves_count();
  } else {
    // The essence of location-aware balancing:
    //   1. Find tablets whose replicas placed in such a way that their
    //      distribution violates the main constraint of the placement policy.
    //      For each non-conforming tablet, move its replicas to restore
    //      the placement policy restrictions. In other words, if a location has
    //      more than the majority of replicas for some tablet,
    //      move the replicas of the tablet to other locations.
    //   2. For every tablet whose replica placement does not violate the
    //      placement policy constraints, balance the load among locations.
    //   3. Balance replica distribution within every location. This is a.k.a.
    //      intra-location balancing. The intra-location balancing involves
    //      moving replicas only within location, no replicas are moved between
    //      locations.
    if (config_.run_policy_fixer) {
      // Fix placement policy violations, if any.
      LOG(INFO) << "fixing placement policy violations";
      PolicyFixer runner(
          this, config_.ignored_tservers, config_.max_moves_per_server, deadline);
      RETURN_NOT_OK(runner.Init(config_.master_addresses));
      RETURN_NOT_OK(RunWith(&runner, result_status));
      moves_count_total += runner.moves_count();
    }
    if (config_.run_cross_location_rebalancing) {
      // Run the rebalancing across locations (inter-location rebalancing).
      LOG(INFO) << "running cross-location rebalancing";
      CrossLocationRunner runner(this,
                                 config_.ignored_tservers,
                                 config_.max_moves_per_server,
                                 config_.load_imbalance_threshold,
                                 deadline);
      RETURN_NOT_OK(runner.Init(config_.master_addresses));
      RETURN_NOT_OK(RunWith(&runner, result_status));
      moves_count_total += runner.moves_count();
    }
    if (config_.run_intra_location_rebalancing) {
      // Run the rebalancing within every location (intra-location rebalancing).
      for (const auto& elem : ts_id_by_location) {
        const auto& location = elem.first;
        // TODO(aserbin): it would be nice to run these rebalancers in parallel
        LOG(INFO) << "running rebalancer within location '" << location << "'";
        IntraLocationRunner runner(this,
                                   config_.ignored_tservers,
                                   config_.max_moves_per_server,
                                   deadline,
                                   location);
        RETURN_NOT_OK(runner.Init(config_.master_addresses));
        RETURN_NOT_OK(RunWith(&runner, result_status));
        moves_count_total += runner.moves_count();
      }
    }
  }
  if (moves_count != nullptr) {
    *moves_count = moves_count_total;
  }

  return Status::OK();
}

Status Rebalancer::KsckResultsToClusterRawInfo(
    const boost::optional<string>& location,
    const KsckResults& ksck_info,
    ClusterRawInfo* raw_info) {
  DCHECK(raw_info);

  // Filter out entities that are not relevant to the specified location.
  vector<KsckServerHealthSummary> tserver_summaries;
  tserver_summaries.reserve(ksck_info.tserver_summaries.size());

  vector<KsckTabletSummary> tablet_summaries;
  tablet_summaries.reserve(ksck_info.tablet_summaries.size());

  vector<KsckTableSummary> table_summaries;
  table_summaries.reserve(table_summaries.size());

  if (!location) {
    // Information on the whole cluster.
    tserver_summaries = ksck_info.tserver_summaries;
    tablet_summaries = ksck_info.tablet_summaries;
    table_summaries = ksck_info.table_summaries;
  } else {
    // Information on the specified location only: filter out non-relevant info.
    const auto& location_str =  *location;

    unordered_set<string> ts_ids_at_location;
    for (const auto& summary : ksck_info.tserver_summaries) {
      if (summary.ts_location == location_str) {
        tserver_summaries.push_back(summary);
        InsertOrDie(&ts_ids_at_location, summary.uuid);
      }
    }

    unordered_set<string> table_ids_at_location;
    for (const auto& summary : ksck_info.tablet_summaries) {
      const auto& replicas = summary.replicas;
      decltype(summary.replicas) replicas_at_location;
      replicas_at_location.reserve(replicas.size());
      for (const auto& replica : replicas) {
        if (ContainsKey(ts_ids_at_location, replica.ts_uuid)) {
          replicas_at_location.push_back(replica);
        }
      }
      if (!replicas_at_location.empty()) {
        table_ids_at_location.insert(summary.table_id);
      }
      tablet_summaries.push_back(summary);
      tablet_summaries.back().replicas = std::move(replicas_at_location);
    }

    for (const auto& summary : ksck_info.table_summaries) {
      if (ContainsKey(table_ids_at_location, summary.id)) {
        table_summaries.push_back(summary);
      }
    }
  }

  raw_info->tserver_summaries = std::move(tserver_summaries);
  raw_info->table_summaries = std::move(table_summaries);
  raw_info->tablet_summaries = std::move(tablet_summaries);

  return Status::OK();
}

// Given high-level description of moves, find tablets with replicas at the
// corresponding tablet servers to satisfy those high-level descriptions.
// The idea is to find all tablets of the specified table that would have a
// replica at the source server, but would not have a replica at the destination
// server. That is to satisfy the restriction of having no more than one replica
// of the same tablet per server.
//
// An additional constraint: it's better not to move leader replicas, if
// possible. If a client has a write operation in progress, moving leader
// replicas of affected tablets would make the client to re-resolve new leaders
// and retry the operations. Moving leader replicas is used as last resort
// when no other candidates are left.
Status Rebalancer::FindReplicas(const TableReplicaMove& move,
                                const ClusterRawInfo& raw_info,
                                vector<string>* tablet_ids,
                                bool *is_healthy_move) {
  tablet_ids->clear();
  const auto& table_id = move.table_id;

  // Tablet ids of replicas on the source tserver that are non-leaders.
  vector<string> tablet_uuids_src;
  // Tablet ids of replicas on the source tserver that are leaders.
  vector<string> tablet_uuids_src_leaders;
  // UUIDs of tablets of the selected table at the destination tserver.
  vector<string> tablet_uuids_dst;

  for (const auto& tablet_summary : raw_info.tablet_summaries) {
    if (tablet_summary.table_id != table_id) {
      continue;
    }
    if (tablet_summary.result != KsckCheckResult::HEALTHY) {
      VLOG(1) << Substitute("table $0: not considering replicas of tablet $1 "
                            "as candidates for movement since the tablet's "
                            "status is '$2'",
                            table_id, tablet_summary.id,
                            KsckCheckResultToString(tablet_summary.result));
      continue;
    }
    for (const auto& replica_summary : tablet_summary.replicas) {
      if (replica_summary.ts_uuid != move.from &&
          replica_summary.ts_uuid != move.to) {
        continue;
      }
      if (!replica_summary.ts_healthy) {
        VLOG(1) << Substitute("table $0: not considering replica movement "
                              "from $1 to $2 since server $3 is not healthy",
                              table_id,
                              move.from, move.to, replica_summary.ts_uuid);
        continue;
      }
      if (replica_summary.ts_uuid == move.from) {
        if (replica_summary.is_leader) {
          tablet_uuids_src_leaders.emplace_back(tablet_summary.id);
        } else {
          tablet_uuids_src.emplace_back(tablet_summary.id);
        }
      } else {
        DCHECK_EQ(move.to, replica_summary.ts_uuid);
        tablet_uuids_dst.emplace_back(tablet_summary.id);
      }
    }
  }
  if (tablet_uuids_src.empty() && tablet_uuids_src_leaders.empty()) {
    // No healthy tablet of specific table at source server
    *is_healthy_move = false;
    return Status::OK();
  }

  sort(tablet_uuids_src.begin(), tablet_uuids_src.end());
  sort(tablet_uuids_dst.begin(), tablet_uuids_dst.end());

  vector<string> tablet_uuids;
  set_difference(
      tablet_uuids_src.begin(), tablet_uuids_src.end(),
      tablet_uuids_dst.begin(), tablet_uuids_dst.end(),
      inserter(tablet_uuids, tablet_uuids.begin()));

  if (!tablet_uuids.empty()) {
    // If there are tablets with non-leader replicas at the source server,
    // those are the best candidates for movement.
    tablet_ids->swap(tablet_uuids);
    return Status::OK();
  }

  // If no tablets with non-leader replicas were found, resort to tablets with
  // leader replicas at the source server.
  DCHECK(tablet_uuids.empty());
  sort(tablet_uuids_src_leaders.begin(), tablet_uuids_src_leaders.end());
  set_difference(
      tablet_uuids_src_leaders.begin(), tablet_uuids_src_leaders.end(),
      tablet_uuids_dst.begin(), tablet_uuids_dst.end(),
      inserter(tablet_uuids, tablet_uuids.begin()));

  tablet_ids->swap(tablet_uuids);

  return Status::OK();
}

void Rebalancer::FilterMoves(const MovesInProgress& scheduled_moves,
                             vector<ReplicaMove>* replica_moves) {
  unordered_set<string> tablet_uuids;
  vector<ReplicaMove> filtered_replica_moves;
  for (auto& move_op : *replica_moves) {
    const auto& tablet_uuid = move_op.tablet_uuid;
    if (ContainsKey(scheduled_moves, tablet_uuid)) {
      // There is a move operation in progress for the tablet, don't schedule
      // another one.
      continue;
    }
    if (PREDICT_TRUE(tablet_uuids.emplace(tablet_uuid).second)) {
      filtered_replica_moves.emplace_back(std::move(move_op));
    } else {
      // Rationale behind the unique tablet constraint: the implementation of
      // the Run() method is designed to re-order operations suggested by the
      // high-level algorithm to use the op-count-per-tablet-server capacity
      // as much as possible. Right now, the RunStep() method outputs only one
      // move operation per tablet in every batch. The code below is to
      // enforce the contract between Run() and RunStep() methods.
      LOG(DFATAL) << "detected multiple replica move operations for the same "
                     "tablet " << tablet_uuid;
    }
  }
  *replica_moves = std::move(filtered_replica_moves);
}

Status Rebalancer::FilterCrossLocationTabletCandidates(
    const unordered_map<string, string>& location_by_ts_id,
    const TabletsPlacementInfo& placement_info,
    const TableReplicaMove& move,
    vector<string>* tablet_ids) {
  DCHECK(tablet_ids);

  if (tablet_ids->empty()) {
    // Nothing to filter.
    return Status::OK();
  }

  const auto& dst_location = FindOrDie(location_by_ts_id, move.to);
  const auto& src_location = FindOrDie(location_by_ts_id, move.from);

  // Sanity check: the source and the destination tablet servers should be
  // in different locations.
  if (src_location == dst_location) {
    return Status::InvalidArgument(Substitute(
        "moving replicas of table $0: the same location '$1' for both "
        "the source ($2) and the destination ($3) tablet servers",
         move.table_id, src_location, move.from, move.to));
  }
  if (dst_location.empty()) {
    // The destination location is not specified, so no restrictions on the
    // destination location to check for.
    return Status::OK();
  }

  vector<string> tablet_ids_filtered;
  for (auto& tablet_id : *tablet_ids) {
    const auto& replica_count_info = FindOrDie(
        placement_info.tablet_location_info, tablet_id);
    const auto* count_ptr = FindOrNull(replica_count_info, dst_location);
    if (count_ptr == nullptr) {
      // Nothing else to clarify: not a single replica in the destnation
      // location for this candidate tablet.
      tablet_ids_filtered.emplace_back(std::move(tablet_id));
      continue;
    }
    const auto location_replica_num = *count_ptr;
    const auto& table_id = FindOrDie(placement_info.tablet_to_table_id, tablet_id);
    const auto& table_info = FindOrDie(placement_info.tables_info, table_id);
    const auto rf = table_info.replication_factor;
    // In case of RF=2*N+1, losing (N + 1) replicas means losing the majority.
    // In case of RF=2*N, losing at least N replicas means losing the majority.
    const auto replica_num_threshold = rf % 2 ? consensus::MajoritySize(rf)
                                              : rf / 2;
    if (location_replica_num + 1 >= replica_num_threshold) {
      VLOG(1) << Substitute("destination location '$0' for candidate tablet $1 "
                            "already contains $2 of $3 replicas",
                            dst_location, tablet_id, location_replica_num, rf);
      continue;
    }
    // No majority of replicas in the destination location: it's OK candidate.
    tablet_ids_filtered.emplace_back(std::move(tablet_id));
  }

  *tablet_ids = std::move(tablet_ids_filtered);

  return Status::OK();
}

Status Rebalancer::PrintBlacklistTserversStats(const ClusterInfo& ci,
                                               ostream& out) const {
  const auto& blacklist_tservers = ci.blacklist_tservers;
  if (blacklist_tservers.empty()) {
    out << "No tablet server is in blacklist." << endl;
    return Status::OK();
  }
  out << "Blacklist_tservers replica distribution summary:" << endl;
  DataTable summary({"Server UUID", "Replica Count"});
  for (const auto& elem: blacklist_tservers) {
    const auto& server_uuid = elem.first;
    const auto& replica_count_by_table = elem.second;
    int32_t replica_count = 0;
    for (const auto& e : replica_count_by_table) {
      replica_count += e.second;
    }
    summary.AddRow({ server_uuid, to_string(replica_count) });
  }
  RETURN_NOT_OK(summary.PrintTo(out));
  out << endl;

  return Status::OK();
}

Status Rebalancer::PrintCrossLocationBalanceStats(const ClusterInfo& ci,
                                                  ostream& out) const {
  // Print location load information.
  map<string, int64_t> replicas_num_by_location;
  for (const auto& elem : ci.balance.servers_by_total_replica_count) {
    const auto& location = FindOrDie(ci.locality.location_by_ts_id, elem.second);
    LookupOrEmplace(&replicas_num_by_location, location, 0) += elem.first;
  }
  out << "Locations load summary:" << endl;
  DataTable location_load_summary({"Location", "Load"});
  for (const auto& elem : replicas_num_by_location) {
    const auto& location = elem.first;
    const auto servers_num =
        FindOrDie(ci.locality.servers_by_location, location).size();
    CHECK_GT(servers_num, 0);
    double location_load = static_cast<double>(elem.second) / servers_num;
    location_load_summary.AddRow({ location, to_string(location_load) });
  }
  RETURN_NOT_OK(location_load_summary.PrintTo(out));
  out << endl;

  return Status::OK();
}

Status Rebalancer::PrintLocationBalanceStats(const string& location,
                                             const ClusterRawInfo& raw_info,
                                             const ClusterInfo& ci,
                                             ostream& out) const {
  if (!location.empty()) {
    out << "--------------------------------------------------" << endl;
    out << "Location: " << location << endl;
    out << "--------------------------------------------------" << endl;
  }

  // Per-server replica distribution stats.
  {
    out << "Per-server replica distribution summary:" << endl;
    DataTable summary({"Statistic", "Value"});

    const auto& servers_load_info = ci.balance.servers_by_total_replica_count;
    if (servers_load_info.empty()) {
      summary.AddRow({ "N/A", "N/A" });
    } else {
      const int64_t total_replica_count = accumulate(
          servers_load_info.begin(), servers_load_info.end(), 0L,
          [](int64_t sum, const pair<int32_t, string>& elem) {
            return sum + elem.first;
          });

      const auto min_replica_count = servers_load_info.begin()->first;
      const auto max_replica_count = servers_load_info.rbegin()->first;
      const double avg_replica_count =
          1.0 * total_replica_count / servers_load_info.size();

      summary.AddRow({ "Minimum Replica Count", to_string(min_replica_count) });
      summary.AddRow({ "Maximum Replica Count", to_string(max_replica_count) });
      summary.AddRow({ "Average Replica Count", to_string(avg_replica_count) });
    }
    RETURN_NOT_OK(summary.PrintTo(out));
    out << endl;

    if (config_.output_replica_distribution_details) {
      const auto& tserver_summaries = raw_info.tserver_summaries;
      unordered_map<string, string> tserver_endpoints;
      for (const auto& summary : tserver_summaries) {
        tserver_endpoints.emplace(summary.uuid, summary.address);
      }

      out << "Per-server replica distribution details:" << endl;
      DataTable servers_info({ "UUID", "Address", "Replica Count" });
      for (const auto& elem : servers_load_info) {
        const auto& id = elem.second;
        servers_info.AddRow({ id, tserver_endpoints[id], to_string(elem.first) });
      }
      RETURN_NOT_OK(servers_info.PrintTo(out));
      out << endl;
    }
  }

  // Per-table replica distribution stats.
  {
    out << "Per-table replica distribution summary:" << endl;
    DataTable summary({ "Replica Skew", "Value" });
    const auto& table_skew_info = ci.balance.table_info_by_skew;
    if (table_skew_info.empty()) {
      summary.AddRow({ "N/A", "N/A" });
    } else {
      const auto min_table_skew = table_skew_info.begin()->first;
      const auto max_table_skew = table_skew_info.rbegin()->first;
      const int64_t sum_table_skew = accumulate(
          table_skew_info.begin(), table_skew_info.end(), 0L,
          [](int64_t sum, const pair<int32_t, TableBalanceInfo>& elem) {
            return sum + elem.first;
          });
      double avg_table_skew = 1.0 * sum_table_skew / table_skew_info.size();

      summary.AddRow({ "Minimum", to_string(min_table_skew) });
      summary.AddRow({ "Maximum", to_string(max_table_skew) });
      summary.AddRow({ "Average", to_string(avg_table_skew) });
    }
    RETURN_NOT_OK(summary.PrintTo(out));
    out << endl;

    if (config_.output_replica_distribution_details) {
      const auto& table_summaries = raw_info.table_summaries;
      unordered_map<string, const KsckTableSummary*> table_info;
      for (const auto& summary : table_summaries) {
        table_info.emplace(summary.id, &summary);
      }
      out << "Per-table replica distribution details:" << endl;
      DataTable skew(
          { "Table Id", "Replica Count", "Replica Skew", "Table Name" });
      for (const auto& elem : table_skew_info) {
        const auto& table_id = elem.second.table_id;
        const auto it = table_info.find(table_id);
        const auto* table_summary =
            (it == table_info.end()) ? nullptr : it->second;
        const auto& table_name = table_summary ? table_summary->name : "";
        const auto total_replica_count = table_summary
            ? table_summary->replication_factor * table_summary->TotalTablets()
            : 0;
        skew.AddRow({ table_id,
                      to_string(total_replica_count),
                      to_string(elem.first),
                      table_name });
      }
      RETURN_NOT_OK(skew.PrintTo(out));
      out << endl;
    }
  }

  return Status::OK();
}

Status Rebalancer::PrintPolicyViolationInfo(const ClusterRawInfo& raw_info,
                                            ostream& out) const {
  TabletsPlacementInfo placement_info;
  RETURN_NOT_OK(BuildTabletsPlacementInfo(
      raw_info, MovesInProgress(), &placement_info));
  vector<PlacementPolicyViolationInfo> ppvi;
  RETURN_NOT_OK(DetectPlacementPolicyViolations(placement_info, &ppvi));
  out << "Placement policy violations:" << endl;
  if (ppvi.empty()) {
    out << "  none" << endl << endl;
    return Status::OK();
  }

  DataTable summary({ "Location",
                      "Number of non-complying tables",
                      "Number of non-complying tablets" });
  typedef pair<unordered_set<string>, unordered_set<string>> TableTabletIds;
  // Location --> sets of identifiers of tables and tablets hosted by the
  // tablet servers at the location. The summary is sorted by location.
  map<string, TableTabletIds> info_by_location;
  for (const auto& info : ppvi) {
    const auto& table_id = FindOrDie(placement_info.tablet_to_table_id,
                                     info.tablet_id);
    auto& elem = LookupOrEmplace(&info_by_location,
                                 info.majority_location, TableTabletIds());
    elem.first.emplace(table_id);
    elem.second.emplace(info.tablet_id);
  }
  for (const auto& elem : info_by_location) {
    summary.AddRow({ elem.first,
                     to_string(elem.second.first.size()),
                     to_string(elem.second.second.size()) });
  }
  RETURN_NOT_OK(summary.PrintTo(out));
  out << endl;
  // If requested, print details on detected policy violations.
  if (config_.output_replica_distribution_details) {
    out << "Placement policy violation details:" << endl;
    DataTable stats(
        { "Location", "Table Name", "Tablet", "RF", "Replicas at location" });
    for (const auto& info : ppvi) {
      const auto& table_id = FindOrDie(placement_info.tablet_to_table_id,
                                       info.tablet_id);
      const auto& table_info = FindOrDie(placement_info.tables_info, table_id);
      stats.AddRow({ info.majority_location,
                     table_info.name,
                     info.tablet_id,
                     to_string(table_info.replication_factor),
                     to_string(info.replicas_num_at_majority_location) });
    }
    RETURN_NOT_OK(stats.PrintTo(out));
    out << endl;
  }

  return Status::OK();
}

Status Rebalancer::BuildClusterInfo(const ClusterRawInfo& raw_info,
                                    const MovesInProgress& moves_in_progress,
                                    ClusterInfo* info) const {
  DCHECK(info);

  // tserver UUID --> total replica count of all table's tablets at the server
  typedef unordered_map<string, int32_t> TableReplicasAtServer;

  // The result information to build.
  ClusterInfo result_info;

  unordered_map<string, int32_t> tserver_replicas_count;
  unordered_map<string, TableReplicasAtServer> table_replicas_info;

  // Build a set of tables with RF=1 (single replica tables).
  unordered_set<string> rf1_tables;
  if (!config_.move_rf1_replicas) {
    for (const auto& s : raw_info.table_summaries) {
      if (s.replication_factor == 1) {
        rf1_tables.emplace(s.id);
      }
    }
  }

  auto& ts_uuids_by_location = result_info.locality.servers_by_location;
  auto& location_by_ts_uuid = result_info.locality.location_by_ts_id;
  for (const auto& summary : raw_info.tserver_summaries) {
    const auto& ts_id = summary.uuid;
    const auto& ts_location = summary.ts_location;
    VLOG(1) << Substitute("found tserver $0 at location '$1'", ts_id, ts_location);
    EmplaceOrDie(&location_by_ts_uuid, ts_id, ts_location);
    auto& ts_ids = LookupOrEmplace(&ts_uuids_by_location,
                                   ts_location, set<string>());
    InsertOrDie(&ts_ids, ts_id);
  }

  for (const auto& s : raw_info.tserver_summaries) {
    if (s.health != KsckServerHealth::HEALTHY) {
      LOG(INFO) << Substitute("skipping tablet server $0 ($1) because of its "
                              "non-HEALTHY status ($2)",
                              s.uuid, s.address,
                              ServerHealthToString(s.health));
      continue;
    }
    tserver_replicas_count.emplace(s.uuid, 0);
  }

  for (const auto& tablet : raw_info.tablet_summaries) {
    if (!config_.move_rf1_replicas) {
      if (ContainsKey(rf1_tables, tablet.table_id)) {
        LOG(INFO) << Substitute("tablet $0 of table '$1' ($2) has single replica, skipping",
                                tablet.id, tablet.table_name, tablet.table_id);
        continue;
      }
    }

    // Check if it's one of the tablets which are currently being rebalanced.
    // If so, interpret the move as successfully completed, updating the
    // replica counts correspondingly.
    const auto it_pending_moves = moves_in_progress.find(tablet.id);
    if (it_pending_moves != moves_in_progress.end()) {
      const auto& move_info = it_pending_moves->second;
      bool is_target_replica_present = false;
      // Verify that the target replica is present in the config.
      for (const auto& tr : tablet.replicas) {
        if (tr.ts_uuid == move_info.ts_uuid_to) {
          is_target_replica_present = true;
          break;
        }
      }
      // If the target replica is present, it will be processed in the code
      // below. Otherwise, it's necessary to pretend as if the target replica
      // is in the config already: the idea is to count in the absent target
      // replica as if the movement has successfully completed already.
      auto it = tserver_replicas_count.find(move_info.ts_uuid_to);
      if (!is_target_replica_present && !move_info.ts_uuid_to.empty() &&
          it != tserver_replicas_count.end()) {
        it->second++;
        auto table_ins = table_replicas_info.emplace(
            tablet.table_id, TableReplicasAtServer());
        TableReplicasAtServer& replicas_at_server = table_ins.first->second;

        auto replicas_ins = replicas_at_server.emplace(move_info.ts_uuid_to, 0);
        replicas_ins.first->second++;
      }
    }

    for (const auto& ri : tablet.replicas) {
      // Increment total count of replicas at the tablet server.
      auto it = tserver_replicas_count.find(ri.ts_uuid);
      if (it == tserver_replicas_count.end()) {
        string msg = Substitute("skipping replica at tserver $0", ri.ts_uuid);
        if (ri.ts_address) {
          msg += " (" + *ri.ts_address + ")";
        }
        msg += " since it's not reported among known tservers";
        LOG(INFO) << msg;
        continue;
      }
      bool do_count_replica = true;
      if (it_pending_moves != moves_in_progress.end()) {
        const auto& move_info = it_pending_moves->second;
        if (move_info.ts_uuid_from == ri.ts_uuid) {
          DCHECK(!ri.ts_uuid.empty());
          // The source replica of the scheduled replica movement operation
          // are still in the config. Interpreting the move as successfully
          // completed, so the source replica should not be counted in.
          do_count_replica = false;
        }
      }
      if (do_count_replica) {
        it->second++;
      }

      auto table_ins = table_replicas_info.emplace(
          tablet.table_id, TableReplicasAtServer());
      TableReplicasAtServer& replicas_at_server = table_ins.first->second;

      auto replicas_ins = replicas_at_server.emplace(ri.ts_uuid, 0);
      if (do_count_replica) {
        replicas_ins.first->second++;
      }
    }
  }

  // Check for the consistency of information derived from the ksck report.
  for (const auto& elem : tserver_replicas_count) {
    const auto& ts_uuid = elem.first;
    int32_t count_by_table_info = 0;
    for (auto& e : table_replicas_info) {
      count_by_table_info += e.second[ts_uuid];
    }
    if (elem.second != count_by_table_info) {
      return Status::Corruption("inconsistent cluster state returned by ksck");
    }
  }

  // Populate ClusterBalanceInfo::servers_by_total_replica_count
  auto& servers_by_count = result_info.balance.servers_by_total_replica_count;
  for (const auto& elem : tserver_replicas_count) {
    servers_by_count.emplace(elem.second, elem.first);
  }

  // Populate ClusterBalanceInfo::table_info_by_skew
  auto& table_info_by_skew = result_info.balance.table_info_by_skew;
  for (const auto& elem : table_replicas_info) {
    const auto& table_id = elem.first;
    int32_t max_count = numeric_limits<int32_t>::min();
    int32_t min_count = numeric_limits<int32_t>::max();
    TableBalanceInfo tbi;
    tbi.table_id = table_id;
    for (const auto& e : elem.second) {
      const auto& ts_uuid = e.first;
      const auto replica_count = e.second;
      tbi.servers_by_replica_count.emplace(replica_count, ts_uuid);
      max_count = std::max(replica_count, max_count);
      min_count = std::min(replica_count, min_count);
    }
    table_info_by_skew.emplace(max_count - min_count, std::move(tbi));
  }

  // Populate ClusterInfo::blacklist_tservers
  auto& blacklist_tservers_info = result_info.blacklist_tservers;
  for (const auto& blacklist_tserver : config_.blacklist_tservers) {
    if (!ContainsKey(tserver_replicas_count, blacklist_tserver)) {
      return Status::InvalidArgument(Substitute(
          "blacklist tserver $0 is not reported among known tservers", blacklist_tserver));
    }
    ReplicaCountByTable replica_count_by_table;
    for (const auto& elem : table_replicas_info) {
      const auto& table_id = elem.first;
      auto& replica_count = LookupOrEmplace(&replica_count_by_table, table_id, 0);
      replica_count += FindOrDie(elem.second, blacklist_tserver);
    }
    blacklist_tservers_info.emplace(blacklist_tserver, std::move(replica_count_by_table));
  }

  // TODO(aserbin): add sanity checks on the result.
  *info = std::move(result_info);

  return Status::OK();
}

Status Rebalancer::RunWith(Runner* runner, RunStatus* result_status) {
  const MonoDelta max_staleness_delta =
      MonoDelta::FromSeconds(config_.max_staleness_interval_sec);
  MonoTime staleness_start = MonoTime::Now();
  bool is_timed_out = false;
  bool resync_state = false;
  while (!is_timed_out) {
    if (resync_state) {
      resync_state = false;
      MonoDelta staleness_delta = MonoTime::Now() - staleness_start;
      if (staleness_delta > max_staleness_delta) {
        LOG(INFO) << Substitute("detected a staleness period of $0",
                                staleness_delta.ToString());
        return Status::Incomplete(Substitute(
            "stalled with no progress for more than $0 seconds, aborting",
            max_staleness_delta.ToString()));
      }
      // The actual re-synchronization happens during GetNextMoves() below:
      // updated info is collected from the cluster and fed into the algorithm.
      LOG(INFO) << "re-synchronizing cluster state";
    }

    bool has_more_moves = false;
    RETURN_NOT_OK(runner->GetNextMoves(&has_more_moves));
    if (!has_more_moves) {
      // No moves are left, done!
      break;
    }

    auto has_errors = false;
    while (!is_timed_out) {
      auto is_scheduled = runner->ScheduleNextMove(&has_errors, &is_timed_out);
      resync_state |= has_errors;
      if (resync_state || is_timed_out) {
        break;
      }
      if (is_scheduled) {
        // Reset the start of the staleness interval: there was some progress
        // in scheduling new move operations.
        staleness_start = MonoTime::Now();

        // Continue scheduling available move operations while there is enough
        // capacity, i.e. until number of pending move operations on every
        // involved tablet server reaches max_moves_per_server. Once no more
        // operations can be scheduled, it's time to check for their status.
        continue;
      }

      // Poll for the status of pending operations. If some of the in-flight
      // operations are complete, it might be possible to schedule new ones
      // by calling Runner::ScheduleNextMove().
      auto has_updates = runner->UpdateMovesInProgressStatus(&has_errors,
                                                             &is_timed_out);
      if (has_updates) {
        // Reset the start of the staleness interval: there was some updates
        // on the status of scheduled move operations.
        staleness_start = MonoTime::Now();
      }
      resync_state |= has_errors;
      if (resync_state || is_timed_out || !has_updates) {
        // If there were errors while trying to get the statuses of pending
        // operations it's necessary to re-synchronize the state of the cluster:
        // most likely something has changed, so it's better to get a new set
        // of planned moves.
        break;
      }

      // Sleep a bit before going next cycle of status polling.
      SleepFor(MonoDelta::FromMilliseconds(200));
    }
  }

  *result_status = is_timed_out ? RunStatus::TIMED_OUT
                                : RunStatus::CLUSTER_IS_BALANCED;
  return Status::OK();
}

Status Rebalancer::GetClusterRawInfo(const boost::optional<string>& location,
                                     ClusterRawInfo* raw_info) {
  RETURN_NOT_OK(RefreshKsckResults());
  return KsckResultsToClusterRawInfo(location, ksck_->results(), raw_info);
}

Status Rebalancer::RefreshKsckResults() {
  shared_ptr<KsckCluster> cluster;
  RETURN_NOT_OK_PREPEND(
      RemoteKsckCluster::Build(config_.master_addresses, &cluster),
      "unable to build KsckCluster");
  cluster->set_table_filters(config_.table_filters);
  ksck_.reset(new Ksck(cluster));
  ignore_result(ksck_->Run());
  return Status::OK();
}

Status Rebalancer::CheckRemovingBLTserversSafe(const ClusterRawInfo& raw_info,
                                               const ClusterInfo& info) const {
  const auto& blacklist_tservers_info = info.blacklist_tservers;
  if (blacklist_tservers_info.empty()) {
    return Status::OK();
  }

  // Determine whether replicas on blacklist tservers can be moved.
  // If move all replicas on blacklist tservers to other servers,
  // the number of remaining servers should be greater than
  // the maximum number of replication_factor of tables.
  const size_t blacklist_tserver_count = blacklist_tservers_info.size();
  const size_t total_tserver_count =  raw_info.tserver_summaries.size();
  size_t max_blacklist_tserver_count = total_tserver_count;
  for (const auto& table_summary : raw_info.table_summaries) {
    max_blacklist_tserver_count =
    (max_blacklist_tserver_count > (total_tserver_count - table_summary.replication_factor))
    ? (total_tserver_count - table_summary.replication_factor)
    : max_blacklist_tserver_count;
  }
  if (blacklist_tserver_count > max_blacklist_tserver_count) {
    return Status::InvalidArgument(Substitute(
      "The number of blacklist tservers cannot exceed the threshold $0.",
      max_blacklist_tserver_count));
  }
  return Status::OK();
}

Rebalancer::BaseRunner::BaseRunner(Rebalancer* rebalancer,
                                   std::unordered_set<std::string> ignored_tservers,
                                   size_t max_moves_per_server,
                                   boost::optional<MonoTime> deadline)
    : rebalancer_(rebalancer),
      ignored_tservers_(std::move(ignored_tservers)),
      max_moves_per_server_(max_moves_per_server),
      deadline_(std::move(deadline)),
      moves_count_(0) {
  CHECK(rebalancer_);
}

Status Rebalancer::BaseRunner::Init(vector<string> master_addresses) {
  DCHECK_EQ(0, moves_count_);
  DCHECK(op_count_per_ts_.empty());
  DCHECK(ts_per_op_count_.empty());
  DCHECK(master_addresses_.empty());
  DCHECK(client_.get() == nullptr);
  master_addresses_ = std::move(master_addresses);
  return KuduClientBuilder()
      .master_server_addrs(master_addresses_)
      .Build(&client_);
}

Status Rebalancer::BaseRunner::GetNextMoves(bool* has_moves) {
  vector<ReplicaMove> replica_moves;
  RETURN_NOT_OK(GetNextMovesImpl(&replica_moves));
  if (replica_moves.empty() && scheduled_moves_.empty()) {
    *has_moves = false;
    return Status::OK();
  }

  // The GetNextMovesImpl() method prescribes replica movements using simplified
  // logic that doesn't know about best practices of safe and robust Raft
  // configuration changes. Here it's necessary to filter out moves for tablets
  // which already have operations in progress. The idea is simple: don't start
  // another operation for a tablet when there is still a pending operation
  // for that tablet.
  Rebalancer::FilterMoves(scheduled_moves_, &replica_moves);
  LoadMoves(std::move(replica_moves));

  // TODO(aserbin): this method reports on availability of move operations
  //                via the 'has_moves' parameter even if all of those were
  //                actually filtered out by the FilterMoves() method.
  //                Would it be more convenient to report only on the new,
  //                not-yet-in-progress operations and check for the presence
  //                of the scheduled moves at the upper level?
  *has_moves = true;
  return Status::OK();
}

void Rebalancer::BaseRunner::UpdateOnMoveCompleted(const string& ts_uuid) {
  const auto op_count = op_count_per_ts_[ts_uuid]--;
  const auto op_range = ts_per_op_count_.equal_range(op_count);
  bool ts_per_op_count_updated = false;
  for (auto it = op_range.first; it != op_range.second; ++it) {
    if (it->second == ts_uuid) {
      ts_per_op_count_.erase(it);
      ts_per_op_count_.emplace(op_count - 1, ts_uuid);
      ts_per_op_count_updated = true;
      break;
    }
  }
  DCHECK(ts_per_op_count_updated);
}

Rebalancer::AlgoBasedRunner::AlgoBasedRunner(
    Rebalancer* rebalancer,
    std::unordered_set<std::string> ignored_tservers,
    size_t max_moves_per_server,
    boost::optional<MonoTime> deadline)
    : BaseRunner(rebalancer,
                 std::move(ignored_tservers),
                 max_moves_per_server,
                 std::move(deadline)),
      random_generator_(random_device_()) {
}

Status Rebalancer::AlgoBasedRunner::Init(vector<string> master_addresses) {
  DCHECK(src_op_indices_.empty());
  DCHECK(dst_op_indices_.empty());
  DCHECK(scheduled_moves_.empty());
  return BaseRunner::Init(std::move(master_addresses));
}

void Rebalancer::AlgoBasedRunner::LoadMoves(vector<ReplicaMove> replica_moves) {
  // The moves to schedule (used by subsequent calls to ScheduleNextMove()).
  replica_moves_.swap(replica_moves);

  // Prepare helper containers.
  src_op_indices_.clear();
  dst_op_indices_.clear();
  op_count_per_ts_.clear();
  ts_per_op_count_.clear();

  // If there are any scheduled moves, it's necessary to count them in
  // to properly handle the 'maximum moves per server' constraint.
  unordered_map<string, int32_t> ts_pending_op_count;
  for (auto it = scheduled_moves_.begin(); it != scheduled_moves_.end(); ++it) {
    ++ts_pending_op_count[it->second.ts_uuid_from];
    ++ts_pending_op_count[it->second.ts_uuid_to];
  }

  // These two references is to make the compiler happy with the lambda below.
  auto& op_count_per_ts = op_count_per_ts_;
  auto& ts_per_op_count = ts_per_op_count_;
  const auto set_op_count = [&ts_pending_op_count,
      &op_count_per_ts, &ts_per_op_count](const string& ts_uuid) {
    auto it = ts_pending_op_count.find(ts_uuid);
    if (it == ts_pending_op_count.end()) {
      // No operations for tablet server ts_uuid yet.
      if (op_count_per_ts.emplace(ts_uuid, 0).second) {
        ts_per_op_count.emplace(0, ts_uuid);
      }
    } else {
      // There are pending operations for tablet server ts_uuid: set the number
      // operations at the tablet server ts_uuid as calculated above with
      // ts_pending_op_count.
      if (op_count_per_ts.emplace(ts_uuid, it->second).second) {
        ts_per_op_count.emplace(it->second, ts_uuid);
      }
      // Once set into op_count_per_ts and ts_per_op_count, this information
      // is no longer needed. In addition, these elements are removed to leave
      // only pending operations those do not intersect with the batch of newly
      // loaded operations.
      ts_pending_op_count.erase(it);
    }
  };

  // Process move operations from the batch of newly loaded ones.
  for (size_t i = 0; i < replica_moves_.size(); ++i) {
    const auto& elem = replica_moves_[i];
    src_op_indices_.emplace(elem.ts_uuid_from, set<size_t>()).first->
        second.emplace(i);
    set_op_count(elem.ts_uuid_from);

    dst_op_indices_.emplace(elem.ts_uuid_to, set<size_t>()).first->
        second.emplace(i);
    set_op_count(elem.ts_uuid_to);
  }

  // Process pending/scheduled move operations which do not intersect
  // with the batch of newly loaded ones.
  for (const auto& elem : ts_pending_op_count) {
    auto op_inserted = op_count_per_ts.emplace(elem.first, elem.second).second;
    DCHECK(op_inserted);
    ts_per_op_count.emplace(elem.second, elem.first);
  }
}

bool Rebalancer::AlgoBasedRunner::ScheduleNextMove(bool* has_errors,
                                                   bool* timed_out) {
  DCHECK(has_errors);
  DCHECK(timed_out);
  *has_errors = false;
  *timed_out = false;

  if (deadline_ && MonoTime::Now() >= *deadline_) {
    *timed_out = true;
    return false;
  }

  // Scheduling one operation per step. Once operation is scheduled, it's
  // necessary to update the ts_per_op_count_ container right after scheduling
  // to avoid oversubscribing of the tablet servers.
  size_t op_idx;
  if (!FindNextMove(&op_idx)) {
    // Nothing to schedule yet: unfruitful outcome. Need to wait until there is
    // an available slot at a tablet server.
    return false;
  }

  // Try to schedule next move operation.
  DCHECK_LT(op_idx, replica_moves_.size());
  const auto& info = replica_moves_[op_idx];
  const auto& tablet_id = info.tablet_uuid;
  const auto& src_ts_uuid = info.ts_uuid_from;
  const auto& dst_ts_uuid = info.ts_uuid_to;

  Status s = ScheduleReplicaMove(master_addresses_, client_,
                                 tablet_id, src_ts_uuid, dst_ts_uuid);
  if (s.ok()) {
    UpdateOnMoveScheduled(op_idx, tablet_id, src_ts_uuid, dst_ts_uuid, true);
    LOG(INFO) << Substitute("tablet $0: '$1' -> '$2' move scheduled",
                            tablet_id, src_ts_uuid, dst_ts_uuid);
    // Successfully scheduled move operation.
    return true;
  }

  DCHECK(!s.ok());
  // The source replica is not found in the tablet's consensus config
  // or the tablet does not exit anymore. The replica might already
  // moved because of some other concurrent activity, e.g.
  // re-replication, another rebalancing session in progress, etc.
  LOG(INFO) << Substitute("tablet $0: '$1' -> '$2' move ignored: $3",
                          tablet_id, src_ts_uuid, dst_ts_uuid, s.ToString());
  UpdateOnMoveScheduled(op_idx, tablet_id, src_ts_uuid, dst_ts_uuid, false);
  // Failed to schedule move operation due to an error.
  *has_errors = true;
  return false;
}

bool Rebalancer::AlgoBasedRunner::UpdateMovesInProgressStatus(
    bool* has_errors, bool* timed_out) {
  DCHECK(has_errors);
  DCHECK(timed_out);
  *has_errors = false;
  *timed_out = false;

  // Update the statuses of the in-progress move operations.
  auto has_updates = false;
  auto error_count = 0;
  for (auto it = scheduled_moves_.begin(); it != scheduled_moves_.end(); ) {
    if (deadline_ && MonoTime::Now() >= *deadline_) {
      *timed_out = true;
      break;
    }
    const auto& tablet_id = it->first;
    DCHECK_EQ(tablet_id, it->second.tablet_uuid);
    const auto& src_ts_uuid = it->second.ts_uuid_from;
    const auto& dst_ts_uuid = it->second.ts_uuid_to;
    auto is_complete = false;
    Status move_status;
    const Status s = CheckCompleteMove(master_addresses_, client_,
                                       tablet_id, src_ts_uuid, dst_ts_uuid,
                                       &is_complete, &move_status);
    has_updates |= s.ok();
    if (!s.ok()) {
      // There was an error while fetching the status of this move operation.
      // Since the actual status of the move is not known, don't update the
      // stats on pending operations per server. The higher-level should handle
      // this situation after returning from this method, re-synchronizing
      // the state of the cluster.
      ++error_count;
      LOG(INFO) << Substitute("tablet $0: '$1' -> '$2' move is abandoned: $3",
                              tablet_id, src_ts_uuid, dst_ts_uuid, s.ToString());
      // Erase the element and advance the iterator.
      it = scheduled_moves_.erase(it);
      continue;
    }
    DCHECK(s.ok());
    if (is_complete) {
      // The move has completed (success or failure): update the stats on the
      // pending operations per server.
      ++moves_count_;
      UpdateOnMoveCompleted(it->second.ts_uuid_from);
      UpdateOnMoveCompleted(it->second.ts_uuid_to);
      LOG(INFO) << Substitute("tablet $0: '$1' -> '$2' move completed: $3",
                              tablet_id, src_ts_uuid, dst_ts_uuid,
                              move_status.ToString());
      // Erase the element and advance the iterator.
      it = scheduled_moves_.erase(it);
      continue;
    }
    // There was an update on the status of the move operation and it hasn't
    // completed yet. Let's poll for the status of the rest.
    ++it;
  }
  *has_errors = (error_count != 0);
  return has_updates;
}

// Run one step of the rebalancer. Due to the inherent restrictions of the
// rebalancing engine, no more than one replica per tablet is moved during
// one step of the rebalancing.
Status Rebalancer::AlgoBasedRunner::GetNextMovesImpl(
    vector<ReplicaMove>* replica_moves) {
  const auto& loc = location();
  ClusterRawInfo raw_info;
  RETURN_NOT_OK(rebalancer_->GetClusterRawInfo(loc, &raw_info));

  // For simplicity, allow to run the rebalancing only when all tablet servers
  // are in good shape (except those specified in 'ignored_tservers_').
  // Otherwise, the rebalancing might interfere with the
  // automatic re-replication or get unexpected errors while moving replicas.
  for (const auto& s : raw_info.tserver_summaries) {
    if (s.health != KsckServerHealth::HEALTHY) {
      if (ContainsKey(ignored_tservers_, s.uuid)) {
        continue;
      }
      return Status::IllegalState(
          Substitute("tablet server $0 ($1): unacceptable health status $2",
                     s.uuid, s.address, ServerHealthToString(s.health)));
    }
  }

  TabletsPlacementInfo tpi;
  if (!loc) {
    RETURN_NOT_OK(BuildTabletsPlacementInfo(raw_info, scheduled_moves_, &tpi));
  }

  // Build 'tablet_id' --> 'target tablet replication factor' map.
  struct TabletExtraInfo {
    int replication_factor;
    int num_voters;
  };
  unordered_map<string, TabletExtraInfo> extra_info_by_tablet_id;
  {
    unordered_map<string, int> replication_factors_by_table;
    for (const auto& s : raw_info.table_summaries) {
      EmplaceOrDie(&replication_factors_by_table, s.id, s.replication_factor);
    }
    for (const auto& s : raw_info.tablet_summaries) {
      int num_voters = 0;
      for (const auto& rs : s.replicas) {
        if (rs.is_voter) {
          ++num_voters;
        }
      }
      const auto rf = FindOrDie(replication_factors_by_table, s.table_id);
      EmplaceOrDie(&extra_info_by_tablet_id,
                   s.id, TabletExtraInfo{rf, num_voters});
    }
  }

  // The number of operations to output by the algorithm. Those will be
  // translated into concrete tablet replica movement operations, the output of
  // this method.
  const size_t max_moves = max_moves_per_server_ *
      raw_info.tserver_summaries.size() * 5;

  replica_moves->clear();
  vector<TableReplicaMove> moves;
  ClusterInfo cluster_info;
  RETURN_NOT_OK(rebalancer_->BuildClusterInfo(
      raw_info, scheduled_moves_, &cluster_info));
  RETURN_NOT_OK(rebalancer_->CheckRemovingBLTserversSafe(raw_info, cluster_info));
  RETURN_NOT_OK(algorithm()->MoveReplicasFromBLTservers(&cluster_info, &moves));
  RETURN_NOT_OK(algorithm()->GetNextMoves(cluster_info, max_moves, &moves));
  if (moves.empty()) {
    // No suitable moves were found: the cluster described by the 'cluster_info'
    // is balanced, assuming the pending moves, if any, will succeed.
    return Status::OK();
  }
  unordered_set<string> tablets_in_move;
  transform(scheduled_moves_.begin(), scheduled_moves_.end(),
            inserter(tablets_in_move, tablets_in_move.begin()),
            [](const MovesInProgress::value_type& elem) {
              return elem.first;
            });
  bool fix_move = false;
  for (auto& move : moves) {
    vector<string> tablet_ids;
    bool is_healthy_move = true;
    RETURN_NOT_OK(FindReplicas(move, raw_info, &tablet_ids, &is_healthy_move));
    if (!is_healthy_move) {
      LOG(WARNING) << Substitute(
          "No healthy replica of table $0 on server $1.", move.table_id, move.from);
      continue;
    }
    if (ContainsKey(cluster_info.blacklist_tservers, move.from) && tablet_ids.empty()) {
      // Some replica(s) at source server could not move to destination server and
      // source server is in blacklist, it is necessary to find another destination
      // server for all replicas at blacklist servers should be moved.
      fix_move = true;
      while (tablet_ids.empty()) {
        LOG(WARNING) << Substitute(
            "table $0: could not find any suitable replica to move from "
            "blacklist server $1 to server $2, retry to find another destination server",
            move.table_id, move.from, move.to);
        vector<string> server_uuids;
        std::transform(raw_info.tserver_summaries.begin(), raw_info.tserver_summaries.end(),
                       back_inserter(server_uuids),
                       [](const KsckServerHealthSummary& elem) {
                         return elem.uuid;
                       });
        shuffle(server_uuids.begin(), server_uuids.end(), random_generator_);
        for (const auto& elem : server_uuids) {
          if (elem != move.to && !ContainsKey(cluster_info.blacklist_tservers, elem)) {
            LOG(INFO) << Substitute("choose destination server: $0",elem);
            move.to = elem;
            break;
          }
        }
        RETURN_NOT_OK(FindReplicas(move, raw_info, &tablet_ids, &is_healthy_move));
      }
    }
    if (!loc) {
      // In case of cross-location (a.k.a. inter-location) rebalancing it is
      // necessary to make sure the majority of replicas would not end up
      // at the same location after the move. If so, remove those tablets
      // from the list of candidates.
      RETURN_NOT_OK(FilterCrossLocationTabletCandidates(
          cluster_info.locality.location_by_ts_id, tpi, move, &tablet_ids));
    }
    // Shuffle the set of the tablet identifiers: that's to achieve even spread
    // of moves across tables with the same skew.
    std::shuffle(tablet_ids.begin(), tablet_ids.end(), random_generator_);
    string move_tablet_id;
    for (const auto& tablet_id : tablet_ids) {
      if (!ContainsKey(tablets_in_move, tablet_id)) {
        // For now, choose the very first tablet that does not have replicas
        // in move. Later on, additional logic might be added to find
        // the best candidate.
        move_tablet_id = tablet_id;
        break;
      }
    }
    if (move_tablet_id.empty()) {
      LOG(WARNING) << Substitute(
          "table $0: could not find any suitable replica to move "
          "from server $1 to server $2", move.table_id, move.from, move.to);
      continue;
    }
    ReplicaMove move_info;
    move_info.tablet_uuid = move_tablet_id;
    move_info.ts_uuid_from = move.from;
    const auto& extra_info = FindOrDie(extra_info_by_tablet_id, move_tablet_id);
    if (extra_info.replication_factor < extra_info.num_voters) {
      // The number of voter replicas is greater than the target replication
      // factor. It might happen the replica distribution would be better
      // if just removing the source replica. Anyway, once a replica is removed,
      // the system will automatically add a new one, if needed, where the new
      // replica will be placed to have balanced replica distribution.
      move_info.ts_uuid_to = "";
    } else {
      move_info.ts_uuid_to = move.to;
    }
    replica_moves->emplace_back(std::move(move_info));
    // Mark the tablet as 'has a replica in move'.
    tablets_in_move.emplace(move_tablet_id);
    if (fix_move) {
      // Some move operation has been fixed， ignore subsequent moves.
      break;
    }
  }

  return Status::OK();
}

bool Rebalancer::AlgoBasedRunner::FindNextMove(size_t* op_idx) {
  vector<size_t> op_indices;
  for (auto it = ts_per_op_count_.begin(); op_indices.empty() &&
       it != ts_per_op_count_.end() && it->first < max_moves_per_server_; ++it) {
    const auto& uuid_0 = it->second;

    auto it_1 = it;
    ++it_1;
    for (; op_indices.empty() && it_1 != ts_per_op_count_.end() &&
         it_1->first < max_moves_per_server_; ++it_1) {
      const auto& uuid_1 = it_1->second;

      // Check for available operations where uuid_0, uuid_1 would be
      // source or destination servers correspondingly.
      {
        const auto it_src = src_op_indices_.find(uuid_0);
        const auto it_dst = dst_op_indices_.find(uuid_1);
        if (it_src != src_op_indices_.end() &&
            it_dst != dst_op_indices_.end()) {
          set_intersection(it_src->second.begin(), it_src->second.end(),
                           it_dst->second.begin(), it_dst->second.end(),
                           back_inserter(op_indices));
        }
      }
      // It's enough to find just one move.
      if (!op_indices.empty()) {
        break;
      }
      {
        const auto it_src = src_op_indices_.find(uuid_1);
        const auto it_dst = dst_op_indices_.find(uuid_0);
        if (it_src != src_op_indices_.end() &&
            it_dst != dst_op_indices_.end()) {
          set_intersection(it_src->second.begin(), it_src->second.end(),
                           it_dst->second.begin(), it_dst->second.end(),
                           back_inserter(op_indices));
        }
      }
    }
  }
  if (!op_indices.empty() && op_idx) {
    *op_idx = op_indices.front();
  }
  return !op_indices.empty();
}

void Rebalancer::AlgoBasedRunner::UpdateOnMoveScheduled(
    size_t idx,
    const string& tablet_uuid,
    const string& src_ts_uuid,
    const string& dst_ts_uuid,
    bool is_success) {
  if (is_success) {
    Rebalancer::ReplicaMove move_info = { tablet_uuid, src_ts_uuid, dst_ts_uuid };
    scheduled_moves_.emplace(tablet_uuid, std::move(move_info));
    // Only one replica of a tablet can be moved at a time.
    // TODO(aserbin): clarify on duplicates
    //DCHECK(ins.second);
  }
  UpdateOnMoveScheduledImpl(idx, src_ts_uuid, is_success, &src_op_indices_);
  UpdateOnMoveScheduledImpl(idx, dst_ts_uuid, is_success, &dst_op_indices_);
}

void Rebalancer::AlgoBasedRunner::UpdateOnMoveScheduledImpl(
    size_t idx,
    const string& ts_uuid,
    bool is_success,
    std::unordered_map<std::string, std::set<size_t>>* op_indices) {
  DCHECK(op_indices);
  auto& indices = (*op_indices)[ts_uuid];
  auto erased = indices.erase(idx);
  DCHECK_EQ(1, erased);
  if (indices.empty()) {
    op_indices->erase(ts_uuid);
  }
  if (is_success) {
    const auto op_count = op_count_per_ts_[ts_uuid]++;
    const auto op_range = ts_per_op_count_.equal_range(op_count);
    bool ts_op_count_updated = false;
    for (auto it = op_range.first; it != op_range.second; ++it) {
      if (it->second == ts_uuid) {
        ts_per_op_count_.erase(it);
        ts_per_op_count_.emplace(op_count + 1, ts_uuid);
        ts_op_count_updated = true;
        break;
      }
    }
    DCHECK(ts_op_count_updated);
  }
}

Rebalancer::IntraLocationRunner::IntraLocationRunner(
    Rebalancer* rebalancer,
    std::unordered_set<std::string> ignored_tservers,
    size_t max_moves_per_server,
    boost::optional<MonoTime> deadline,
    std::string location)
    : AlgoBasedRunner(rebalancer,
                      std::move(ignored_tservers),
                      max_moves_per_server,
                      std::move(deadline)),
      location_(std::move(location)) {
}

Rebalancer::CrossLocationRunner::CrossLocationRunner(Rebalancer* rebalancer,
    std::unordered_set<std::string> ignored_tservers,
    size_t max_moves_per_server,
    double load_imbalance_threshold,
    boost::optional<MonoTime> deadline)
    : AlgoBasedRunner(rebalancer,
                      std::move(ignored_tservers),
                      max_moves_per_server,
                      std::move(deadline)),
      algorithm_(load_imbalance_threshold) {
}

Rebalancer::PolicyFixer::PolicyFixer(
    Rebalancer* rebalancer,
    std::unordered_set<std::string> ignored_tservers,
    size_t max_moves_per_server,
    boost::optional<MonoTime> deadline)
    : BaseRunner(rebalancer,
                 std::move(ignored_tservers),
                 max_moves_per_server,
                 std::move(deadline)) {
}

Status Rebalancer::PolicyFixer::Init(vector<string> master_addresses) {
  DCHECK(moves_to_schedule_.empty());
  return BaseRunner::Init(std::move(master_addresses));
}

void Rebalancer::PolicyFixer::LoadMoves(
    vector<ReplicaMove> replica_moves) {
  // Replace the list of moves operations to schedule. Even if it's not empty,
  // some elements of it might be irrelevant anyway, so there is no need to
  // keep any since the new information is the most up-to-date. The input list
  // is already filtered and should not contain any operations which are
  // tracked as already scheduled ones.
  moves_to_schedule_.clear();

  for (auto& move_info : replica_moves) {
    auto ts_uuid = move_info.ts_uuid_from;
    DCHECK(!ts_uuid.empty());
    moves_to_schedule_.emplace(std::move(ts_uuid), std::move(move_info));
  }

  // Refresh the helper containers.
  for (const auto& elem : moves_to_schedule_) {
    const auto& ts_uuid = elem.first;
    DCHECK(!ts_uuid.empty());
    if (op_count_per_ts_.emplace(ts_uuid, 0).second) {
      // No operations for tablet server ts_uuid: add ts_per_op_count_ entry.
      ts_per_op_count_.emplace(0, ts_uuid);
    }
  }
}

bool Rebalancer::PolicyFixer::ScheduleNextMove(bool* has_errors,
                                               bool* timed_out) {
  DCHECK(has_errors);
  DCHECK(timed_out);
  *has_errors = false;
  *timed_out = false;

  if (deadline_ && MonoTime::Now() >= *deadline_) {
    *timed_out = true;
    return false;
  }

  ReplicaMove move_info;
  if (!FindNextMove(&move_info)) {
    return false;
  }

  // Find a move that's doesn't have its tserver UUID in scheduled_moves_.
  const auto s = SetReplace(client_,
                            move_info.tablet_uuid,
                            move_info.ts_uuid_from,
                            move_info.config_opid_idx);
  if (!s.ok()) {
    *has_errors = true;
    return false;
  }

  // Remove the element from moves_to_schedule_.
  bool erased = false;
  auto range = moves_to_schedule_.equal_range(move_info.ts_uuid_from);
  for (auto it = range.first; it != range.second; ++it) {
    if (move_info.tablet_uuid == it->second.tablet_uuid) {
      moves_to_schedule_.erase(it);
      erased = true;
      break;
    }
  }
  CHECK(erased) << Substitute("T $0 P $1: move information not found",
                              move_info.tablet_uuid, move_info.ts_uuid_from);
  LOG(INFO) << Substitute("tablet $0: '$1' -> '?' move scheduled",
                          move_info.tablet_uuid, move_info.ts_uuid_from);
  // Add information on scheduled move into the scheduled_moves_.
  // Only one replica of a tablet can be moved at a time.
  auto tablet_uuid = move_info.tablet_uuid;
  EmplaceOrDie(&scheduled_moves_, std::move(tablet_uuid), std::move(move_info));
  return true;
}

bool Rebalancer::PolicyFixer::UpdateMovesInProgressStatus(
    bool* has_errors, bool* timed_out) {
  DCHECK(has_errors);
  DCHECK(timed_out);

  auto has_updates = false;
  auto error_count = 0;
  auto out_of_time = false;
  for (auto it = scheduled_moves_.begin(); it != scheduled_moves_.end(); ) {
    if (deadline_ && MonoTime::Now() >= *deadline_) {
      out_of_time = true;
      break;
    }
    bool is_complete;
    Status completion_status;
    const auto& tablet_id = it->second.tablet_uuid;
    const auto& ts_uuid = it->second.ts_uuid_from;
    auto s = CheckCompleteReplace(client_, tablet_id, ts_uuid,
                                  &is_complete, &completion_status);
    if (!s.ok()) {
      // Update on the movement status has failed: remove the move operation
      // as if it didn't exist. Once the cluster status is re-synchronized,
      // the corresponding operation will be scheduled again, if needed.
      ++error_count;
      LOG(INFO) << Substitute("tablet $0: '$1' -> '?' move is abandoned: $2",
                              tablet_id, ts_uuid, s.ToString());
      it = scheduled_moves_.erase(it);
      continue;
    }
    DCHECK(s.ok());
    if (is_complete) {
      // The replacement has completed (success or failure): update the stats
      // on the pending operations per server.
      ++moves_count_;
      has_updates = true;
      LOG(INFO) << Substitute("tablet $0: '$1' -> '?' move completed: $2",
                              tablet_id, ts_uuid, completion_status.ToString());
      UpdateOnMoveCompleted(ts_uuid);
      it = scheduled_moves_.erase(it);
      continue;
    }
    ++it;
  }

  *timed_out = out_of_time;
  *has_errors = (error_count > 0);
  return has_updates;
}

Status Rebalancer::PolicyFixer::GetNextMovesImpl(
    vector<ReplicaMove>* replica_moves) {
  ClusterRawInfo raw_info;
  RETURN_NOT_OK(rebalancer_->GetClusterRawInfo(boost::none, &raw_info));

  // For simplicity, allow to run the rebalancing only when all tablet servers
  // are in good shape (except those specified in 'ignored_tservers_').
  // Otherwise, the rebalancing might interfere with the
  // automatic re-replication or get unexpected errors while moving replicas.
  // TODO(aserbin): move it somewhere else?
  for (const auto& s : raw_info.tserver_summaries) {
    if (s.health != KsckServerHealth::HEALTHY) {
      if (ContainsKey(ignored_tservers_, s.uuid)) {
        continue;
      }
      return Status::IllegalState(
          Substitute("tablet server $0 ($1): unacceptable health status $2",
                     s.uuid, s.address, ServerHealthToString(s.health)));
    }
  }
  ClusterInfo ci;
  RETURN_NOT_OK(rebalancer_->BuildClusterInfo(raw_info, scheduled_moves_, &ci));

  TabletsPlacementInfo placement_info;
  RETURN_NOT_OK(
      BuildTabletsPlacementInfo(raw_info, scheduled_moves_, &placement_info));

  vector<PlacementPolicyViolationInfo> ppvi;
  RETURN_NOT_OK(DetectPlacementPolicyViolations(placement_info, &ppvi));

  // Filter out all reported violations which are already taken care of.
  // The idea is to have not more than one pending operation per tablet.
  {
    decltype(ppvi) ppvi_filtered;
    for (auto& info : ppvi) {
      if (ContainsKey(scheduled_moves_, info.tablet_id)) {
        continue;
      }
      ppvi_filtered.emplace_back(std::move(info));
    }
    ppvi = std::move(ppvi_filtered);
  }

  RETURN_NOT_OK(FindMovesToReimposePlacementPolicy(
      placement_info, ci.locality, ppvi, replica_moves));

  if (PREDICT_FALSE(VLOG_IS_ON(1))) {
    for (const auto& info : ppvi) {
      VLOG(1) << Substitute("policy violation at location '$0': tablet $1",
                            info.majority_location, info.tablet_id);
    }
    for (const auto& move : *replica_moves) {
      VLOG(1) << Substitute("policy fix for tablet $0: replica to remove $1",
                            move.tablet_uuid, move.ts_uuid_from);
    }
  }

  return Status::OK();
}

bool Rebalancer::PolicyFixer::FindNextMove(ReplicaMove* move) {
  DCHECK(move);
  // TODO(aserbin): use pessimistic /2 limit for max_moves_per_servers_
  // since the desitnation servers for the move of the replica marked with
  // the REPLACE attribute is not known.

  // Load the least loaded (in terms of scheduled moves) tablet servers first.
  for (const auto& elem : ts_per_op_count_) {
    const auto& ts_uuid = elem.second;
    if (FindCopy(moves_to_schedule_, ts_uuid, move)) {
      return true;
    }
  }
  return false;
}

} // namespace tools
} // namespace kudu
