/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "replica/database_fwd.hh"
#include "locator/host_id.hh"
#include "locator/load_sketch.hh"
#include "locator/tablets.hh"
#include "tablet_allocator_fwd.hh"
#include "locator/token_metadata_fwd.hh"

namespace service {

using tablet_migration_info = locator::tablet_migration_info;

/// Represents intention to emit resize (split or merge) request for a
/// table, and finalize or revoke the request previously initiated.
struct table_resize_plan {
    std::unordered_map<table_id, locator::resize_decision> resize;
    std::unordered_set<table_id> finalize_resize;

    size_t size() const { return resize.size() + finalize_resize.size(); }

    void merge(table_resize_plan&& other) {
        for (auto&& [id, other_resize] : other.resize) {
            if (!resize.contains(id) || other_resize.sequence_number > resize[id].sequence_number) {
                resize[id] = std::move(other_resize);
            }
        }
        finalize_resize.merge(std::move(other.finalize_resize));
    }
};

class migration_plan {
public:
    using migrations_vector = utils::chunked_vector<tablet_migration_info>;
private:
    migrations_vector _migrations;
    table_resize_plan _resize_plan;
    bool _has_nodes_to_drain = false;
public:
    /// Returns true iff there are decommissioning nodes which own some tablet replicas.
    bool has_nodes_to_drain() const { return _has_nodes_to_drain; }

    const migrations_vector& migrations() const { return _migrations; }
    bool empty() const { return _migrations.empty() && !_resize_plan.size(); }
    size_t size() const { return _migrations.size() + _resize_plan.size(); }
    size_t tablet_migration_count() const { return _migrations.size(); }
    size_t resize_decision_count() const { return _resize_plan.size(); }

    void add(tablet_migration_info info) {
        _migrations.emplace_back(std::move(info));
    }

    void merge(migration_plan&& other) {
        std::move(other._migrations.begin(), other._migrations.end(), std::back_inserter(_migrations));
        _has_nodes_to_drain |= other._has_nodes_to_drain;
        _resize_plan.merge(std::move(other._resize_plan));
    }

    void set_has_nodes_to_drain(bool b) {
        _has_nodes_to_drain = b;
    }

    const table_resize_plan& resize_plan() const { return _resize_plan; }

    void set_resize_plan(table_resize_plan resize_plan) {
        _resize_plan = std::move(resize_plan);
    }
};

class migration_notifier;

class tablet_allocator {
public:
    struct config {
        unsigned initial_tablets_scale = 1;
    };
    class impl {
    public:
        virtual ~impl() = default;
    };
private:
    std::unique_ptr<impl> _impl;
    tablet_allocator_impl& impl();
public:
    tablet_allocator(config cfg, service::migration_notifier& mn, replica::database& db);
public:
    future<> stop();

    /// Returns a tablet migration plan that aims to achieve better load balance in the whole cluster.
    /// The plan is computed based on information in the given token_metadata snapshot
    /// and thus should be executed and reflected, at least as pending tablet transitions, in token_metadata
    /// before this is called again.
    ///
    /// For any given global_tablet_id there is at most one tablet_migration_info in the returned plan.
    ///
    /// To achieve full balance, do:
    ///
    ///    while (true) {
    ///        auto plan = co_await balance_tablets(get_token_metadata());
    ///        if (plan.empty()) {
    ///            break;
    ///        }
    ///        co_await execute(plan);
    ///    }
    ///
    /// It is ok to invoke the algorithm with already active tablet migrations. The algorithm will take them into account
    /// when balancing the load as if they already succeeded. This means that applying a series of migration plans
    /// produced by this function will give the same result regardless of whether applying means they are fully executed or
    /// only initiated by creating corresponding transitions in tablet metadata.
    ///
    /// The algorithm takes care of limiting the streaming load on the system, also by taking active migrations into account.
    ///
    future<migration_plan> balance_tablets(locator::token_metadata_ptr, locator::load_stats_ptr = {});

    future<locator::tablet_map> split_tablets(locator::token_metadata_ptr, table_id);

    /// Should be called when the node is no longer a leader.
    void on_leadership_lost();
};

struct load_balancer_dc_stats {
    uint64_t calls = 0;
    uint64_t migrations_produced = 0;
    uint64_t migrations_skipped = 0;
    uint64_t tablets_skipped_node = 0;
    uint64_t tablets_skipped_rack = 0;
    uint64_t stop_balance = 0;
    uint64_t stop_load_inversion = 0;
    uint64_t stop_no_candidates = 0;
    uint64_t stop_skip_limit = 0;
    uint64_t stop_batch_size = 0;
};

struct load_balancer_node_stats {
    double load = 0;
};

struct load_balancer_cluster_stats {
    uint64_t resizes_emitted = 0;
    uint64_t resizes_revoked = 0;
    uint64_t resizes_finalized = 0;
};

using dc_name = sstring;

class load_balancer_stats_manager {
    sstring group_name;
    std::unordered_map<dc_name, std::unique_ptr<load_balancer_dc_stats>> _dc_stats;
    std::unordered_map<locator::host_id, std::unique_ptr<load_balancer_node_stats>> _node_stats;
    load_balancer_cluster_stats _cluster_stats;
    seastar::metrics::label dc_label{"target_dc"};
    seastar::metrics::label node_label{"target_node"};
    seastar::metrics::metric_groups _metrics;

    void setup_metrics(const dc_name& dc, load_balancer_dc_stats& stats) {
        namespace sm = seastar::metrics;
        auto dc_lb = dc_label(dc);
        _metrics.add_group(group_name, {
            sm::make_counter("calls", sm::description("number of calls to the load balancer"),
                             stats.calls)(dc_lb),
            sm::make_counter("migrations_produced", sm::description("number of migrations produced by the load balancer"),
                             stats.migrations_produced)(dc_lb),
            sm::make_counter("migrations_skipped", sm::description("number of migrations skipped by the load balancer due to load limits"),
                             stats.migrations_skipped)(dc_lb),
        });
    }

    void setup_metrics(const dc_name& dc, locator::host_id node, load_balancer_node_stats& stats) {
        namespace sm = seastar::metrics;
        auto dc_lb = dc_label(dc);
        auto node_lb = node_label(node);
        _metrics.add_group(group_name, {
            sm::make_gauge("load", sm::description("node load during last load balancing"),
                           stats.load)(dc_lb)(node_lb)
        });
    }

    void setup_metrics(load_balancer_cluster_stats& stats) {
        namespace sm = seastar::metrics;
        // FIXME: we can probably improve it by making it per resize type (split, merge or none).
        _metrics.add_group(group_name, {
            sm::make_counter("resizes_emitted", sm::description("number of resizes produced by the load balancer"),
                stats.resizes_emitted),
            sm::make_counter("resizes_revoked", sm::description("number of resizes revoked by the load balancer"),
                stats.resizes_revoked),
            sm::make_counter("resizes_finalized", sm::description("number of resizes finalized by the load balancer"),
                stats.resizes_finalized)
        });
    }
public:
    load_balancer_stats_manager(sstring group_name = "load_balancer"):
        group_name(std::move(group_name))
    {
        setup_metrics(_cluster_stats);
    }

    load_balancer_dc_stats& for_dc(const dc_name& dc) {
        auto it = _dc_stats.find(dc);
        if (it == _dc_stats.end()) {
            auto stats = std::make_unique<load_balancer_dc_stats>();
            setup_metrics(dc, *stats);
            it = _dc_stats.emplace(dc, std::move(stats)).first;
        }
        return *it->second;
    }

    load_balancer_node_stats& for_node(const dc_name& dc, locator::host_id node) {
        auto it = _node_stats.find(node);
        if (it == _node_stats.end()) {
            auto stats = std::make_unique<load_balancer_node_stats>();
            setup_metrics(dc, node, *stats);
            it = _node_stats.emplace(node, std::move(stats)).first;
        }
        return *it->second;
    }

    load_balancer_cluster_stats& for_cluster() {
        return _cluster_stats;
    }

    void unregister() {
        _metrics.clear();
    }
};

locator::tablet_replica_set
get_replicas_for_tablet_load(const locator::tablet_info& ti, const locator::tablet_transition_info* trinfo);

using global_shard_id = locator::tablet_replica;
using shard_id = seastar::shard_id;

// Represents metric for per-node load which we want to equalize between nodes.
// It's an average per-shard load in terms of tablet count.
using load_type = double;

struct shard_load {
    size_t tablet_count = 0;

    // Number of tablets which are streamed from this shard.
    size_t streaming_read_load = 0;

    // Number of tablets which are streamed to this shard.
    size_t streaming_write_load = 0;

    // Tablets which still have a replica on this shard which are candidates for migrating away from this shard.
    std::unordered_set<locator::global_tablet_id> candidates;

    future<> clear_gently() {
        return utils::clear_gently(candidates);
    }
};

struct node_load {

    locator::host_id id;
    uint64_t shard_count = 0;
    uint64_t tablet_count = 0;

    // The average shard load on this node.
    load_type avg_load = 0;

    std::vector<shard_id> shards_by_load; // heap which tracks most-loaded shards using shards_by_load_cmp().
    std::vector<shard_load> shards; // Indexed by shard_id to which a given shard_load corresponds.

    std::optional<locator::load_sketch> target_load_sketch;

    future<locator::load_sketch&> get_load_sketch(const locator::token_metadata_ptr& tm) {
        if (!target_load_sketch) {
            target_load_sketch.emplace(tm);
            co_await target_load_sketch->populate(id);
        }
        co_return *target_load_sketch;
    }

    // Call when tablet_count changes.
    void update() {
        avg_load = get_avg_load(tablet_count);
    }

    load_type get_avg_load(uint64_t tablets) const {
        return double(tablets) / shard_count;
    }

    auto shards_by_load_cmp() {
        return [this] (const auto& a, const auto& b) {
            return shards[a].tablet_count < shards[b].tablet_count;
        };
    }

    future<> clear_gently() {
        return utils::clear_gently(shards);
    }
};


}

template <>
struct fmt::formatter<service::tablet_migration_info> : fmt::formatter<std::string_view> {
    auto format(const service::tablet_migration_info&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
