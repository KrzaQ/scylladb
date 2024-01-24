/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/coroutine.hh>
#include "alter_keyspace_statement.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "db/system_keyspace.hh"
#include "data_dictionary/data_dictionary.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/ks_prop_defs.hh"
#include "create_keyspace_statement.hh"
#include "gms/feature_service.hh"

bool is_system_keyspace(std::string_view keyspace);

cql3::statements::alter_keyspace_statement::alter_keyspace_statement(sstring name, ::shared_ptr<ks_prop_defs> attrs)
    : _name(name)
    , _attrs(std::move(attrs))
{}

const sstring& cql3::statements::alter_keyspace_statement::keyspace() const {
    return _name;
}

future<> cql3::statements::alter_keyspace_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return state.has_keyspace_access(_name, auth::permission::ALTER);
}

void cql3::statements::alter_keyspace_statement::validate(query_processor& qp, const service::client_state& state) const {
        auto tmp = _name;
        std::transform(tmp.begin(), tmp.end(), tmp.begin(), ::tolower);
        if (is_system_keyspace(tmp)) {
            throw exceptions::invalid_request_exception("Cannot alter system keyspace");
        }

        _attrs->validate();

        if (!bool(_attrs->get_replication_strategy_class()) && !_attrs->get_replication_options().empty()) {
            throw exceptions::configuration_exception("Missing replication strategy class");
        }
        try {
            auto ks = qp.db().find_keyspace(_name);
            data_dictionary::storage_options current_options = ks.metadata()->get_storage_options();
            data_dictionary::storage_options new_options = _attrs->get_storage_options();
            if (!qp.proxy().features().keyspace_storage_options && !new_options.is_local_type()) {
                throw exceptions::invalid_request_exception("Keyspace storage options not supported in the cluster");
            }
            if (!current_options.can_update_to(new_options)) {
                throw exceptions::invalid_request_exception(format("Cannot alter storage options: {} to {} is not supported",
                        current_options.type_string(), new_options.type_string()));
            }

            auto new_ks = _attrs->as_ks_metadata_update(ks.metadata(), *qp.proxy().get_token_metadata_ptr(), qp.proxy().features());
            locator::replication_strategy_params params(new_ks->strategy_options(), new_ks->initial_tablets());
            auto new_rs = locator::abstract_replication_strategy::create_replication_strategy(new_ks->strategy_name(), params);
            if (new_rs->is_per_table() != ks.get_replication_strategy().is_per_table()) {
                throw exceptions::invalid_request_exception(format("Cannot alter replication strategy vnode/tablets flavor"));
            }
        } catch (const std::runtime_error& e) {
            throw exceptions::invalid_request_exception(e.what());
        }
#if 0
        // The strategy is validated through KSMetaData.validate() in announceKeyspaceUpdate below.
        // However, for backward compatibility with thrift, this doesn't validate unexpected options yet,
        // so doing proper validation here.
        AbstractReplicationStrategy.validateReplicationStrategy(name,
                                                                AbstractReplicationStrategy.getClass(attrs.getReplicationStrategyClass()),
                                                                StorageService.instance.getTokenMetadata(),
                                                                DatabaseDescriptor.getEndpointSnitch(),
                                                                attrs.getReplicationOptions());
#endif
}

#include "locator/load_sketch.hh"
#include "service/topology_guard.hh"
#include "service/topology_mutation.hh"
#include "service/storage_service.hh"
#include "locator/tablet_replication_strategy.hh"

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>>
cql3::statements::alter_keyspace_statement::prepare_schema_mutations(query_processor& qp, api::timestamp_type ts) const {
    try {
        auto old_ksm = qp.db().find_keyspace(_name).metadata();
        const auto& feat = qp.proxy().features();
        const auto& tm = qp.proxy().get_token_metadata_ptr();
        auto m = service::prepare_keyspace_update_announcement(qp.db().real_database(), _attrs->as_ks_metadata_update(old_ksm, *tm, feat), ts);

        using namespace cql_transport;
        auto ret = ::make_shared<event::schema_change>(
                event::schema_change::change_type::UPDATED,
                event::schema_change::target_type::KEYSPACE,
                keyspace());

        return make_ready_future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>>(std::make_tuple(std::move(ret), std::move(m), std::vector<sstring>()));
    } catch (data_dictionary::no_such_keyspace& e) {
        return make_exception_future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>>(exceptions::invalid_request_exception("Unknown keyspace " + _name));
    }
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::alter_keyspace_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<alter_keyspace_statement>(*this));
}

static logging::logger mylogger("alter_keyspace");

future<> alter_tablets_keyspace(cql3::query_processor& qp, service::group0_guard& guard) {
    // TODO: check if new RF differs by at most 1 from the old RF. Fail the query otherwise
    if (this_shard_id() != 0) {
        // change coordinator changes can only be applied from shard 0
        co_return;
    }

    if (!qp.topology_global_queue_empty()) {
        auto topology = qp.proxy().get_token_metadata_ptr()->get_topology_change_info();
        service::topology_mutation_builder builder(guard.write_timestamp());
        builder.set_global_topology_request(service::global_topology_request::keyspace_rf_change);
        builder.set_keyspace_rf_change_data(_name, rf_per_dc); // TODO: implement
        service::topology_change change{{builder.build()}};
        auto& abort_source = guard.get_abort_source();
        sstring reason = format("TBD Alter tablets ks");
        // TODO: get group0 client from qp.remote().ss._group0->client()
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard, reason);
        while (true) {
            try {
                co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &abort_source);
                break;
            } catch (group0_concurrent_modification &) {
                mylogger.debug("alter tablets ks: concurrent modification, retrying");
            }
        }
    }
    else {
    // TODO: before returning, wait until the current global topology request is done. Blocker: #16374
    co_return make_exception_future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>>(
            exceptions::invalid_request_exception(
                    "topology mutation cannot be performed while other request is ongoing"));
    }
}

future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::alter_keyspace_statement::execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
    std::vector<sstring> warnings = check_against_restricted_replication_strategies(qp, keyspace(), *_attrs, qp.get_cql_stats());

    auto&& replication_strategy = qp.db().find_keyspace(_name).get_replication_strategy();
    if (replication_strategy.uses_tablets()) {
        alter_tablets_keyspace(qp, *guard); // TODO guard may be null
    }
    else {
        return schema_altering_statement::execute(qp, state, options, std::move(guard)).then(
                [warnings = std::move(warnings)](::shared_ptr<messages::result_message> msg) {
                    for (const auto &warning: warnings) {
                        msg->add_warning(warning);
                        mylogger.warn("{}", warning);
                    }
                    return msg;
                });
    }
}
