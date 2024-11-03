// dummy_extension_extension.cpp
#define DUCKDB_EXTENSION_MAIN
#include "dummy_extension_extension.hpp"
#include "physical_dummy_operator.hpp"

namespace duckdb {

void DummyExtensionExtension::InjectDummyOperator(unique_ptr<LogicalOperator> &plan) {
    if (!plan) {
        return;
    }

    for (auto &child : plan->children) {
        InjectDummyOperator(child);
    }

    if (plan->type == LogicalOperatorType::LOGICAL_GET) {
        printf("Found LOGICAL_GET, injecting dummy operator\n");
        try {
            auto dummy = make_uniq<LogicalDummyOperator>(std::move(plan));
            plan = std::move(dummy);
            printf("Successfully injected dummy operator\n");
        } catch (const std::exception &e) {
            printf("Error injecting dummy operator: %s\n", e.what());
        }
    }
}

static void OptimizePlan(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    printf("OptimizePlan called\n");
    DummyExtensionExtension::InjectDummyOperator(plan);
}

void DummyExtensionExtension::Load(DuckDB &db) {
    try {
        printf("Loading dummy extension...\n");

        auto optimizer_extension = make_uniq<OptimizerExtension>();
        optimizer_extension->optimize_function = OptimizePlan;
        db.instance->config.optimizer_extensions.push_back(std::move(*optimizer_extension));

        printf("Dummy extension loaded successfully\n");
    } catch (const std::exception& e) {
        printf("Error loading dummy extension: %s\n", e.what());
    }
}

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void dummy_extension_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::DummyExtensionExtension>();
}

DUCKDB_EXTENSION_API const char *dummy_extension_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}
