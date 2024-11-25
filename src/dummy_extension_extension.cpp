#define DUCKDB_EXTENSION_MAIN
#include "dummy_extension_extension.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include <iostream>

namespace duckdb {

idx_t DummyState::rowid_idx = 0;
bool DummyState::in_group_by = false;
idx_t DummyState::table_idx = 0;
bool DummyState::first_projection_done = false;

DummyLineageOperator::DummyLineageOperator(vector<LogicalType> types, idx_t estimated_cardinality)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
    std::cout << "DummyLineageOperator constructor - type count: " << types.size() << "\n";
    this->types = std::move(types);
    this->estimated_cardinality = estimated_cardinality;
}

void DummyLineageOperator::ResolveTypes() {
    std::cout << "[DEBUG] DummyLineageOperator::ResolveTypes - entry\n";
    if (children.empty()) {
        std::cout << "[DEBUG] No children in DummyLineageOperator::ResolveTypes\n";
        return;
    }
    // Copy types from child and log them
    types = children[0]->types;
    std::cout << "[DEBUG] Child types resolved: ";
    for (auto &type : types) {
        std::cout << type.ToString() << " ";
    }
    std::cout << "\n";
    std::cout << "[DEBUG] DummyLineageOperator::ResolveTypes - exit\n";
}

vector<ColumnBinding> DummyLineageOperator::GetColumnBindings() {
    std::cout << "[DEBUG] DummyLineageOperator::GetColumnBindings - entry\n";
    if (children.empty()) {
        std::cout << "[DEBUG] No children in DummyLineageOperator::GetColumnBindings\n";
        return {};
    }
    auto child_bindings = children[0]->GetColumnBindings();
    std::cout << "[DEBUG] Child column bindings: ";
    for (auto &binding : child_bindings) {
        std::cout << binding.ToString() << " ";
    }
    std::cout << "\n";
    return child_bindings;
}




void InjectRowIdAndProjection(unique_ptr<LogicalOperator> &op) {
    if (!op) return;

    for (auto &child : op->children) {
        InjectRowIdAndProjection(child);
    }

    if (op->type == LogicalOperatorType::LOGICAL_GET) {
        auto &get = op->Cast<LogicalGet>();
        if (std::find(get.names.begin(), get.names.end(), "rowid") == get.names.end()) {
            get.AddColumnId(COLUMN_IDENTIFIER_ROW_ID);
            get.returned_types.push_back(LogicalType::BIGINT);
            get.names.push_back("rowid");
            DummyState::table_idx = get.table_index;
            DummyState::rowid_idx = get.returned_types.size() - 1;
            std::cout << "RowID injected in LogicalGet\n";
            std::cout << "LogicalGet types after injection: " << get.returned_types.size() << "\n";
        }
    }
}

AggregateFunction GetListFunction(ClientContext &context) {
    auto &catalog = Catalog::GetSystemCatalog(context);
    auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
        context, DEFAULT_SCHEMA, "list"
    );
    return entry.functions.GetFunctionByArguments(context, {LogicalType::BIGINT});
}

void ModifyLogicalAggregate(unique_ptr<LogicalOperator> &op, ClientContext &context) {
    if (!op) return;

    for (auto &child : op->children) {
        ModifyLogicalAggregate(child, context);
    }

    if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
        auto &aggr = op->Cast<LogicalAggregate>();
        if (!aggr.groups.empty()) {
            // Check if LIST(rowid) already exists
            bool has_list_rowid = false;
            for (auto &expr : aggr.expressions) {
                if (expr->type == ExpressionType::BOUND_AGGREGATE) {
                    auto &aggregate_expr = expr->Cast<BoundAggregateExpression>();
                    if (aggregate_expr.function.name == "list") {
                        // Check if the aggregate operates on rowid
                        if (!aggregate_expr.children.empty() &&
                            aggregate_expr.children[0]->type == ExpressionType::BOUND_COLUMN_REF) {
                            auto &col_ref = aggregate_expr.children[0]->Cast<BoundColumnRefExpression>();
                            if (col_ref.binding.column_index == DummyState::rowid_idx) {
                                has_list_rowid = true;
                                break;
                            }
                        }
                    }
                }
            }

            if (has_list_rowid) {
                std::cout << "[DEBUG] LIST(rowid) already present. Skipping modification.\n";
                return;
            }

            // Inject LIST(rowid)
            std::cout << "[DEBUG] Modifying Aggregate operator\n";
            std::cout << "[DEBUG] Aggregate types before modification: " << aggr.types.size() << "\n";
            
            DummyState::in_group_by = true;
            auto list_function = GetListFunction(context);
            auto rowid_colref = make_uniq<BoundColumnRefExpression>(
                "rowid",
                LogicalType::BIGINT,
                ColumnBinding(DummyState::table_idx, DummyState::rowid_idx)
            );

            vector<unique_ptr<Expression>> children;
            children.push_back(std::move(rowid_colref));

            auto list_aggregate = make_uniq<BoundAggregateExpression>(
                list_function,
                std::move(children),
                nullptr,
                nullptr,
                AggregateType::NON_DISTINCT
            );

            vector<LogicalType> original_types = aggr.types;
            aggr.expressions.push_back(std::move(list_aggregate));
            aggr.types.push_back(LogicalType::LIST(LogicalType::BIGINT));
            
            std::cout << "[DEBUG] Aggregate types after adding LIST(rowid): " << aggr.types.size() << "\n";
            
            auto dummy = make_uniq<DummyLineageOperator>(aggr.types, aggr.estimated_cardinality);
            dummy->children.push_back(std::move(op));
            op = std::move(dummy);
            
            std::cout << "[DEBUG] Aggregate operator modified and DummyLineage added\n";
        }
    }
}



std::string DummyExtensionExtension::Name() {
    return "dummy_extension";
}

void DummyExtensionExtension::Load(DuckDB &db) {
    auto optimizer_extension = make_uniq<OptimizerExtension>();
    optimizer_extension->optimize_function = [](OptimizerExtensionInput &input, 
                                            unique_ptr<LogicalOperator> &plan) {
        DummyState::in_group_by = false;
        DummyState::first_projection_done = false;
        
        InjectRowIdAndProjection(plan);
        ModifyLogicalAggregate(plan, input.context);
    };

    db.instance->config.optimizer_extensions.emplace_back(*optimizer_extension);
    std::cout << "Dummy extension loaded successfully.\n";
}

extern "C" {
DUCKDB_EXTENSION_API void dummy_extension_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::DummyExtensionExtension>();
}

DUCKDB_EXTENSION_API const char *dummy_extension_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

} // namespace duckdb
