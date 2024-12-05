#define DUCKDB_EXTENSION_MAIN
#include "dummy_extension_extension.hpp"
#include "physical_dummy_operator.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include <iostream>

namespace duckdb {

idx_t DummyState::rowid_idx = 0;
bool DummyState::in_group_by = false;
idx_t DummyState::table_idx = 0;
bool DummyState::first_projection_done = false;

LogicalDummyOperator::LogicalDummyOperator(vector<LogicalType> types, idx_t estimated_cardinality) {
    std::cout << "DummyLineageOperator constructor - type count: " << types.size() << "\n";
    this->estimated_cardinality = estimated_cardinality;
}

void LogicalDummyOperator::ResolveTypes() {
    std::cout << "[DEBUG] DummyLineageOperator::ResolveTypes - entry\n";
    if (children.empty()) {
        std::cout << "[DEBUG] No children in DummyLineageOperator::ResolveTypes\n";
        return;
    }
    // Copy types from child and log them
    types = children[0]->types;
    types.pop_back();
    types.push_back(LogicalType::ROW_TYPE);
    for (auto &type : types) {
        std::cout << type.ToString() << " ";
    }
    std::cout << "\n";
    std::cout << "[DEBUG] DummyLineageOperator::ResolveTypes - exit\n";
}

vector<ColumnBinding> LogicalDummyOperator::GetColumnBindings() {
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
  std::cout << "[DEBUG] DummyLineageOperator::GetColumnBindings - exist\n";
  return child_bindings;
}

AggregateFunction GetListFunction(ClientContext &context) {
    auto &catalog = Catalog::GetSystemCatalog(context);
    auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
        context, DEFAULT_SCHEMA, "list"
    );
    return entry.functions.GetFunctionByArguments(context, {LogicalType::ROW_TYPE});
}

void InjectLineageOperator(unique_ptr<LogicalOperator> &op, ClientContext &context) {
    if (!op) return;

    // Recursively process child nodes
    for (auto &child : op->children) {
        InjectLineageOperator(child, context);
    }

    if (op->type == LogicalOperatorType::LOGICAL_GET) {
        auto &get = op->Cast<LogicalGet>();
        get.AddColumnId(COLUMN_IDENTIFIER_ROW_ID);
        get.types.push_back(LogicalType::ROW_TYPE);
        DummyState::table_idx = get.table_index;
        DummyState::rowid_idx = get.types.size() - 1;

        if (!get.projection_ids.empty()) {
            get.projection_ids.push_back(get.GetColumnIds().size() - 1);
        }

        std::cout << "RowID injected in LogicalGet " << DummyState::rowid_idx << std::endl;

    } else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
        int col_id = DummyState::rowid_idx;

        // Ensure ROW_TYPE column is added to the projection
        if (col_id != 0) {
            std::cout << "[DEBUG] Adding ROW_TYPE to LogicalProjection.\n";
            op->expressions.push_back(make_uniq<BoundReferenceExpression>(LogicalType::ROW_TYPE, col_id));
            op->types.push_back(LogicalType::ROW_TYPE);
        } else {
            std::cout << "[DEBUG] ROW_TYPE column missing, skipping LogicalProjection lineage.\n";
        }

    } else if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
        auto &filter = op->Cast<LogicalFilter>();

        // Add lineage propagation logic
        int col_id = DummyState::rowid_idx;
        if (col_id == 0) return;

        // Update types to include ROW_TYPE
        filter.types.push_back(LogicalType::ROW_TYPE);

        // Add ROW_TYPE as a new expression to propagate lineage
        filter.expressions.push_back(make_uniq<BoundReferenceExpression>(LogicalType::ROW_TYPE, col_id));

        std::cout << "[DEBUG] Lineage injected in LogicalFilter with ROW_TYPE column.\n";

    } else if (op->type == LogicalOperatorType::LOGICAL_JOIN) {
        auto &join = op->Cast<LogicalJoin>();

        // Track lineage for left and right tables involved in the join
        int col_id_left = DummyState::rowid_idx;
        int col_id_right = DummyState::rowid_idx + 1;

        // Inject ROW_TYPE for left table if not already present
        if (col_id_left != 0) {
            std::cout << "[DEBUG] Adding ROW_TYPE to left side of join.\n";
            join.left_projection_map.push_back(col_id_left); // Mapping ROW_TYPE to LHS projection
        }

        // Inject ROW_TYPE for right table if not already present
        if (col_id_right != 0) {
            std::cout << "[DEBUG] Adding ROW_TYPE to right side of join.\n";
            join.right_projection_map.push_back(col_id_right); // Mapping ROW_TYPE to RHS projection
        }

        // Merge the ROW_TYPE columns into the join output (after join)
        join.types.push_back(LogicalType::ROW_TYPE);
        DummyState::rowid_idx = join.types.size() - 1;

        std::cout << "[DEBUG] Lineage injected into LogicalJoin for both left and right sides.\n";
        
    } else if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
        auto &aggr = op->Cast<LogicalAggregate>();

        if (!aggr.groups.empty()) {
            // Handle grouping logic (unchanged)
            DummyState::in_group_by = true;
            auto list_function = GetListFunction(context);
            auto rowid_colref = make_uniq_base<Expression, BoundReferenceExpression>(
                LogicalType::ROW_TYPE, DummyState::rowid_idx
            );
            vector<unique_ptr<Expression>> children;
            children.push_back(std::move(rowid_colref));

            unique_ptr<FunctionData> bind_info = list_function.bind(context, list_function, children);

            auto list_aggregate = make_uniq<BoundAggregateExpression>(
                list_function,
                std::move(children),
                nullptr,
                std::move(bind_info),
                AggregateType::NON_DISTINCT
            );

            aggr.expressions.push_back(std::move(list_aggregate));
            aggr.types.push_back(LogicalType::LIST(LogicalType::ROW_TYPE));

            aggr.types.pop_back();
            aggr.types.push_back(LogicalType::ROW_TYPE);
            auto dummy = make_uniq<LogicalDummyOperator>(aggr.types, aggr.estimated_cardinality);
            dummy->AddChild(std::move(op));

            op = std::move(dummy);

        } else {
            // Handle ungrouped aggregates
            DummyState::in_group_by = false;

            // Remove ROW_TYPE column if present
            if (DummyState::rowid_idx != 0) {
                std::cout << "[DEBUG] Removing ROW_TYPE column for ungrouped aggregate.\n";
                aggr.types.pop_back();
                DummyState::rowid_idx = 0;
            }

            // Ensure aggregate expressions remain intact without ROW_TYPE
            std::cout << "[DEBUG] No grouping found in LogicalAggregate, skipping ROW_TYPE handling.\n";
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
        DummyState::rowid_idx = 0;
        
        std::cout << "Plan prior to modifications" << std::endl;
        std::cout << plan->ToString() << std::endl;
        InjectLineageOperator(plan, input.context);
        std::cout << "Plan after to modifications" << std::endl;
        std::cout << plan->ToString() << std::endl;
        // TODO: inject lineage op at the root of the plan to extract any annotation columns
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
