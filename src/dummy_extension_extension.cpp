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

void InjectLineageOperator(unique_ptr<LogicalOperator> &op,ClientContext &context) {
    if (!op) return;
    for (auto &child : op->children) {
        InjectLineageOperator(child, context);
    }

    if (op->type == LogicalOperatorType::LOGICAL_GET) {
        auto &get = op->Cast<LogicalGet>();
        get.AddColumnId(COLUMN_IDENTIFIER_ROW_ID);
        get.types.push_back(LogicalType::ROW_TYPE);
        DummyState::table_idx = get.table_index;
        DummyState::rowid_idx = get.types.size()-1;
        // projection_ids index into column_ids. if any exist then reference new column
        if (!get.projection_ids.empty()) get.projection_ids.push_back(get.GetColumnIds().size()-1);
        std::cout << "RowID injected in LogicalGet " << DummyState::rowid_idx << std::endl;
        std::cout << "LogicalGet types after injection: " << get.names.size() << " " << get.projection_ids.size() <<
          " " << get.returned_types.size() <<  " " << get.types.size() << " " << get.GetColumnIds().size() << "\n";
    } else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
      int col_id = DummyState::rowid_idx;
      // HACK TO AVOID RUNNING THIS FOR ALL QUERIES
      // TODO: add pragma or function to indicate we want to add lineage to a query or not
      if (col_id == 0) return;
      op->expressions.push_back(make_uniq<BoundReferenceExpression>(LogicalType::ROW_TYPE, col_id));
      op->types.push_back(LogicalType::ROW_TYPE);
    } else if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
        auto &aggr = op->Cast<LogicalAggregate>();
        if (!aggr.groups.empty()) {
            // Inject LIST(rowid)
            std::cout << "[DEBUG] Modifying Aggregate operator\n";
            std::cout << "[DEBUG] Aggregate types before modification: " << aggr.types.size() << "\n";
            
            DummyState::in_group_by = true;
            auto list_function = GetListFunction(context);
            auto rowid_colref = make_uniq_base<Expression, BoundReferenceExpression>(LogicalType::ROW_TYPE,
                DummyState::rowid_idx);
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
            
            std::cout << "[DEBUG] Aggregate types after adding LIST(rowid): " << aggr.types.size() << "\n";
            
            // remove list(bigint) lineage type and use bigint instead
            aggr.types.pop_back();
            aggr.types.push_back(LogicalType::ROW_TYPE);
            auto dummy = make_uniq<LogicalDummyOperator>(aggr.types, aggr.estimated_cardinality);
            dummy->AddChild(std::move(op));

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
