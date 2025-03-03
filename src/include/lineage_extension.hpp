#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

struct LineageState {
   static bool capture;
   static idx_t rowid_idx;
   static bool in_group_by;
   static idx_t table_idx;
   static bool first_projection_done;
   static std::unordered_map<string, vector<std::pair<Vector, int>>> lineage_store;
};


class LineageExtension : public Extension {
public:
   void Load(DuckDB &db) override;
   std::string Name() override;
   std::string Version() const override { return "v0.0.1"; }
};

} // namespace duckdb
