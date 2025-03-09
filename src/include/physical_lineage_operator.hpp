// physical_lineage_operator.hpp
#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include <iostream>

namespace duckdb {

// Our actual physical operator
class PhysicalLineageOperator : public PhysicalOperator {
public:
    PhysicalLineageOperator(vector<LogicalType> types, unique_ptr<PhysicalOperator> child,
        idx_t operator_id, idx_t query_id, LogicalOperatorType dependent_type,
        idx_t left_rid, idx_t right_rid, bool is_root);

    OperatorResultType Execute(ExecutionContext &context,
                             DataChunk &input, 
                             DataChunk &chunk,
                             GlobalOperatorState &gstate,
                             OperatorState &state) const override;

    unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;


    bool ParallelOperator() const override {
      return true;
    }

    string GetName() const override {
        return "PHYSICAL_LINEAGE";
    }
public:
    bool is_root;
    idx_t operator_id;
    idx_t query_id;
    idx_t left_rid;
    idx_t right_rid;
    LogicalOperatorType dependent_type;
};


} // namespace duckdb
