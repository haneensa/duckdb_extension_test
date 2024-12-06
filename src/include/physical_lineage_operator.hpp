// physical_lineage_operator.hpp
#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include <iostream>

namespace duckdb {

// Our actual physical operator
class PhysicalLineageOperator : public PhysicalOperator {
public:
    PhysicalLineageOperator(vector<LogicalType> types, unique_ptr<PhysicalOperator> child);

    OperatorResultType Execute(ExecutionContext &context,
                             DataChunk &input, 
                             DataChunk &chunk,
                             GlobalOperatorState &gstate,
                             OperatorState &state) const override;

    unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

    string GetName() const override {
        return "PHYSICAL_LINEAGE";
    }
};


} // namespace duckdb
