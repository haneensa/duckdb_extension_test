// physical_dummy_operator.hpp
#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include <iostream>

namespace duckdb {

// Our actual physical operator
class PhysicalDummyOperator : public PhysicalOperator {
public:
    PhysicalDummyOperator(vector<LogicalType> types, unique_ptr<PhysicalOperator> child);

    OperatorResultType Execute(ExecutionContext &context,
                             DataChunk &input, 
                             DataChunk &chunk,
                             GlobalOperatorState &gstate,
                             OperatorState &state) const override;

    string GetName() const override {
        return "PHYSICAL_DUMMY";
    }
};

// Our logical operator that extends LogicalExtensionOperator
class LogicalDummyOperator : public LogicalExtensionOperator {
public:
    explicit LogicalDummyOperator(vector<LogicalType> types, idx_t estimated_cardinality);
    string GetName() const override {
        return "DUMMY_OPERATOR";
    }
    unique_ptr<PhysicalOperator> CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) override {
      std::cout << "CreatePlan" << std::endl;
      // Get a plan for our child using the public API
      auto child = generator.CreatePlan(std::move(children[0]));
      std::cout << child->ToString() << std::endl;
      std::cout << types.size() << std::endl;
      return make_uniq<PhysicalDummyOperator>(types, std::move(child));
    }

protected:
    void ResolveTypes() override;
    vector<ColumnBinding> GetColumnBindings() override;
};

} // namespace duckdb
