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
    PhysicalDummyOperator(vector<LogicalType> types, unique_ptr<PhysicalOperator> child)
        : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), child->estimated_cardinality) {
        children.push_back(std::move(child));
    }

    OperatorResultType Execute(ExecutionContext &context,
                             DataChunk &input, 
                             DataChunk &chunk,
                             GlobalOperatorState &gstate,
                             OperatorState &state) const override {
        std::cout << "[DEBUG] Executing PhysicalDummyOperator" << std::endl;
        chunk.Reference(input);
        return OperatorResultType::NEED_MORE_INPUT;
    }

    string GetName() const override {
        return "PHYSICAL_DUMMY";
    }
};

// Our logical operator that extends LogicalExtensionOperator
class LogicalDummyOperator : public LogicalExtensionOperator {
public:
    explicit LogicalDummyOperator(unique_ptr<LogicalOperator> child) {
        children.push_back(std::move(child));
    }

    string GetName() const override {
        return "DUMMY_OPERATOR";
    }

    unique_ptr<PhysicalOperator> CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) override {
    // Get a plan for our child using the public API
    auto child = generator.CreatePlan(std::move(children[0]));  // Generate physical plan for the child operator
    
    auto child_types = child->types; // Save types before moving child
    
    // Create the physical operator (PhysicalDummyOperator)
    auto physical_operator = make_uniq<PhysicalDummyOperator>(child_types, std::move(child));

    std::cout << "[DEBUG] Created Physical Operator: " << typeid(*physical_operator).name() << std::endl;
    
    // Log the creation of the physical operator
    std::cout << "[DEBUG] Created Physical Operator: " << physical_operator->GetName() << std::endl;
    
    return std::move(physical_operator);  // Return the physical operator
}


protected:
    void ResolveTypes() override {
        if (children.empty()) {
            throw InternalException("Dummy operator needs a child");
        }
        types = children[0]->types;
    }
};

} // namespace duckdb
