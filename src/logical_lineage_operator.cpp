#include "physical_lineage_operator.hpp"
#include "logical_lineage_operator.hpp"
#include <iostream>

namespace duckdb {

LogicalLineageOperator::LogicalLineageOperator(vector<LogicalType> types, idx_t estimated_cardinality) {
    std::cout << "LogicalLineageOperator constructor - type count: " << types.size() << "\n";
    this->estimated_cardinality = estimated_cardinality;
}

void LogicalLineageOperator::ResolveTypes() {
    std::cout << "[DEBUG] LogicalLineageOperator::ResolveTypes - entry\n";
    if (children.empty()) {
        std::cout << "[DEBUG] No children in LogicalLineageOperator::ResolveTypes\n";
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
    std::cout << "[DEBUG] LogicalLineageOperator::ResolveTypes - exit\n";
}

vector<ColumnBinding> LogicalLineageOperator::GetColumnBindings() {
  std::cout << "[DEBUG] LogicalLineageOperator::GetColumnBindings - entry\n";
  if (children.empty()) {
     std::cout << "[DEBUG] No children in LogicalLineageOperator::GetColumnBindings\n";
     return {};
  }
  auto child_bindings = children[0]->GetColumnBindings();
  std::cout << "[DEBUG] Child column bindings: ";
  for (auto &binding : child_bindings) {
      std::cout << binding.ToString() << " ";
  }
  std::cout << "\n";
  std::cout << "[DEBUG] LogicalLineageOperator::GetColumnBindings - exist\n";
  return child_bindings;
}
unique_ptr<PhysicalOperator> LogicalLineageOperator::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
  // Get a plan for our child using the public API
  auto child = generator.CreatePlan(std::move(children[0]));
  std::cout << child->ToString() << std::endl;
  std::cout << types.size() << std::endl;
  return make_uniq<PhysicalLineageOperator>(types, std::move(child));
}
}
