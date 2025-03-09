#include "lineage_extension.hpp"
#include "physical_lineage_operator.hpp"
#include "logical_lineage_operator.hpp"
#include <iostream>

namespace duckdb {

LogicalLineageOperator::LogicalLineageOperator(idx_t estimated_cardinality,
    idx_t operator_id, idx_t query_id, LogicalOperatorType dependent_type,
    idx_t left_rid, idx_t right_rid, bool is_root)  {
    if (LineageState::debug)
      std::cout << "LogicalLineageOperator  with child type:" << EnumUtil::ToChars<LogicalOperatorType>(dependent_type) << "\n";
    this->estimated_cardinality = estimated_cardinality;
    this->operator_id = operator_id;
    this->query_id = query_id;
    this->dependent_type = dependent_type;
    this->is_root = is_root;
    this->left_rid = left_rid;
    this->right_rid = right_rid;
}

void LogicalLineageOperator::ResolveTypes() {
    if (children.empty()) {
        std::cout << "[DEBUG] No children in LogicalLineageOperator::ResolveTypes\n";
        return;
    }
    // Copy types from child and log them
    types = children[0]->types;
    if (LineageState::debug) {
      std::cout << "child[0] types: " << std::endl;
      for (auto &type : types) {
          std::cout << type.ToString() << " ";
      }
      std::cout << "\n";
    }
    if (this->dependent_type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
      if (LineageState::debug) {
        std::cout << "child[0] types with left_rid: " << left_rid << std::endl;
        for (auto &type : children[0]->children[0]->types) {
            std::cout << type.ToString() << " ";
        }
        std::cout << "\n";
      }
      // need to remove from the end of the left child
      types.erase(types.begin() + left_rid);
    }
    
    types.pop_back();
    
    if (!is_root) types.push_back(LogicalType::ROW_TYPE);
}

vector<ColumnBinding> LogicalLineageOperator::GetColumnBindings() {
  if (children.empty()) {
     return {};
  }
  auto child_bindings = children[0]->GetColumnBindings();
  if (LineageState::debug) {
    std::cout << this->operator_id << "[DEBUG] Child column bindings" << std::endl;
    for (auto &binding : child_bindings) {
        std::cout << binding.ToString() << " ";
    }
    std::cout << "\n";
  }
  if (this->dependent_type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
      child_bindings.erase(child_bindings.begin() + left_rid);
  } 
  return child_bindings;
}
unique_ptr<PhysicalOperator> LogicalLineageOperator::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
  // Get a plan for our child using the public API
  auto child = generator.CreatePlan(std::move(children[0]));
  if (LineageState::debug) {
    std::cout << "[DEBUG] LogicalLineageOperator::CreatePlan. " << std::endl;
    std::cout << child->ToString() << std::endl;
  }
  return make_uniq<PhysicalLineageOperator>(types, std::move(child), operator_id, query_id, dependent_type,
      left_rid, right_rid, is_root);
}
}
