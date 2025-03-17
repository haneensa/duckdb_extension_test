#include "lineage_extension.hpp"
#include "physical_lineage_operator.hpp"
#include "logical_lineage_operator.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include <iostream>

namespace duckdb {

LogicalLineageOperator::LogicalLineageOperator(idx_t estimated_cardinality,
    idx_t operator_id, idx_t query_id, LogicalOperatorType dependent_type,
    idx_t left_rid, idx_t right_rid, bool is_root)  {
  if (LineageState::debug)
    std::cout << "LogicalLineageOperator with child type:" << EnumUtil::ToChars<LogicalOperatorType>(dependent_type) << "\n";
  this->estimated_cardinality = estimated_cardinality;
  this->operator_id = operator_id;
  this->query_id = query_id;
  this->dependent_type = dependent_type;
  this->is_root = is_root;
  this->left_rid = left_rid;
  this->right_rid = right_rid;
  this->mark_join = false;
}

void LogicalLineageOperator::ResolveTypes()  {
  if (children.empty()) return;
  types = children[0]->types; // Copy types from child and log them
  if (this->dependent_type == LogicalOperatorType::LOGICAL_DELIM_GET) { }
  if (this->dependent_type == LogicalOperatorType::LOGICAL_CHUNK_GET) { 
    types.push_back(LogicalType::ROW_TYPE);
    return;
  }
  if (LineageState::debug) {
    std::cout << "Resolve Types (child[0]): " << this->operator_id << " " <<  EnumUtil::ToChars<LogicalOperatorType>(dependent_type) << "\n";
    for (auto &type : types) { std::cout << type.ToString() << " ";}
     std::cout << "\n";
  }
  if (this->dependent_type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN
     || this->dependent_type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
    auto& join = children[0]->Cast<LogicalJoin>();
    if (LineageState::debug) {
      std::cout << "Child[0] types with left_rid: " << left_rid << std::endl;
      for (auto &type : children[0]->children[0]->types) { std::cout << type.ToString() << " "; }
      std::cout << "\n";
      
      std::cout << "Child[1] types with right_rid: " << right_rid << std::endl;
      for (auto &type : children[0]->children[1]->types) { std::cout << type.ToString() << " "; }
      std::cout << "\n";
    }
    if (join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI
     || join.join_type == JoinType::RIGHT_SEMI || join.join_type == JoinType::RIGHT_ANTI) {
      return;
    }
    types.erase(types.begin() + left_rid);
  }
  if (mark_join) {
    // if mark join, then need to move the end of the left child to the last column
    // std::cout << "Mark join " << std::endl;
    //types.erase(types.begin() + left_rid);
    //types.push_back(LogicalType::ROW_TYPE);
    // for (auto &type : types) { std::cout << type.ToString() << " "; }
    // std::cout << "\n";
    return;
  }
  types.pop_back();
  if (!is_root) types.push_back(LogicalType::ROW_TYPE);
}

vector<ColumnBinding> LogicalLineageOperator::GetColumnBindings() {
//  std::cout << "**** " << std::endl;
  if (children.empty()) return {};
//  std::cout << "[ Child type: " << EnumUtil::ToChars<LogicalOperatorType>(dependent_type) << "\n";
  auto child_bindings = children[0]->GetColumnBindings();
  if (this->dependent_type == LogicalOperatorType::LOGICAL_CHUNK_GET) { 
    if (child_bindings.empty()) return child_bindings;
    idx_t table_index = child_bindings.back().table_index;
    child_bindings.emplace_back(table_index, child_bindings.size());
  //  std::cout << this->operator_id << "[DEBUG] Child column bindings" << std::endl;
  //  for (auto &binding : child_bindings) { std::cout << binding.ToString() << " "; }
  //  std::cout << "\n"; // no op for now
    return child_bindings;
  }

  if (this->dependent_type == LogicalOperatorType::LOGICAL_DELIM_GET) { return child_bindings; }
  if (LineageState::debug) {
    std::cout << this->operator_id << "[DEBUG] Child column bindings " <<  EnumUtil::ToChars<LogicalOperatorType>(dependent_type) << "\n";
    for (auto &binding : child_bindings) { std::cout << binding.ToString() << " ";}
    std::cout << "\n";
  }

  if (mark_join) {
     // std::cout << "join binding: " << left_rid << " " << child_bindings.size() << " " << types.size() << std::endl;
        auto& join = children[0]->children[0]->Cast<LogicalJoin>();
       // std::cout << "( join left: " << std::endl;
       // for (auto &binding : join.children[0]->GetColumnBindings()) { std::cout << binding.ToString() << " "; }
        //std::cout << "\n ) " << left_rid << " " << child_bindings.size() << " " 
      //    << EnumUtil::ToChars<LogicalOperatorType>(dependent_type) << "\n";
    //for (auto &binding : child_bindings) { std::cout << binding.ToString() << " ";}
    // std::cout << "\n";
      auto left_most = child_bindings[left_rid];
      child_bindings.erase(child_bindings.begin() + left_rid);
      child_bindings.push_back(left_most);
      // get bindings of child
     // for (auto &binding : child_bindings) { std::cout << binding.ToString() << " ";}
     // std::cout << "\n";
  }

  if (this->dependent_type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN
       || this->dependent_type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        auto& join = children[0]->Cast<LogicalJoin>();
      //  std::cout << "join left: " << std::endl;
      //  for (auto &binding : join.children[0]->GetColumnBindings()) { std::cout << binding.ToString() << " "; }
      //  std::cout << "\n";
      //  std::cout << "join right: " << std::endl;
      //  for (auto &binding : join.children[1]->GetColumnBindings()) { std::cout << binding.ToString() << " "; }
      //  std::cout << "\n";
      if (join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI
       || join.join_type == JoinType::RIGHT_SEMI || join.join_type == JoinType::RIGHT_ANTI) {
        return child_bindings;
      }
      child_bindings.erase(child_bindings.begin() + left_rid);
    //  std::cout << "-> join binding: " << left_rid << " " << child_bindings.size() << " " << types.size() << std::endl;
    //  for (auto &binding : child_bindings) { std::cout << binding.ToString() << " ";}
    //  std::cout << "\n<- ";
  } 
  //std::cout << "done ]" << std::endl;
  return child_bindings;
}
unique_ptr<PhysicalOperator> LogicalLineageOperator::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
  // Get a plan for our child using the public API
  bool debug = false;
  auto child = generator.CreatePlan(std::move(children[0]));
  if (this->dependent_type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
    // this has distinct and join we need to modify
    auto& delim = child->Cast<PhysicalDelimJoin>();
    if (debug) std::cout << "Delim join: -------- " << delim.distinct->types.size() << std::endl;
    auto &catalog = Catalog::GetSystemCatalog(context);
    auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
        context, DEFAULT_SCHEMA, "list"
    );
    idx_t child_left_rid = delim.children.back()->GetTypes().size()-1; // the index of the row id column distinct read from
    if (debug) std::cout << "Delim join types: " << child_left_rid << std::endl;
   // for (auto &type : delim.children.back()->GetTypes()) { std::cout << type.ToString() << " "; }
   // std::cout << "\n";
    AggregateFunction list_fun = entry.functions.GetFunctionByArguments(context, 
                                                  {LogicalType::ROW_TYPE});
     auto rowid_colref = make_uniq_base<Expression, BoundReferenceExpression>(
                                            LogicalType::ROW_TYPE, child_left_rid);
     vector<unique_ptr<Expression>> children;
     children.push_back(std::move(rowid_colref));
     unique_ptr<FunctionData> bind_info = list_fun.bind(context, list_fun, children);
     auto list_aggregate = make_uniq<BoundAggregateExpression>(
          list_fun, std::move(children), nullptr, std::move(bind_info),
          AggregateType::NON_DISTINCT
      );

      delim.distinct->grouped_aggregate_data.bindings.push_back(list_aggregate.get());
      delim.distinct->grouped_aggregate_data.aggregates.push_back(std::move(list_aggregate));
      delim.distinct->grouped_aggregate_data.aggregate_return_types.push_back(LogicalType::LIST(LogicalType::ROW_TYPE));
      delim.distinct->types.push_back(LogicalType::LIST(LogicalType::ROW_TYPE));
      delim.distinct->grouped_aggregate_data.payload_types.push_back(LogicalType::ROW_TYPE);
      delim.distinct->non_distinct_filter.push_back(0);
      if (debug) std::cout << "-> DELIM JOIN --------------------------- child left: " << child_left_rid << std::endl;
     // std::cout << delim.distinct->ToString() << std::endl;
      //if (left_rid > 0) {
      //  left_rid = child_left_rid;;
     // }
  }
  if (LineageState::debug) {
    std::cout << "[DEBUG] LogicalLineageOperator::CreatePlan. " << std::endl;
    std::cout << child->ToString() << std::endl;
  }
  return make_uniq<PhysicalLineageOperator>(types, std::move(child), operator_id, query_id, dependent_type,
      left_rid, right_rid, is_root, mark_join);
}
}
