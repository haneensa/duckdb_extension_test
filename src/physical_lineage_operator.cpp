#include "lineage_extension.hpp"
#include "physical_lineage_operator.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include <iostream>

namespace duckdb {
PhysicalLineageOperator::PhysicalLineageOperator(vector<LogicalType> types, unique_ptr<PhysicalOperator> child,
        idx_t operator_id, idx_t query_id, LogicalOperatorType dependent_type,
        idx_t left_rid, idx_t right_rid, bool is_root)
      : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), child->estimated_cardinality),
      is_root(is_root), dependent_type(dependent_type), operator_id(operator_id), query_id(query_id),
      left_rid(left_rid), right_rid(right_rid) {
      children.push_back(std::move(child));
}

class PhysicalLineageState : public OperatorState {
public:
  explicit PhysicalLineageState(ExecutionContext &context, string table_name,
      LogicalOperatorType dependent_type) : offset(0), table_name(table_name), dependent_type(dependent_type) {
  }

public:
  void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
    if (LineageState::capture == false) return;
    if (LineageState::lineage_store[table_name].size()) return;

    if (true || LineageState::debug) {
      std::cout << "[DEBUG] persist lineage " <<  table_name << " " << lineage.size()
        << " " << lineage_right.size() <<  std::endl;
    }

    LineageState::lineage_types[table_name] = dependent_type;
    LineageState::lineage_store[table_name] = std::move(lineage);
    if (dependent_type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
      LineageState::lineage_store[table_name+"_right"] = std::move(lineage_right);
    }
  }
   
  // todo: maybe decompose it into two arrays? one for Vectors one for size
  // this way we don't have to duplicate size for the right side
  vector<std::pair<Vector, int>> lineage;
  vector<std::pair<Vector, int>> lineage_right;
  string table_name;
  LogicalOperatorType dependent_type;
  idx_t offset;
};


unique_ptr<OperatorState> PhysicalLineageOperator::GetOperatorState(ExecutionContext &context) const {
  string table_name = GetName() + "_" + to_string(query_id) + "_" + to_string(operator_id);
	return make_uniq<PhysicalLineageState>(context, table_name, dependent_type);
}

OperatorResultType PhysicalLineageOperator::Execute(ExecutionContext &context,
                         DataChunk &input, 
                         DataChunk &chunk,
                         GlobalOperatorState &gstate,
                         OperatorState &state_p) const {
    auto &state = state_p.Cast<PhysicalLineageState>();

    // D_ASSERT();

    // reference payload from the input
    chunk.SetCapacity(input);
    chunk.SetCardinality(input);
    for (idx_t i = 0; i < left_rid; i++) {
      chunk.data[i].Reference(input.data[i]);
    }

    for (idx_t i = left_rid+1; i < left_rid+right_rid+1; i++) {
      chunk.data[i-1].Reference(input.data[i]);
    }

    // Extract annotations payload from left input
    idx_t annotation_col = left_rid;
    Vector annotations(input.data[annotation_col].GetType());
    VectorOperations::Copy(input.data[annotation_col], annotations, input.size(), 0, 0);
    state.lineage.push_back({annotations, input.size()});
    
    if (this->dependent_type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
      // Extract annotations payload from the right input
      idx_t annotation_col = input.ColumnCount() - 1;
      Vector annotations(input.data[annotation_col].GetType());
      VectorOperations::Copy(input.data[annotation_col], annotations, input.size(), 0, 0);
      state.lineage_right.push_back({annotations, input.size()});
    }

    if (!is_root) {
      // This is not the root, reindex complex annotations
      chunk.data.back().Sequence(state.offset, 1, input.size());
      state.offset += input.size();
    }
    
    
    return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
