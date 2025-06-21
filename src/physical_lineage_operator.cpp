#include "lineage_extension.hpp"
#include "physical_lineage_operator.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/lineage_logger.hpp"
#include <iostream>

namespace duckdb {
PhysicalLineageOperator::PhysicalLineageOperator(vector<LogicalType> types, unique_ptr<PhysicalOperator> child,
        idx_t operator_id, idx_t query_id, LogicalOperatorType dependent_type, int source_count,
        idx_t left_rid, idx_t right_rid, bool is_root, string join_type, bool pre, bool post)
      : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), child->estimated_cardinality),
      is_root(is_root), dependent_type(dependent_type), source_count(source_count),
      operator_id(operator_id), query_id(query_id),
      left_rid(left_rid), right_rid(right_rid), join_type(join_type), pre(pre), post(post) {
      children.push_back(std::move(child));
}

class PhysicalLineageState : public OperatorState {
public:
  explicit PhysicalLineageState(ExecutionContext &context, string table_name,
      LogicalOperatorType dependent_type, int source_count, string join_type) 
    : offset(0), source_count(source_count), table_name(table_name),
      join_type(join_type), dependent_type(dependent_type), pre(false), post(false) {
  }

public:
  void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
    if (LineageState::capture == false || LineageState::persist == false) return;
    if (LineageState::lineage_store[table_name].size()) return;
    if (post) return;

    if (LineageState::debug) {
      std::cout << "[DEBUG] persist lineage " <<  table_name << " " << lineage.size()
        << " " << lineage_right.size() << " " << EnumUtil::ToChars<LogicalOperatorType>(this->dependent_type) << std::endl;
    }

    // TODO: local per thread, need to track thread_id/partition_id too
    LineageState::lineage_types[table_name] = dependent_type;
    LineageState::lineage_store[table_name] = std::move(lineage);
    if (source_count == 2 || join_type=="RIGHT_SEMI" || join_type=="RIGHT") {
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
  int source_count;
  string join_type;
  bool pre, post;
};


unique_ptr<OperatorState> PhysicalLineageOperator::GetOperatorState(ExecutionContext &context) const {
  string table_name = GetName() + "_" + to_string(query_id) + "_" + to_string(operator_id);
 // std::cout << pre << " " << post << " Lineage store address from LM: " << &LineageGlobal::LS << " thread: " << &context.thread << std::endl;
	return make_uniq<PhysicalLineageState>(context, table_name, dependent_type, source_count, join_type);
}

// TODO: we need to also include thread_id/partition_id annotation and propagate it
OperatorResultType PhysicalLineageOperator::Execute(ExecutionContext &context,
                         DataChunk &input, 
                         DataChunk &chunk,
                         GlobalOperatorState &gstate,
                         OperatorState &state_p) const {
    // local per thread
    auto &state = state_p.Cast<PhysicalLineageState>();
    if (LineageState::debug) {
      std::cout << pre << " " << post << " [PhysicalLineageOperator] join_type:" <<  join_type << ", source_count: " << source_count 
      << ", left_rid: " << left_rid << ", right_rid:" << right_rid << ", dependent_type: " << 
       EnumUtil::ToChars<LogicalOperatorType>(this->dependent_type) << std::endl;
      std::cout << input.ColumnCount() << std::endl;
      for (auto &type : input.GetTypes()) { std::cout << type.ToString() << " "; }
      std::cout << "\n -------" << std::endl;
      
      std::cout << chunk.ColumnCount() << std::endl;
      for (auto &type : chunk.GetTypes()) { std::cout << type.ToString() << " "; }
      std::cout << "\n -------" << std::endl;
    }

    if (left_rid == 0 && right_rid > 0) { // right semi join
      chunk.SetCardinality(input);
      chunk.Reference(input);
      // pass annotations to parent since it is single annotations
      return OperatorResultType::NEED_MORE_INPUT;
    }

    if (this->dependent_type == LogicalOperatorType::LOGICAL_CHUNK_GET) { 
      chunk.SetCapacity(input);
      chunk.SetCardinality(input);
      for (idx_t i = 0; i < left_rid; i++) {
        chunk.data[i].Reference(input.data[i]);
      }
      // Append row identifier since it's hard to modify Chunk Get
      chunk.data.back().Sequence(state.offset, 1, input.size());
      state.offset += input.size();
      return OperatorResultType::NEED_MORE_INPUT;
    }

    // reference payload from the input
    chunk.SetCapacity(input);
    chunk.SetCardinality(input);
    for (idx_t i = 0; i < left_rid; i++) {
      chunk.data[i].Reference(input.data[i]);
    }
    
    if (join_type == "MARK") {
      // pass annotations to parent since it is single annotations
      chunk.data.back().Reference(input.data[left_rid]);
      chunk.data[left_rid].Reference(input.data.back());
      return OperatorResultType::NEED_MORE_INPUT;
    }

    for (idx_t i = left_rid+1; i < left_rid+right_rid+1; i++) {
      chunk.data[i-1].Reference(input.data[i]);
    }

    // Extract annotations payload from left input
    if (!post && left_rid > 0 && LineageState::persist) {
      idx_t annotation_col = left_rid;
      Vector annotations(input.data[annotation_col].GetType());
      VectorOperations::Copy(input.data[annotation_col], annotations, input.size(), 0, 0);
      state.lineage.push_back({annotations, input.size()});
    }

    if (!post && (this->source_count == 2 || join_type=="RIGHT_SEMI" || join_type=="RIGHT") && LineageState::persist) {
      // Extract annotations payload from the right input
      idx_t annotation_col = input.ColumnCount() - 1;
      Vector annotations(input.data[annotation_col].GetType());
      VectorOperations::Copy(input.data[annotation_col], annotations, input.size(), 0, 0);
      state.lineage_right.push_back({annotations, input.size()});
    }

    if (pre) {
      state.offset += input.size();
    }

    if (!is_root && !pre) {
      // This is not the root, reindex complex annotations
      chunk.data.back().Sequence(state.offset, 1, input.size());
      state.offset += input.size();
    }
    
    return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
