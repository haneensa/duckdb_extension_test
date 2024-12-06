#include "physical_lineage_operator.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include <iostream>

namespace duckdb {
PhysicalLineageOperator::PhysicalLineageOperator(vector<LogicalType> types, unique_ptr<PhysicalOperator> child)
      : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), child->estimated_cardinality) {
      children.push_back(std::move(child));
}

class LineageState : public OperatorState {
public:
  explicit LineageState(ExecutionContext &context) {
  }

public:
  void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
    // TODO: gather lineage into global lineage
    std::cout << "Debug lineage" << std::endl;
    for (auto& l : lineage) {
      std::cout << l.first.ToString(l.second) << std::endl;
    }
  }
    
  vector<std::pair<Vector, int>> lineage;
};


unique_ptr<OperatorState> PhysicalLineageOperator::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<LineageState>(context);
}

OperatorResultType PhysicalLineageOperator::Execute(ExecutionContext &context,
                         DataChunk &input, 
                         DataChunk &chunk,
                         GlobalOperatorState &gstate,
                         OperatorState &state_p) const {
  	auto &state = state_p.Cast<LineageState>();

    std::cout << "Lineage:Execute:input" << std::endl;
    std::cout << input.ToString() << std::endl;

    // reference payload from the input
    chunk.SetCapacity(input);
    chunk.SetCardinality(input);
    for (idx_t i = 0; i < input.ColumnCount()-1; i++) {
      chunk.data[i].Reference(input.data[i]);
    }
    
    // TODO: extract annotations payload from input and persist it in memory
    idx_t annotation_col = input.ColumnCount() - 1;
    Vector annotations(input.data[annotation_col].GetType());
    VectorOperations::Copy(input.data[annotation_col], annotations, input.size(), 0, 0);

    state.lineage.push_back({annotations, input.size()});

    // TODO: if this is not the root, reindex complex annotations
    chunk.data[input.ColumnCount()-1].Sequence(0, 1, input.size());
    
    std::cout << "Lineage:Execute:output" << std::endl;
    std::cout << chunk.ToString() << std::endl;

    return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
