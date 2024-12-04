#include "physical_dummy_operator.hpp"
#include <iostream>

namespace duckdb {
  PhysicalDummyOperator::PhysicalDummyOperator(vector<LogicalType> types, unique_ptr<PhysicalOperator> child)
        : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), child->estimated_cardinality) {
        children.push_back(std::move(child));
        std::cout << "Dummy:Construct" << std::endl;
    }

    OperatorResultType PhysicalDummyOperator::Execute(ExecutionContext &context,
                             DataChunk &input, 
                             DataChunk &chunk,
                             GlobalOperatorState &gstate,
                             OperatorState &state) const {
        std::cout << "Dummy:Execute" << std::endl;
        std::cout << input.ToString() << std::endl;
        std::cout << chunk.ToString() << std::endl;

        // TODO: extract annotations payload from input and persist it in memory
        chunk.SetCapacity(input);
	      chunk.SetCardinality(input);
        for (idx_t i = 0; i < input.ColumnCount()-1; i++) {
          chunk.data[i].Reference(input.data[i]);
        }

        // TODO: reindex complex annotations
        chunk.data[input.ColumnCount()-1].Sequence(0, 1, input.size());
        // TODO: remove annotations if this is the root operator
        std::cout << chunk.ToString() << std::endl;
        return OperatorResultType::NEED_MORE_INPUT;
    }

} // namespace duckdb
