#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {

// Our logical operator that extends LogicalExtensionOperator
class LogicalLineageOperator : public LogicalExtensionOperator {
public:
    explicit LogicalLineageOperator(vector<LogicalType> types, idx_t estimated_cardinality);
    string GetName() const override {
        return "LINEAGE_OPERATOR";
    }
    unique_ptr<PhysicalOperator> CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) override;

protected:
    void ResolveTypes() override;
    vector<ColumnBinding> GetColumnBindings() override;
};

}
