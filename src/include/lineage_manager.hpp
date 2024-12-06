#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

class LineageManager;

// Declaration of the global and thread_local variables
extern LineageManager* lineage_manager;


//! OperatorLineage
/*!
    OperatorLineage is xxx
*/
class OperatorLineage {
public:
	explicit OperatorLineage();
};

//! LineageManager
/*!
    LineageManager is xxx
*/
class LineageManager {
public:
	explicit LineageManager() : capture(false), persist(false) {}
  //void CreateLineageTables(ClientContext &context, PhysicalOperator *op, idx_t query_id);

public:
  bool capture;
  bool persist;

  //! map between lineage relational table name and its in-mem lineage
  std::unordered_map<std::string, shared_ptr<OperatorLineage>> table_lineage_op;
};


} // namespace duckdb
