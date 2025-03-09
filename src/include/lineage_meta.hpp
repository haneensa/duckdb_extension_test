#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "lineage_query.hpp"

namespace duckdb {

struct LineageMetaBindData : public TableFunctionData {
  idx_t cardinality;
  idx_t chunk_count;

  void Initialize() {
    cardinality = 0;
    chunk_count = 0;
  }
};

struct LineageMetaLocalState : public LocalTableFunctionState {
};

struct LineageMetaGlobalState : public GlobalTableFunctionState {
};

class LineageMetaFunction {
  public:
    static void LineageMetaImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    static unique_ptr<FunctionData> LineageMetaBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names);
    static unique_ptr<GlobalTableFunctionState> LineageMetaInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input);
    static unique_ptr<LocalTableFunctionState>
    LineageMetaInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p);
    static unique_ptr<TableRef> ReadLineageReplacement(ClientContext &context, ReplacementScanInput &input,
        optional_ptr<ReplacementScanData> data);
    static TableFunctionSet GetFunctionSet();
    static unique_ptr<NodeStatistics> Cardinality(ClientContext &context, const FunctionData *bind_data);
    static unique_ptr<BaseStatistics> ScanStats(ClientContext &context, const FunctionData *bind_data_p,
        column_t column_index);
};

} // namespace duckdb
