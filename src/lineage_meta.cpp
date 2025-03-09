#include "lineage_extension.hpp"
#include "lineage_meta.hpp"

namespace duckdb {

void LineageMetaFunction::LineageMetaImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  if (!data_p.local_state) return;
  auto &data = data_p.local_state->Cast<LineageMetaLocalState>();
  auto &gstate = data_p.global_state->Cast<LineageMetaGlobalState>();
  auto &bind_data = data_p.bind_data->CastNoConst<LineageMetaBindData>();

  if (bind_data.chunk_count > 0) return;

  output.data[0].Sequence(0, 1, LineageState::query_id % STANDARD_VECTOR_SIZE);
  // TODO: add plan
  // TODO: add list of table names accessed

  output.SetCardinality(LineageState::query_id);
  bind_data.chunk_count++;
}

// table name: lineage_scan(table_name)
unique_ptr<FunctionData> LineageMetaFunction::LineageMetaBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

  auto result = make_uniq<LineageMetaBindData>();

  // optional: provide a query (equivalent to filter push down). if 'ALL' then return all.
  // result->table_name = query;

  names.emplace_back("query_id");
  return_types.emplace_back(LogicalType::INTEGER);
  
  //names.emplace_back("plan");
	//return_types.emplace_back(LogicalType::VARCHAR);

  return std::move(result);
}

unique_ptr<LocalTableFunctionState>
LineageMetaFunction::LineageMetaInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
    GlobalTableFunctionState *gstate_p) {
  return make_uniq<LineageMetaLocalState>();
}

unique_ptr<GlobalTableFunctionState> LineageMetaFunction::LineageMetaInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
    return make_uniq<LineageMetaGlobalState>();
}

unique_ptr<TableRef> LineageMetaFunction::ReadLineageReplacement(ClientContext &context, ReplacementScanInput &input,
    optional_ptr<ReplacementScanData> data) {
  auto table_name = ReplacementScan::GetFullPath(input);

  if (!ReplacementScan::CanReplace(table_name, {"lineage_meta"})) {
    return nullptr;
  }
  
  // if it has lineage as prefix
  auto table_function = make_uniq<TableFunctionRef>();
  vector<unique_ptr<ParsedExpression>> children;
  children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
  table_function->function = make_uniq<FunctionExpression>("lineage_meta", std::move(children));

  if (!FileSystem::HasGlob(table_name)) {
    auto &fs = FileSystem::GetFileSystem(context);
    table_function->alias = fs.ExtractBaseName(table_name);
  }

  return std::move(table_function);
}

unique_ptr<NodeStatistics> LineageMetaFunction::Cardinality(ClientContext &context, const FunctionData *bind_data) {
  auto &data = bind_data->CastNoConst<LineageMetaBindData>();
  return make_uniq<NodeStatistics>(10);
}
unique_ptr<BaseStatistics> LineageMetaFunction::ScanStats(ClientContext &context,
    const FunctionData *bind_data_p, column_t column_index) {
  auto &bind_data = bind_data_p->CastNoConst<LineageMetaBindData>();
  auto stats = NumericStats::CreateUnknown(LogicalType::ROW_TYPE);
  NumericStats::SetMin(stats, Value::BIGINT(0));
  NumericStats::SetMax(stats, Value::BIGINT(10));
  stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES); // depends on the type of operator
  return stats.ToUnique();
}

TableFunctionSet LineageMetaFunction::GetFunctionSet() {
  // table_name/query_name: VARCHAR, lineage_ids: List(INT)
  TableFunction table_function("lineage_meta", {LogicalType::INTEGER}, LineageMetaImplementation,
      LineageMetaBind, LineageMetaInitGlobal, LineageMetaInitLocal);

  table_function.statistics = ScanStats;
  table_function.cardinality = Cardinality;
  table_function.projection_pushdown = true;
  table_function.filter_pushdown = false;
  table_function.filter_prune = false;
  return MultiFileReader::CreateFunctionSet(table_function);
}
  
} // namespace duckdb
