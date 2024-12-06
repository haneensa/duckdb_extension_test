#include "lineage_reader.hpp"

namespace duckdb {

void LineageScanFunction::LineageScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  if (!data_p.local_state) return;
  auto &data = data_p.local_state->Cast<LineageReadLocalState>();
  auto &gstate = data_p.global_state->Cast<LineageReadGlobalState>();
  auto &bind_data = data_p.bind_data->CastNoConst<LineageReadBindData>();
  // TODO: access lineage data and return the in-memory lineage as relation
  if (bind_data.chunk_count == 0) {
    output.data[0].Sequence(10, 1, 1);
    output.SetCardinality(1);
  } else {
    return;
  }

  bind_data.chunk_count++;
}

unique_ptr<FunctionData> LineageScanFunction::LineageScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

  auto result = make_uniq<LineageReadBindData>();

  if (input.inputs[0].IsNull()) {
    throw BinderException("lineage_scan first parameter cannot be NULL");
  }

  string query = StringValue::Get(input.inputs[0]);
  for (char& c : query) {
			c = toupper(c);
  }

  std::cout << "Querying for: " << query << std::endl;

  // TODO: look for in-memory lineage the user requested
  // init result.lineage with it

  /*auto map = lineage_manager->table_lineage_op;
  if (map.find(table_name_upper) != map.end()) {
    auto lineage_op = map[table_name_upper];
  }*/

  return_types.emplace_back(LogicalType::ROW_TYPE);
  names.emplace_back("rowid");
  return std::move(result);
}

unique_ptr<LocalTableFunctionState>
LineageScanFunction::LineageScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
    GlobalTableFunctionState *gstate_p) {
  return make_uniq<LineageReadLocalState>();
}

unique_ptr<GlobalTableFunctionState> LineageScanFunction::LineageScanInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
    return make_uniq<LineageReadGlobalState>();
}

unique_ptr<TableRef> LineageScanFunction::ReadLineageReplacement(ClientContext &context, ReplacementScanInput &input,
    optional_ptr<ReplacementScanData> data) {
  auto table_name = ReplacementScan::GetFullPath(input);

  if (!ReplacementScan::CanReplace(table_name, {"lineage"})) {
    return nullptr;
  }

  // if it has lineage as prefix
  auto table_function = make_uniq<TableFunctionRef>();
  vector<unique_ptr<ParsedExpression>> children;
  children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
  table_function->function = make_uniq<FunctionExpression>("lineage_scan", std::move(children));

  if (!FileSystem::HasGlob(table_name)) {
    auto &fs = FileSystem::GetFileSystem(context);
    table_function->alias = fs.ExtractBaseName(table_name);
  }

  return std::move(table_function);
}

unique_ptr<NodeStatistics> LineageScanFunction::Cardinality(ClientContext &context, const FunctionData *bind_data) {
  auto &data = bind_data->CastNoConst<LineageReadBindData>();
  return make_uniq<NodeStatistics>(10);
}
unique_ptr<BaseStatistics> LineageScanFunction::ScanStats(ClientContext &context,
    const FunctionData *bind_data_p, column_t column_index) {
  auto &bind_data = bind_data_p->CastNoConst<LineageReadBindData>();
  auto stats = NumericStats::CreateUnknown(LogicalType::ROW_TYPE);
  NumericStats::SetMin(stats, Value::BIGINT(0));
  NumericStats::SetMax(stats, Value::BIGINT(10));
  stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES); // depends on the type of operator
  return stats.ToUnique();
}

TableFunctionSet LineageScanFunction::GetFunctionSet() {
  TableFunction table_function("lineage_scan", {LogicalType::VARCHAR}, LineageScanImplementation,
      LineageScanBind, LineageScanInitGlobal, LineageScanInitLocal);

  table_function.statistics = ScanStats;
  table_function.cardinality = Cardinality;
  // table_function.table_scan_progress = LineageProgress;
  // table_function.get_bind_info = LineageGetBindInfo;
  table_function.projection_pushdown = true;
  table_function.filter_pushdown = false;
  table_function.filter_prune = false;
  return MultiFileReader::CreateFunctionSet(table_function);
}
  
} // namespace duckdb
