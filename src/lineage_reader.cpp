#include "lineage_extension.hpp"
#include "lineage_reader.hpp"
#include "lineage_query.hpp"
#include <regex>

namespace duckdb {

void LineageScanFunction::LineageScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  if (!data_p.local_state) return;
  auto &data = data_p.local_state->Cast<LineageReadLocalState>();
  auto &gstate = data_p.global_state->Cast<LineageReadGlobalState>();
  auto &bind_data = data_p.bind_data->CastNoConst<LineageReadBindData>();
  std::cout << "LineageScanImplementation: " << bind_data.operator_id << " " << bind_data.query_id << std::endl;
  if (bind_data.query_id == -1) return;

  if (bind_data.operator_id != -1) { // Access all operator lineage
    idx_t total_chunks = LineageState::lineage_store[bind_data.table_name].size();
    if (bind_data.chunk_count >= total_chunks) {
      return;
    }
    output.data[0].Reference(LineageState::lineage_store[bind_data.table_name][bind_data.chunk_count].first);
    idx_t count = LineageState::lineage_store[bind_data.table_name][bind_data.chunk_count].second;
    output.SetCardinality(count);
  } else {
    // call the lineage querying function to get the end to end point lineage
    bind_data.lquery_manager.GetNextChunk(output);
  }
  bind_data.chunk_count++;
}

// table name: lineage_scan(table_name)
unique_ptr<FunctionData> LineageScanFunction::LineageScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

  auto result = make_uniq<LineageReadBindData>();
  result->Initialize();

  if (input.inputs[0].IsNull()) {
    throw BinderException("lineage_scan first parameter cannot be NULL");
  }

  string query = StringValue::Get(input.inputs[0]);
  for (char& c : query) {
			c = toupper(c);
  }

  result->table_name = query;

  std::regex pattern(R"_(\d+)_");
  std::sregex_iterator it(query.begin(), query.end(), pattern);
  std::sregex_iterator end;
  while (it != end) {
    result->operator_id = result->query_id;  // Shift second to first
    result->query_id = std::stoi(it->str());  // Update second with new match
    ++it;
  }

  std::cout << "Result: " << result->query_id << " " << result->operator_id << " " << std::endl;
  result->lquery_manager.query_id = result->query_id;

  auto list_values = ListValue::GetChildren(input.inputs[1]) ;
	for (idx_t i = 0; i < list_values.size(); i++) {
		auto &child = list_values[i];
    result->lquery_manager.oids.push_back(child.GetValue<int>());
	}

  // if the string part == QUERY : then query end to end. 
  // how to get type of the output? op1: 1D, op2: 1D, 
  // each row is prov polynomial for that row. agg would have 1D since it is for a single output group
  // its child if not another agg would be a 1D but here the dependency is for each element in parent, it maps to the element in the child
  // if its another agg: each id in the parent list would result in an array
  // values don't need to have uniform types. 
  // I can predetermind the types for each pipeline
  // agg_0: 1D, agg_1: 2D, agg_3: 2D, join_left: 1D, join_right: 1D, others: 1D
  
  // based on the type of the dependent, decide on the type of the output
  if (result->operator_id != -1) {
    if (LineageState::lineage_types[query] == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
      return_types.emplace_back(LogicalType::ROW_TYPE);
      names.emplace_back("rowid_0");
      return_types.emplace_back(LogicalType::ROW_TYPE);
      names.emplace_back("rowid_1");
    } else if (LineageState::lineage_types[query] == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
      return_types.emplace_back(LogicalType::LIST(LogicalType::ROW_TYPE));
      names.emplace_back("rowid");
    } else {
      return_types.emplace_back(LogicalType::ROW_TYPE);
      names.emplace_back("rowid");
    }
  } else {
    return_types.emplace_back(LogicalType::ROW_TYPE);
    names.emplace_back("input");
    // for each pipeline
    idx_t n_pipelines = LineageState::pipelines[result->query_id].size();
    for (int i=0; i < n_pipelines; ++i) {
      return_types.emplace_back(LogicalType::LIST(LogicalType::ROW_TYPE));
      // return_types.emplace_back(LogicalType::ROW_TYPE);
      names.emplace_back("output_"+to_string(i));
    }
    std::cout << " init types: " << return_types.size() << " " << names.size() << std::endl;

  }
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

  if (!ReplacementScan::CanReplace(table_name, {"lineage_scan"})) {
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
  // table_name/query_name: VARCHAR, lineage_ids: List(INT)
  // operator_name_{query_id}_{operator_id}
  TableFunction table_function("lineage_scan", {LogicalType::VARCHAR,  LogicalType::LIST(LogicalType::INTEGER)}, LineageScanImplementation,
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
