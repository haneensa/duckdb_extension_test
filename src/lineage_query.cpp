#include "lineage_query.hpp"
#include "lineage_extension.hpp"
#include <iostream>

namespace duckdb {

// [0, 1]
// 0->[1, 2, 4], 1->[0, 3, 5]
// [1, 2, 1, 1, 2]
// run this between aggs to replace all values with base values

idx_t get_lineage(DataChunk& output, idx_t query_id, idx_t pipeline_idx, idx_t cur_op,
    vector<int64_t> oids_list, vector<int64_t>& buffer, vector<Value>& child, bool use_child=false) {
  vector<vector<std::pair<idx_t, LogicalOperatorType>>>& qpipelines = LineageState::pipelines[query_id];
  idx_t n_pipelines = qpipelines.size();
  if (pipeline_idx > n_pipelines) return 0;
  std::cout << "get_lineage() " << use_child << " " << oids_list.size() << " " << buffer.size()
    << " " << query_id << " " << n_pipelines << " " << pipeline_idx << " " << cur_op << std::endl;
  idx_t operator_id = qpipelines[pipeline_idx][cur_op].first;
  bool is_leaf = (cur_op+1 == qpipelines[pipeline_idx].size());
  LogicalOperatorType& t = qpipelines[pipeline_idx][cur_op].second;
  string table_name = "PHYSICAL_LINEAGE_" + to_string(query_id) + "_" + to_string(operator_id);
  std::cout <<  table_name << " " << operator_id << " " << EnumUtil::ToChars<LogicalOperatorType>(t) << std::endl;
  // each chunk has an ID
  switch (t) {
    case LogicalOperatorType::LOGICAL_PROJECTION: 
    case LogicalOperatorType::LOGICAL_FILTER:
    case LogicalOperatorType::LOGICAL_TOP_N:
    case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
    case LogicalOperatorType::LOGICAL_ORDER_BY:  {
    int n = oids_list.size();
    if (use_child) n = child.size();
    for (int i=0; i < n; ++i) {
      int64_t oid = -1;
      if (use_child) {
        oid = child[i].GetValue<int64_t>();
      } else {
        oid = oids_list[i];
      }
      if  (LineageState::lineage_store[ table_name ].empty()) return 0;
      // 1: locate the chunk with that ID
      // 1: I need total size of elements.
      idx_t total_count = 0;
      idx_t index = 0;
      for (auto& p: LineageState::lineage_store[ table_name ] ) {
        total_count += p.second;
        if (oid < total_count) {
          break;
        }
        index++;
      }
      // 2: if doesnt exist return nothing
      if (oid >= total_count || LineageState::lineage_store[ table_name ].size() <= index ) return 0;
      Vector& lineage = LineageState::lineage_store[ table_name ][index].first;
      idx_t new_oid = lineage.GetValue(oid).GetValue<int64_t>();
      buffer.push_back(new_oid);
      // std::cout << "-> " << n << " " << oid << " " << new_oid << std::endl;
    }
    if (is_leaf) {
      // if this is leaf, just return vector. else, fill the buffer to get leaf ids
      //  output.data[1].Reference(lineage.GetValue(oid));
      //  construct a ListValue, appendit
      //Vector new_oid_vec(LogicalType::BIGINT, (data_ptr_t)buffer.data());
       //output.data[1].Reference( new_oid_vec );
      // std::cout << new_oid_vec.ToString(buffer.size()) << std::endl;
     // std::cout << " 1 uuu " << ListVector::GetListSize(output.data[1]) << std::endl;
      
      //Vector& vec = output.data[1].GetValue(0); //.DefaultCastAs(LogicalType::LIST(LogicalType::BIGINT));
     // auto& child_temp  = ListValue::GetChildren(output.data[1].GetValue(oid));
      //auto list_val = Value::LIST(LogicalType::BIGINT, {Value::BIGINT(9)});
      output.data[1].Initialize();
      output.data[1].SetValue(0, Value::LIST(LogicalType::BIGINT, {Value::BIGINT(9)}));
      //output.data[1].Reference( new_oid_vec );
 //     ListVector::Append(output.data[1], new_oid_vec, buffer.size(), 0);
     // std::cout << " 2 uuu " << buffer.size() << " " << ListVector::GetListSize(output.data[1]) << std::endl;
      // std::cout << output.data[1].ToString(buffer.size()) << std::endl;
     // std::cout << " 3 uuu " << std::endl;
      return 1; // buffer.size() % STANDARD_VECTOR_SIZE;
    } else {
      oids_list = std::move(buffer);
      buffer.clear();
      return get_lineage(output, query_id, pipeline_idx, cur_op+1, oids_list, buffer, child);
    }
    break;
  } case  LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
    // current assumption, single oid. if there was nested agg, then would would have multiple ids
    if (oids_list.empty() || LineageState::lineage_store[ table_name ].empty()) {
        buffer.clear();
        return 0;
    }
    int64_t oid = oids_list[0];
    Vector& lineage = LineageState::lineage_store[ table_name ][0].first;
    // std::cout << lineage.GetValue(oid).ToString() << std::endl;
    if (is_leaf) {
      // if this is leaf, just return vector. else, fill the buffer to get leaf ids
      output.data[1].Reference(lineage.GetValue(oid));
      //output.data[1].Reference(lineage.GetValue(oid));
      //child  = ListValue::GetChildren(lineage.GetValue(oid));
      //Vector new_oid_vec(LogicalType::BIGINT, (data_ptr_t)child.data());
      //output.data[1].Reference( new_oid_vec );
      return 1; //child.size() % STANDARD_VECTOR_SIZE;
    } else {
      child  = ListValue::GetChildren(lineage.GetValue(oid));
      std::cout << "GetChildren: " << child.size() << std::endl;
      return get_lineage(output, query_id, pipeline_idx, cur_op+1, oids_list, buffer, child, true);
    }
    break;
  } default: {}
  }

  return 0;
}

void LineageQuery::GetNextChunk(DataChunk& output) {
  // for each pipeline. start from the last operator?
  // 1. if specific table is specified, find the pipeline for that table
  if (cur >= oids.size()) return;
  buffer.clear();
  idx_t pipeline_idx = 0;
  idx_t cur_op = 0;
  int64_t oid = oids[cur++];
  child_buf.clear();
  idx_t count = get_lineage(output, query_id, pipeline_idx, cur_op, {oid}, buffer, child_buf, false);
  
  output.SetCardinality(count);
  Vector oid_vec(Value::BIGINT(oid));
  output.data[0].Reference(oid_vec);

  if (count == 0) return;

  
}

} // namespace duckdb
