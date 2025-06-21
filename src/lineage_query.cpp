// Q8 is weird
#include "lineage_query.hpp"
#include "lineage_extension.hpp"
#include <iostream>

namespace duckdb {

std::pair<int, int> LocateChunk(string table_name, idx_t oid) {
  idx_t total_count = 0;
  idx_t index = 0;
  for (auto& p: LineageState::lineage_store[ table_name ] ) {
    total_count += p.second;
    if (oid < total_count) {
      break;
    }
    index++;
  }
  if (oid >= total_count || LineageState::lineage_store[ table_name ].size() <= index ) return {-1, 0};
  idx_t offset = total_count -  LineageState::lineage_store[ table_name ][index].second;
  return {index, offset};
}

// oids_list:
// buffer:
// child: reference to temporary vector that act as oids_list if present
idx_t get_lineage(DataChunk& output, idx_t query_id, idx_t pipeline_idx, idx_t cur_op,
    vector<int64_t> oids_list, vector<int64_t>& buffer, vector<Value>& child, bool use_child=false,
    bool is_right_child=false) {
  vector<vector<std::pair<idx_t, LogicalOperatorType>>>& qpipelines = LineageState::pipelines[query_id];
  idx_t n_pipelines = qpipelines.size();
  std::cout << "get_lineage(): use_child: " << use_child << " is_right_child: " << is_right_child << " oids_list: "
    << oids_list.size() << " buffer:" << buffer.size()
    << " child: " << child.size() << " qid: " << query_id << " n: " << n_pipelines << " pidx: "
    << pipeline_idx << " cur_op: " << cur_op << std::endl;
  if (pipeline_idx >= n_pipelines) return 0;
  idx_t operator_id = qpipelines[pipeline_idx][cur_op].first;
  bool is_leaf = (cur_op+1 == qpipelines[pipeline_idx].size());
  LogicalOperatorType& t = qpipelines[pipeline_idx][cur_op].second;
  string table_name = "PHYSICAL_LINEAGE_" + to_string(query_id) + "_" + to_string(operator_id);

  idx_t col_idx = pipeline_idx+1;
  std::cout << "-> " <<  table_name << " " << operator_id << " " << EnumUtil::ToChars<LogicalOperatorType>(t) << std::endl;
  
  switch (t) {
    case LogicalOperatorType::LOGICAL_PROJECTION: 
    case LogicalOperatorType::LOGICAL_FILTER:
    case LogicalOperatorType::LOGICAL_TOP_N:
    case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
    case LogicalOperatorType::LOGICAL_ORDER_BY:  {
    vector<Value> right_child;
    // there could be issue with the temporary buffer. we may need to use a list of list to hold oids for each pipeline
    vector<int64_t> right_oids_list;
    int n = oids_list.size();
    if (use_child) n = child.size();
    // something wrong with PHYSICAL_LINEAGE_0_3
    for (int i=0; i < n; ++i) {
      int64_t oid = -1;
      if (use_child) {
        oid = child[i].GetValue<int64_t>();
      } else {
        oid = oids_list[i];
      }
      if  (LineageState::lineage_store[ table_name ].empty()) return 0;
      std::pair<int, int> index_offset = LocateChunk(table_name, oid);
      int index = index_offset.first;
      int offset = index_offset.second;
      if (index == -1) return 0;
      Vector& lineage = LineageState::lineage_store[ table_name ][index].first;
      idx_t lineage_size = LineageState::lineage_store[ table_name ][index].second;
      idx_t new_oid = lineage.GetValue(oid-offset).GetValue<int64_t>();
      if (is_leaf & use_child) {
        child[i] = Value::BIGINT(new_oid); // replace it with a new value cause it is not owned ; wouldn't this change the original val?
      } else if (is_leaf) {
        child.push_back(Value::BIGINT(new_oid));
      } else if (use_child) {
        buffer.push_back(new_oid);
      } else {
        oids_list[i] = new_oid; // replace it. this is ok because it is one to one
      }
      if (t == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        Vector& lineage_right = LineageState::lineage_store[ table_name + "_right" ][index].first;
        idx_t right_new_oid = lineage_right.GetValue(oid-offset).GetValue<int64_t>();
        if (is_leaf) {
          right_child.push_back(Value::BIGINT(right_new_oid));
        } else {
          right_oids_list.push_back(right_new_oid);
        }
      }
    }
    if (is_leaf) {
      //Vector new_oid_vec(LogicalType::BIGINT, (data_ptr_t)buffer.data());
       //output.data[1].Reference( new_oid_vec );
      // std::cout << new_oid_vec.ToString(buffer.size()) << std::endl;
      output.data[col_idx].Initialize();
      output.data[col_idx].SetValue(0, Value::LIST(LogicalType::BIGINT, child));
      buffer.clear();
    } else {
      if (use_child) {
        oids_list = std::move(buffer);
        child.clear();
      }
      buffer.clear();
      get_lineage(output, query_id, pipeline_idx, cur_op+1, oids_list, buffer, child);
    }
    if (t == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
      vector<int64_t> right_buffer;
      // the first child of the right pipeline is the same as this operator.
      // case 1: that child is leaf (the only operator), then just persist data
      // case 2: that child is not leaf, then use the current lineage from the right side as the new oids for that pipeline
      idx_t right_pipeline_idx = LineageState::op_pipelines[table_name + "_right"]; //pipeline_idx+1; 
      if (qpipelines[right_pipeline_idx].size() == 1) {
        if (!is_leaf) {
          child.clear();
          for (auto& o : right_oids_list) {
            right_child.push_back(Value::BIGINT(o));
          }
          right_oids_list.clear();
         }
        output.data[right_pipeline_idx+1].Initialize();
        output.data[right_pipeline_idx+1].SetValue(0, Value::LIST(LogicalType::BIGINT, right_child));
        // pass to the child because we did the work for the join
        buffer.clear();
        return 1; // std::max(1, (int)get_lineage(output, query_id, pipeline_idx+1, 0, oids_list, buffer, child, true, false));
      } else {
        return std::max(1, (int)get_lineage(output, query_id, right_pipeline_idx, 1, right_oids_list, right_buffer, right_child, is_leaf, true));
      }
    }
    return 1;
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
      output.data[col_idx].Reference(lineage.GetValue(oid));
      //child  = ListValue::GetChildren(lineage.GetValue(oid));
      //Vector new_oid_vec(LogicalType::BIGINT, (data_ptr_t)child.data());
      //output.data[1].Reference( new_oid_vec );
      return 1; //child.size() % STANDARD_VECTOR_SIZE;
    } else {
      child  = ListValue::GetChildren(lineage.GetValue(oid));
      return get_lineage(output, query_id, pipeline_idx, cur_op+1, oids_list, buffer, child, true);
    }
    break;
  } default: {}
  }

  return 0;
}

// start from sink: get_lineage(sink, src1, oid) -> iids s.t. src1 = join(a, b)
// last src becomes sink: get_lineage(src1, a, iids1) -> iids && get_lineage(src1, b, iids) -> iids2
void LineageQuery::GetNextChunk(DataChunk& output) {
  // for each pipeline. start from the last operator?
  // 1. if specific table is specified, find the pipeline for that table
  if (cur >= oids.size()) return;
  buffer.clear();
  idx_t pipeline_idx = 0;
  int64_t oid = oids[cur++];
  child_buf.clear();

  idx_t count = get_lineage(output, query_id, pipeline_idx, 0, {oid}, buffer, child_buf, false);
  
  output.SetCardinality(count);
  Vector oid_vec(Value::BIGINT(oid));
  output.data[0].Reference(oid_vec);

  if (count == 0) return;

  
}

} // namespace duckdb
