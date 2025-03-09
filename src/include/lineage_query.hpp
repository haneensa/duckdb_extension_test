#pragma once
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

class LineageQuery {
public:
  explicit LineageQuery() : query_id(-1), cur(0) {};
  void GetNextChunk(DataChunk& output);
public:
  idx_t query_id;
  vector<int> oids;
  // have a buffer per pipeline
  vector<int64_t> buffer;
  vector<Value> child_buf;
  idx_t cur;
};

} // namespace duckdb
