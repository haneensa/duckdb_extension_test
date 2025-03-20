// safe: q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, q11, q12, q13, q14, q15, q17, q18, q19, q20, q21
// 2: duckdb.duckdb.InternalException: INTERNAL Error: Failed to bind column reference "n_name" [3.2] (bindings: {#[15.0], #[2.1], #[2.2], #[0.0], #[2.3], #[1.0], #[1.2], #[1.3], #[1.4], #[1.5], #[15.0]})
// 20: semi, right_semi,delim_join (right) duckdb.duckdb.InternalException: INTERNAL Error: Failed to bind column reference "ps_suppkey" [8.2] (bindings: {#[23.0], #[8.0], #[8.1], #[23.0]})
// 17: delim join right duckdb.duckdb.InternalException: INTERNAL Error: Failed to bind column reference "l_extendedprice" [0.2] (bindings: {#[9.0], #[0.0], #[0.1], #[9.0]})
//    new: duckdb.duckdb.InternalException: INTERNAL Error: Vector::Reference used on vector of different type
//
// 22: delim_join right_anti + mark join (duckdb.duckdb.InternalException: INTERNAL Error: Vector::Reference used on vector of different type)
// q16 filter expression after mark join-> duckdb.duckdb.InternalException: INTERNAL Error: Vector::Reference used on vector of different type
// 4 and 21 are the same (right delim join) (NEED TO FIGURE HOW to agg rowids)
#define DUCKDB_EXTENSION_MAIN
#include "duckdb/main/client_context.hpp"
#include "lineage_extension.hpp"
#include "lineage_reader.hpp"
#include "lineage_meta.hpp"
#include "logical_lineage_operator.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/parser/expression_util.hpp"
#include "duckdb/main/extension_util.hpp"
#include <iostream>

namespace duckdb {

idx_t LineageState::query_id = 0;
idx_t LineageState::global_id = 0;
bool LineageState::capture = false;
bool LineageState::debug = false;
bool LineageState::persist = true;
idx_t LineageState::table_idx = 0;
std::unordered_map<string, idx_t> LineageState::op_pipelines;
std::unordered_map<string, vector<std::pair<Vector, int>>> LineageState::lineage_store;
std::unordered_map<string, LogicalOperatorType> LineageState::lineage_types;
std::unordered_map<idx_t, vector<vector<std::pair<idx_t, LogicalOperatorType>>>> LineageState::pipelines;

AggregateFunction GetListFunction(ClientContext &context) {
    auto &catalog = Catalog::GetSystemCatalog(context);
    auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
        context, DEFAULT_SCHEMA, "list"
    );
    return entry.functions.GetFunctionByArguments(context, {LogicalType::ROW_TYPE});
}

// when querying, start from the end of the operator's list. backward until we reach leaf nodes (
// the issue is for aggregates, the are n ids. I need to go back for each n to find the leaf id.
// assumption 1:1 mapping. issue if there is nested aggregates (how to handle this?)
// Q1. point query: Back(Q1, oid) -> ? what is the best output structure? reimplement ideas from Postgres
//     that take special functions to interpret the polynomials. 1) provide one for debugging
//     2) boolean, etc. (give meaning to X and + and fix annotations)
//     1) Prov Polynomials most use cases they don't want to take the data out, just evaluate the polynomial. -> FaDE like
//     Lineage(Q1, oid, tname) -> list of ids for the table
//     Lineage(Q1, oid) -> for each table name list of ids
void InitPipelines(unique_ptr<LogicalOperator> &plan, idx_t query_id, idx_t pipeline_idx) {
    if (!plan) return;
    if (plan->type != LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
      for (auto &child : plan->children) {
          // pass through
          InitPipelines(child, query_id, pipeline_idx);
      }
      return;
    }

    auto &op = plan->Cast<LogicalLineageOperator>();
    string table_name = "PHYSICAL_LINEAGE_" + to_string(query_id) + "_" + to_string(op.operator_id);
    switch (op.dependent_type) {
      case  LogicalOperatorType::LOGICAL_GET: {
        if (LineageState::debug)
          std::cout << "1. InitPipelines add: " << pipeline_idx << " " << op.operator_id << " " << EnumUtil::ToChars<LogicalOperatorType>(op.dependent_type) << std::endl;
        LineageState::op_pipelines[table_name] = pipeline_idx;
        LineageState::pipelines[query_id][pipeline_idx].push_back({op.operator_id, op.dependent_type});
        break;
      } case LogicalOperatorType::LOGICAL_FILTER: {
      } case LogicalOperatorType::LOGICAL_TOP_N: {
      } case LogicalOperatorType::LOGICAL_ORDER_BY: {
      } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
      if (LineageState::debug)
        std::cout << "2. InitPipelines add: " << pipeline_idx << " " << op.operator_id << " " << EnumUtil::ToChars<LogicalOperatorType>(op.dependent_type) << std::endl;
        LineageState::pipelines[query_id][pipeline_idx].push_back({op.operator_id, op.dependent_type});
        LineageState::op_pipelines[table_name] = pipeline_idx;
        InitPipelines(plan->children[0], query_id, pipeline_idx);
        break;
      } case LogicalOperatorType::LOGICAL_PROJECTION: {
        InitPipelines(plan->children[0], query_id, pipeline_idx);
        break;
      } case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
        break;
      } case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
      } case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
      } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
      if (LineageState::debug)
        std::cout << "3. InitPipelines add: " << pipeline_idx << " " << op.operator_id << " "
          << EnumUtil::ToChars<LogicalOperatorType>(op.dependent_type) << std::endl;
        LineageState::pipelines[query_id][pipeline_idx].push_back({op.operator_id, op.dependent_type});

        idx_t new_pipeline_idx = LineageState::pipelines[query_id].size();
        // new pipeline use as to scan
        LineageState::pipelines[query_id].emplace_back();
      if (LineageState::debug)
        std::cout << "4. InitPipelines add: " << table_name << " " << new_pipeline_idx << " " << EnumUtil::ToChars<LogicalOperatorType>(op.dependent_type) << std::endl;
        LineageState::pipelines[query_id][new_pipeline_idx].push_back({op.operator_id, op.dependent_type});
        
        LineageState::op_pipelines[table_name] = pipeline_idx;
        LineageState::op_pipelines[table_name + "_right"] = new_pipeline_idx;
      if (LineageState::debug)
        std::cout << "pipeline: " << table_name << " " << pipeline_idx << " " << new_pipeline_idx << std::endl;
        InitPipelines(plan->children[0]->children[0], query_id, pipeline_idx);
        InitPipelines(plan->children[0]->children[1], query_id, new_pipeline_idx);
      } default: {}
    }
    

}

// auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, LineageState::global_id++, query_id, op->type, 0, 0);
idx_t ProcessJoin(unique_ptr<LogicalOperator> &op, vector<idx_t>& rowids, idx_t query_id) {
  auto &join = op->Cast<LogicalComparisonJoin>();
  idx_t left_col_id = 0;
  idx_t right_col_id = 0;
  if (LineageState::debug)
  std::cout << "Process join: " << EnumUtil::ToChars<JoinType>(join.join_type) << std::endl;
  //std::cout << "LEFT " << join.children[0]->ToString() << std::endl;
  //std::cout << "RIGHT " << join.children[1]->ToString() << std::endl;
  if (join.join_type == JoinType::RIGHT_SEMI || join.join_type == JoinType::RIGHT_ANTI) {
    if (LineageState::debug)
    std::cout << "inject right semi join: " << rowids[1] << " " << join.right_projection_map.size() << std::endl;
    if (!join.right_projection_map.empty()) {
      right_col_id = join.right_projection_map.size();
      join.right_projection_map.push_back(rowids[1]);
    } else {
      right_col_id = rowids[1];
    }

    if (LineageState::debug)
    std::cout << "-> " << left_col_id + right_col_id << " " << left_col_id << " " << right_col_id << " " << rowids[0] << " " << rowids[1] << " "
      << join.left_projection_map.size() << " " << join.right_projection_map.size() << std::endl;
    auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, LineageState::global_id++, query_id, op->type, 0, right_col_id);
    lop->AddChild(std::move(op));
    op = std::move(lop);
    return right_col_id;
  }

  if (!join.left_projection_map.empty()) {
    left_col_id = join.left_projection_map.size();
    join.left_projection_map.push_back(rowids[0]);
  } else {
    left_col_id = rowids[0];
  }
  
  if (join.join_type == JoinType::MARK) {
      if (LineageState::debug)
    std::cout << "inject mark join: " << left_col_id << " " << join.left_projection_map.size() << std::endl;
    auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, LineageState::global_id++, query_id, op->type, left_col_id, 0);
    lop->AddChild(std::move(op));
    lop->mark_join = true;
    op = std::move(lop);
    // add projection?
    return left_col_id + 1 /* bool col */;
  } else if (join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI) {
      if (LineageState::debug)
    std::cout << "inject semi join: " << left_col_id << " " << join.left_projection_map.size() << std::endl;
    auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, LineageState::global_id++, query_id, op->type, left_col_id, 0);
    lop->AddChild(std::move(op));
    op = std::move(lop);
    return left_col_id;
  }

  if (!join.right_projection_map.empty()) {
    right_col_id = join.right_projection_map.size();
    join.right_projection_map.push_back(rowids[1]);
  } else {
    right_col_id = rowids[1];
  }

      if (LineageState::debug)
  std::cout << "-> " << left_col_id + right_col_id << " " << left_col_id << " " << right_col_id << " " << rowids[0] << " " << rowids[1] << " " << join.left_projection_map.size() << " " << join.right_projection_map.size() << std::endl;
  auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, LineageState::global_id++, query_id, op->type, left_col_id, right_col_id);
  lop->AddChild(std::move(op));
  op = std::move(lop);
  return left_col_id + right_col_id;
}

idx_t InjectLineageOperator(unique_ptr<LogicalOperator> &op,ClientContext &context, idx_t query_id) {
    if (!op) return 0;
    vector<idx_t> rowids = {};
    for (auto &child : op->children) {
        rowids.push_back( InjectLineageOperator(child, context,  query_id) );
    }

    if (LineageState::debug) {
      std::cout << "Inject: " << op->GetName() << " " << LineageState::global_id;
      for (int i = 0; i < rowids.size(); ++i) {
        std::cout << " -> " << rowids[i];
      }
      std::cout << std::endl;
    }
    if (op->type == LogicalOperatorType::LOGICAL_GET) {
      // leaf node. add rowid attribute to propagate.
      auto &get = op->Cast<LogicalGet>();
      get.AddColumnId(COLUMN_IDENTIFIER_ROW_ID);
      LineageState::table_idx = get.table_index;
      idx_t col_id  =  get.GetColumnIds().size() - 1;
      // projection_ids index into column_ids. if any exist then reference new column
      if (!get.projection_ids.empty()) {
        get.projection_ids.push_back(col_id);
        col_id = get.projection_ids.size() - 1;
      }
      if (LineageState::debug)
        std::cout << "LogicalGet types after injection: " << col_id << " " << op->expressions.size() 
          << " " << get.names.size() << " " << get.projection_ids.size() <<
          " " << get.returned_types.size() <<  " " << get.types.size() << " " << get.GetColumnIds().size() << "\n";
      return col_id;
    }  else if (op->type == LogicalOperatorType::LOGICAL_CHUNK_GET) { // CTE_SCAN too
      // add lineage op to generate ids
      auto& col = op->Cast<LogicalColumnDataGet>();
      idx_t col_id = col.chunk_types.size();
      if (LineageState::debug) std::cout << "chunk get " << col_id << std::endl;
      auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, LineageState::global_id++, query_id, op->type, col_id, 0);
      lop->AddChild(std::move(op));
      op = std::move(lop);
      return col_id;
    }  else if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) { // CTE_SCAN too
        if (LineageState::debug)  std::cout << " cte ? " << rowids[0] << " " << rowids[1] << std::endl;
        return rowids[1];
    }  else if (op->type == LogicalOperatorType::LOGICAL_CTE_REF) { // CTE_SCAN too
      // add lineage op to generate ids
      auto& col = op->Cast<LogicalCTERef>();
      idx_t col_id = col.chunk_types.size();
      if (LineageState::debug) std::cout << "cte ref " << col_id << " " << col.bound_columns[0] << " " << std::endl;
      col.chunk_types.push_back(LogicalType::ROW_TYPE);
      col.bound_columns.push_back("rowid");
      //auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, LineageState::global_id++, query_id, op->type, col_id, 0);
      //lop->AddChild(std::move(op));
      //op = std::move(lop);
      return col_id;
    } else if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
      auto &filter = op->Cast<LogicalFilter>();
      int col_id = rowids[0];
      int new_col_id = col_id;

      if (op->children[0]->type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
        // check if the child is mark join
        if (op->children[0]->Cast<LogicalLineageOperator>().mark_join) {
            // pull up lineage op
            auto lop = std::move(op->children[0]);
            if (LineageState::debug)
              std::cout << "pull up lineage op " << rowids[0] << " " 
            << filter.expressions.size() << " " << filter.projection_map.size() << " " << 
            lop->Cast<LogicalLineageOperator>().left_rid << std::endl;
            lop->Cast<LogicalLineageOperator>().dependent_type = op->type;
      
            if (!filter.projection_map.empty()) {
              filter.projection_map.push_back(col_id); 
              new_col_id = filter.projection_map.size()-1; 
            }
            lop->Cast<LogicalLineageOperator>().left_rid = new_col_id;
            op->children[0] = std::move(lop->children[0]);
            lop->children[0] = std::move(op);
            op = std::move(lop);
            return rowids[0];
        }
      }
      if (!filter.projection_map.empty()) {
          filter.projection_map.push_back(col_id); 
          new_col_id = filter.projection_map.size()-1; 
      }
      if (LineageState::debug)
      std::cout << "Filter " << filter.expressions.size() << " " << filter.projection_map.size() << " " << col_id << " " << new_col_id << std::endl;
      return new_col_id;
    } else if (op->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
      // it passes through child types. except if projections is not empty, then we need to add it
      auto &order = op->Cast<LogicalOrder>();
      if (LineageState::debug)
      std::cout << "Order by " << order.projections.size() << " " << rowids[0] << std::endl;
      if (!order.projections.empty()) {
       // order.projections.push_back(); the rowid of child
      }
      return rowids[0];
    } else if (op->type == LogicalOperatorType::LOGICAL_TOP_N) {
      // passes through child types
      return rowids[0];
    } else if (op->type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
      if (rowids.size() > 0)  return rowids[0];
      else return 0;
    } else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
      // projection, just make sure we propagate any annotation columns
      int col_id = rowids[0];
      op->expressions.push_back(make_uniq<BoundReferenceExpression>(LogicalType::ROW_TYPE, col_id));
      int new_col_id = op->expressions.size()-1;
      if (LineageState::debug)
        std::cout << "[DEBUG] Projection types before modification: " << col_id << " " << new_col_id  << "\n";
      return new_col_id;
    } else if (op->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
      // duplicate eliminated scan (output of distinct)
      auto &get = op->Cast<LogicalDelimGet>();
      if (LineageState::debug)
        std::cout << "LogicalDelimGet types after injection: " << get.table_index << " " << get.chunk_types.size() << std::endl;
      int col_id = get.chunk_types.size();
      get.chunk_types.push_back(LogicalType::LIST(LogicalType::ROW_TYPE));
      auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, LineageState::global_id++, query_id, op->type, col_id, 0);
      lop->AddChild(std::move(op));
      op = std::move(lop);
      return get.chunk_types.size()-1; // TODO: adjust once I adjust distinct types
    } else if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
      // the JOIN right child, becomes right_delim_join child that is used as input to
      // JOIN and DISTINCT
      // 1) access to distinct to add LIST(rowid) expression
      // 2) JOIN to add annotations from both sides
      // the fist n childrens are n delim scans
      return ProcessJoin(op, rowids, query_id);
    } else if (op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
    } else if (op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
    } else if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
      // check op.join_type = {SEMI, ANTI, RIGHT_ANTI, RIGHT_SEMI, MARK}
      // Propagate annotations from the left and right sides.
      // Add PhysicaLineage to extraxt the last two columns
      // and replace it with a single annotations column
      return ProcessJoin(op, rowids, query_id);
    } else if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
        auto &aggr = op->Cast<LogicalAggregate>();
       // if (!aggr.groups.empty()) {
            if (LineageState::debug) std::cout << "[DEBUG] Modifying Aggregate operator\n";
            auto list_function = GetListFunction(context);
            auto rowid_colref = make_uniq_base<Expression, BoundReferenceExpression>(LogicalType::ROW_TYPE, rowids[0]);
            vector<unique_ptr<Expression>> children;
            children.push_back(std::move(rowid_colref));
            unique_ptr<FunctionData> bind_info = list_function.bind(context, list_function, children);
            auto list_aggregate = make_uniq<BoundAggregateExpression>(
                list_function, std::move(children), nullptr, std::move(bind_info),
                AggregateType::NON_DISTINCT
            );

            aggr.expressions.push_back(std::move(list_aggregate));
            idx_t new_col_id = aggr.groups.size() + aggr.expressions.size() + aggr.grouping_functions.size() - 1;
            
            auto dummy = make_uniq<LogicalLineageOperator>(aggr.estimated_cardinality, LineageState::global_id++, query_id, op->type, new_col_id, 0);
            dummy->AddChild(std::move(op));

            op = std::move(dummy);
            
            return new_col_id;
      //  } // if simple agg, add operator below to remove annotations, and operator above to generate annotations
    }
    return 0;
}

std::string LineageExtension::Name() {
    return "lineage";
}

static void PragmaClearLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::op_pipelines.clear();
  LineageState::lineage_store.clear();
  LineageState::lineage_types.clear();
  LineageState::pipelines.clear();
}

static void PragmaEnablePersistLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::persist = true;
}

static void PragmaDisablePersistLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::persist = false;
}


static void PragmaEnableLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::capture = true;
}

static void PragmaDisableLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::capture = false;
}

static void PragmaDisableFilterPushDown(ClientContext &context, const FunctionParameters &parameters) {
  LineageGlobal::enable_filter_pushdown = false;
  std::cout << "Disable Filter Pushdown" << std::endl;
}

static void PragmaEnableFilterPushDown(ClientContext &context, const FunctionParameters &parameters) {
  LineageGlobal::enable_filter_pushdown = true;
	std::cout << "Enable Filter Pushdown" << std::endl;
}

static string PragmaSetJoin(ClientContext &context, const FunctionParameters &parameters) {
	string join_type = parameters.values[0].ToString();
	D_ASSERT(join_type == "hash" || join_type == "merge" || join_type == "nl" || join_type == "index" || join_type == "block" || join_type == "clear");
	std::cout << "Setting join type to " << join_type << " - be careful! Failures possible for hash/index join if non equijoin." << std::endl;
	if (join_type == "clear") {
    LineageGlobal::explicit_join_type = "";
	} else {
    LineageGlobal::explicit_join_type = join_type;
	}
  return "select 1";
}

static void PragmaSetAgg(ClientContext &context, const FunctionParameters &parameters) {
	string agg_type = parameters.values[0].ToString();
	D_ASSERT(agg_type == "perfect" || agg_type == "reg" || agg_type == "clear");
	std::cout << "Setting agg type to " << agg_type << " - be careful! Failures possible if too many buckets (I think)." << std::endl;
	if (agg_type == "clear") {
    LineageGlobal::explicit_agg_type = "";
	} else {
    LineageGlobal::explicit_agg_type = agg_type;
	}
}


void LineageExtension::Load(DuckDB &db) {
    auto optimizer_extension = make_uniq<OptimizerExtension>();
    optimizer_extension->optimize_function = [](OptimizerExtensionInput &input, 
                                            unique_ptr<LogicalOperator> &plan) {
        if (LineageState::capture == false || plan->type == LogicalOperatorType::LOGICAL_PRAGMA
            || plan->type == LogicalOperatorType::LOGICAL_SET) return;
        idx_t query_id = LineageState::query_id++; 
        LineageState::global_id = 0;
        if (LineageState::debug) {
          std::cout << "Plan prior to modifications" << std::endl;
          std::cout << plan->ToString() << std::endl;
        }
        idx_t final_rowid = InjectLineageOperator(plan, input.context, query_id);
        // inject lineage op at the root of the plan to extract any annotation columns
        // If root is create table, then add lineage operator below it
        auto root = make_uniq<LogicalLineageOperator>(plan->estimated_cardinality, LineageState::global_id++, query_id, plan->children[0]->type, final_rowid, 0, true);
        if (plan->type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
          auto child = std::move(plan->children[0]);
          root->AddChild(std::move(child));
          plan->children[0] = std::move(root);
        } else {
          root->AddChild(std::move(plan));
          plan = std::move(root);
        }
        if (LineageState::debug) {
          std::cout << "Plan after to modifications" << std::endl;
          std::cout << plan->ToString() << std::endl;
        } 
        LineageState::pipelines[query_id].emplace_back();
        InitPipelines(plan, query_id, 0);

        // LineageState::pipelines[10].emplace_back();
        // InitPipelinesOld(plan, 10, 0);
    };

    auto &db_instance = *db.instance;
    db_instance.config.optimizer_extensions.emplace_back(*optimizer_extension);
    std::cout << "Lineage extension loaded successfully.\n";
    
  	ExtensionUtil::RegisterFunction(db_instance, LineageScanFunction::GetFunctionSet());
  	ExtensionUtil::RegisterFunction(db_instance, LineageMetaFunction::GetFunctionSet());

    auto clear_lineage_fun = PragmaFunction::PragmaStatement("clear_lineage", PragmaClearLineage);
    auto enable_persist_fun = PragmaFunction::PragmaStatement("enable_persist_lineage", PragmaEnablePersistLineage);
    auto disable_persist_fun = PragmaFunction::PragmaStatement("disable_persist_lineage", PragmaDisablePersistLineage);
    auto enable_lineage_fun = PragmaFunction::PragmaStatement("enable_lineage", PragmaEnableLineage);
    auto disable_lineage_fun = PragmaFunction::PragmaStatement("disable_lineage", PragmaDisableLineage);
    auto enable_filter_scan = PragmaFunction::PragmaStatement("enable_filter_pushdown", PragmaEnableFilterPushDown);
    auto disable_filter_scan = PragmaFunction::PragmaStatement("disable_filter_pushdown", PragmaDisableFilterPushDown);
	  auto set_join_fun = PragmaFunction::PragmaCall("set_join", PragmaSetJoin, {LogicalType::VARCHAR});
	  auto set_agg_fun = PragmaFunction::PragmaCall("set_agg", PragmaSetAgg, {LogicalType::VARCHAR});

    ExtensionUtil::RegisterFunction(db_instance, clear_lineage_fun);
    ExtensionUtil::RegisterFunction(db_instance, enable_persist_fun);
    ExtensionUtil::RegisterFunction(db_instance, disable_persist_fun);
    ExtensionUtil::RegisterFunction(db_instance, enable_lineage_fun);
    ExtensionUtil::RegisterFunction(db_instance, disable_lineage_fun);
    ExtensionUtil::RegisterFunction(db_instance, enable_filter_scan);
    ExtensionUtil::RegisterFunction(db_instance, disable_filter_scan);
    ExtensionUtil::RegisterFunction(db_instance, set_join_fun);
    ExtensionUtil::RegisterFunction(db_instance, set_agg_fun);
    // JSON replacement scan
    auto &config = DBConfig::GetConfig(*db.instance);
    config.replacement_scans.emplace_back(LineageScanFunction::ReadLineageReplacement);
}

extern "C" {
DUCKDB_EXTENSION_API void lineage_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::LineageExtension>();
}

DUCKDB_EXTENSION_API const char *lineage_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

} // namespace duckdb
