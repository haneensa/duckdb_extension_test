// q2, q15, q16, q17, q18, q20 -> issues with binding, q4, q21 -> querying
// simple agg: q6, q14, q19
// q13: check if there are null values
#define DUCKDB_EXTENSION_MAIN
#include "lineage_extension.hpp"
#include "lineage_reader.hpp"
#include "lineage_meta.hpp"
#include "logical_lineage_operator.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
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
bool LineageState::debug = true;
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
void InitPipelinesOld(unique_ptr<LogicalOperator> &plan, idx_t query_id, idx_t pipeline_idx) {
    if (!plan) return;
    idx_t operator_id = 0; // maintain mapping
    switch (plan->type) {
      case  LogicalOperatorType::LOGICAL_GET: {
        std::cout << "InitPipelinesOld add: " << pipeline_idx << " " << operator_id << " " << EnumUtil::ToChars<LogicalOperatorType>(plan->type) << std::endl;
        LineageState::pipelines[query_id][pipeline_idx].push_back({operator_id, plan->type});
        break;
      } case LogicalOperatorType::LOGICAL_FILTER: {
      } case LogicalOperatorType::LOGICAL_TOP_N: {
      } case LogicalOperatorType::LOGICAL_ORDER_BY: {
      } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
        std::cout << "InitPipelinesOld add: " << pipeline_idx << " " << operator_id << " " << EnumUtil::ToChars<LogicalOperatorType>(plan->type) << std::endl;
        LineageState::pipelines[query_id][pipeline_idx].push_back({operator_id, plan->type});
        InitPipelinesOld(plan->children[0], query_id, pipeline_idx);
        break;
      } case LogicalOperatorType::LOGICAL_PROJECTION: {
        InitPipelinesOld(plan->children[0], query_id, pipeline_idx);
        break;
      } case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
        /*
          if (!op->delim_handled) {
            // TODO handle multithreading here?
            idx_t thread_id = -1;

            // set this child to join's child to appropriately line up chunk scan lineage
            dynamic_cast<PhysicalDelimJoin *>(op)->join->children[0] = move(op->children[0]);

            // distinct input is delim join input
            // distinct should be the input to delim scan
            op->lineage_op[thread_id]->children[2]->children.push_back(op->lineage_op[thread_id]->children[0]);

            // chunk scan input is delim join input
            op->lineage_op[thread_id]->children[1]->children[1] = op->lineage_op[thread_id]->children[0];

            // we only want to do the child reordering once
            op->delim_handled = true;
          }
          return GenerateCustomLineagePlan(dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), cxt, lineage_ids, move(left), simple_agg_flag, pipelines);
         */
        break;
      } case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
      } case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
      } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
        std::cout << "InitPipelines add: " << pipeline_idx << " " << operator_id << " " << EnumUtil::ToChars<LogicalOperatorType>(plan->type) << std::endl;
        LineageState::pipelines[query_id][pipeline_idx].push_back({operator_id, plan->type});

        idx_t new_pipeline_idx = LineageState::pipelines[query_id].size();
        // new pipeline use as to scan
        LineageState::pipelines[query_id].emplace_back();
        std::cout << "InitPipelines add: " << new_pipeline_idx << " " << operator_id << " " << EnumUtil::ToChars<LogicalOperatorType>(plan->type) << std::endl;
        LineageState::pipelines[query_id][new_pipeline_idx].push_back({operator_id, plan->type});
        std::cout << "InitPipelines post " << plan->children.size() << std::endl;
        
        InitPipelinesOld(plan->children[0], query_id, pipeline_idx);
        InitPipelinesOld(plan->children[1], query_id, new_pipeline_idx);
        break;
      } default: {
        std::cout << "InitPipelines no match" << EnumUtil::ToChars<LogicalOperatorType>(plan->type) << std::endl;
      }
    }

}
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
        std::cout << "1. InitPipelines add: " << pipeline_idx << " " << op.operator_id << " " << EnumUtil::ToChars<LogicalOperatorType>(op.dependent_type) << std::endl;
        LineageState::op_pipelines[table_name] = pipeline_idx;
        LineageState::pipelines[query_id][pipeline_idx].push_back({op.operator_id, op.dependent_type});
        break;
      } case LogicalOperatorType::LOGICAL_FILTER: {
      } case LogicalOperatorType::LOGICAL_TOP_N: {
      } case LogicalOperatorType::LOGICAL_ORDER_BY: {
      } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
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
        std::cout << "3. InitPipelines add: " << pipeline_idx << " " << op.operator_id << " "
          << EnumUtil::ToChars<LogicalOperatorType>(op.dependent_type) << std::endl;
        LineageState::pipelines[query_id][pipeline_idx].push_back({op.operator_id, op.dependent_type});

        idx_t new_pipeline_idx = LineageState::pipelines[query_id].size();
        // new pipeline use as to scan
        LineageState::pipelines[query_id].emplace_back();
        std::cout << "4. InitPipelines add: " << table_name << " " << new_pipeline_idx << " " << EnumUtil::ToChars<LogicalOperatorType>(op.dependent_type) << std::endl;
        LineageState::pipelines[query_id][new_pipeline_idx].push_back({op.operator_id, op.dependent_type});
        
        LineageState::op_pipelines[table_name] = pipeline_idx;
        LineageState::op_pipelines[table_name + "_right"] = new_pipeline_idx;
        std::cout << "pipeline: " << table_name << " " << pipeline_idx << " " << new_pipeline_idx << std::endl;
        InitPipelines(plan->children[0]->children[0], query_id, pipeline_idx);
        InitPipelines(plan->children[0]->children[1], query_id, new_pipeline_idx);
      } default: {}
    }
    

}

idx_t InjectLineageOperator(unique_ptr<LogicalOperator> &op,ClientContext &context, idx_t query_id) {
    if (!op) return 0;
    vector<idx_t> rowids = {};
    for (auto &child : op->children) {
        rowids.push_back( InjectLineageOperator(child, context,  query_id) );
    }

    std::cout << op->GetName() << " " << LineageState::global_id << std::endl;
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
    } else if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
      // assert rowids.size() == 1
      // add row_type to projection_map
      int col_id = rowids[0];
      int new_col_id = col_id;
      auto &filter = op->Cast<LogicalFilter>();
      if (!filter.projection_map.empty()) {
          filter.projection_map.push_back(col_id); 
          new_col_id = filter.projection_map.size()-1; 
      }
      std::cout << "Filter " << filter.projection_map.size() << " " << col_id << " " << new_col_id << std::endl;
      return new_col_id;
    } else if (op->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
      // it passes through child types. except if projections is not empty, then we need to add it
      auto &order = op->Cast<LogicalOrder>();
      std::cout << "Order by " << order.projections.size() << " " << rowids[0] << std::endl;
      if (!order.projections.empty()) {
       // order.projections.push_back(); the rowid of child
      }
      return rowids[0];
    } else if (op->type == LogicalOperatorType::LOGICAL_TOP_N) {
      // passes through child types
      return rowids[0];
    } else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
      // projection, just make sure we propagate any annotation columns
      int col_id = rowids[0];
      op->expressions.push_back(make_uniq<BoundReferenceExpression>(LogicalType::ROW_TYPE, col_id));
      int new_col_id = op->expressions.size()-1;
      if (LineageState::debug)
        std::cout << "[DEBUG] Projection types before modification: " << col_id << " " << new_col_id  << "\n";
      return new_col_id;
    } else if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
    } else if (op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
    } else if (op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
    } else if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
      // check op.join_type = {SEMI, ANTI, RIGHT_ANTI, RIGHT_SEMI, MARK}
      // Propagate annotations from the left and right sides.
      // Add PhysicaLineage to extraxt the last two columns
      // and replace it with a single annotations column
      auto &join = op->Cast<LogicalComparisonJoin>();
      std::cout << EnumUtil::ToChars<JoinType>(join.join_type) << std::endl;
      idx_t left_col_id = 0;
      idx_t right_col_id = 0;
      if (!join.left_projection_map.empty()) {
        left_col_id = join.left_projection_map.size();
		    join.left_projection_map.push_back(rowids[0]);
      } else {
        left_col_id = rowids[0];
      }
		  if (!join.right_projection_map.empty()) {
        right_col_id = join.right_projection_map.size();
		    join.right_projection_map.push_back(rowids[1]);
      } else {
        right_col_id = rowids[1];
      }

      std::cout << "-> " << left_col_id + right_col_id << " " << left_col_id << " " << right_col_id << " " << rowids[0] << " " << rowids[1] << " " << join.left_projection_map.size() << " " << join.right_projection_map.size() << std::endl;
      auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, LineageState::global_id++, query_id, op->type, left_col_id, right_col_id);
      lop->AddChild(std::move(op));
      op = std::move(lop);
      return left_col_id + right_col_id;
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

static void PragmaEnableLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::capture = true;
}

static void PragmaDisableLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::capture = false;
}

void LineageExtension::Load(DuckDB &db) {
    auto optimizer_extension = make_uniq<OptimizerExtension>();
    optimizer_extension->optimize_function = [](OptimizerExtensionInput &input, 
                                            unique_ptr<LogicalOperator> &plan) {
        if (LineageState::capture == false || plan->type == LogicalOperatorType::LOGICAL_PRAGMA) return;
        idx_t query_id = LineageState::query_id++; 
        LineageState::global_id = 0;
        if (LineageState::debug) {
          std::cout << "Plan prior to modifications" << std::endl;
          std::cout << plan->ToString() << std::endl;
        }
        idx_t final_rowid = InjectLineageOperator(plan, input.context, query_id);
        // inject lineage op at the root of the plan to extract any annotation columns
        auto root = make_uniq<LogicalLineageOperator>(plan->estimated_cardinality, LineageState::global_id++, query_id, plan->children[0]->type, final_rowid, 0, true);
        root->AddChild(std::move(plan));
        plan = std::move(root);
        
        std::cout << "Plan after to modifications" << std::endl;
        std::cout << plan->ToString() << std::endl;
        
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

    auto enable_lineage_fun = PragmaFunction::PragmaStatement("enable_lineage", PragmaEnableLineage);
    auto disable_lineage_fun = PragmaFunction::PragmaStatement("disable_lineage", PragmaDisableLineage);
    ExtensionUtil::RegisterFunction(db_instance, enable_lineage_fun);
    ExtensionUtil::RegisterFunction(db_instance, disable_lineage_fun);
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
