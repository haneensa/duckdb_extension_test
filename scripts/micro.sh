#!/bin/bash

current_date=$(date +"%Y%m%d_%H%M")
#current_date=$(date +"%Y%m%d")
note="exp_$current_date"
db="micro_benchmark_$note.out"
repeat=3
mkdir figures

#micro_benchmark_exp_20250317_1956.out
#rm filter_micro_db.out
#rm join_micro_db.out
#rm micro_agg_db.out
#rm micro_agg_db_v2.out
#exps="--run_filter" #--run_filter --run_agg --run_hj --run_ineq --run_hj_mtn"
#exps="--run_agg" #--run_filter --run_agg --run_hj --run_ineq --run_hj_mtn"
exps="--run_filter --run_agg --run_hj" # --run_ineq --run_hj_mtn"

# iterate over each experiment
python3 scripts/micro_run.py --notes $note --repeat $repeat --save $exps
python3 scripts/micro_run.py --notes $note --repeat $repeat --save $exps --lineage --no_persist
python3 scripts/micro_run.py --notes $note --repeat $repeat --save $exps --lineage
python3 scripts/micro_run.py --notes $note --repeat $repeat --save $exps --perm

#python3 ~/smokedduck/benchmark/smokedduck-scripts/micro_plot.py --db $db $exps

echo $note
