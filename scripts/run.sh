#!/bin/bash

current_date=$(date +"%Y%m%d_%H%M")
note="exp_$current_date"
sf_list=("1"  "10") # "20")
repeat=3

for sf in ${sf_list[@]}
do
  python3 scripts/tpch_capture.py $note --repeat $repeat  --save_csv --csv_append --sf $sf
  python3 scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf --lineage
  python3 scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf  --lineage --no_persist
  python3 scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf  --lineage --hybrid
  python3 scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf  --lineage --hybrid --no_persist
  #python3 scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf --perm
  #python3 scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf --perm --opt
  #python3 scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf --gprom
done

echo $note
