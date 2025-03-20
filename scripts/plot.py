import json
import pandas as pd
import argparse
from pygg import *
import duckdb
from duckdb.typing import *

legend = theme_bw() + theme(**{
  "legend.background": element_blank(), #element_rect(fill=esc("#f7f7f7")),
  "legend.justification":"c(1,0)", "legend.position":"c(1,0)",
  "legend.key" : element_blank(),
  "legend.title":element_blank(),
  "text": element_text(colour = "'#333333'", size=11, family = "'Arial'"),
  "axis.text": element_text(colour = "'#333333'", size=11),  
  "plot.background": element_blank(),
  "panel.border": element_rect(color=esc("#e0e0e0")),
  "strip.background": element_rect(fill=esc("#efefef"), color=esc("#e0e0e0")),
  "strip.text": element_text(color=esc("#333333"))
  
})
# need to add the following to ggsave call:
#    libs=['grid']
legend_bottom = legend + theme(**{
  "legend.position":esc("bottom"),
  #"legend.spacing": "unit(-.5, 'cm')"

})
legend_none = legend + theme(**{"legend.position": esc("none")})

legend_side = legend + theme(**{
  "legend.position":esc("right"),
})

type1 = [1, 3, 5, 6, 7, 8, 9, 10, 12, 13, 14, 19]
type2 = [11, 15, 16, 18]
type3 = [2, 4, 17, 20, 21, 22]

def cat(qid):
    if int(qid) in type1:
        return "1. Joins-Aggregations"
    elif int(qid) in type2:
        return "2. Uncorrelated subQs"
    else:
        return "3. Correlated subQs"


parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--db', type=str, help='queries folder', default='tpch_benchmark_capture_exp_20250316_2334.db')
args = parser.parse_args()


con = duckdb.connect(args.db)
con.create_function("cat", cat, [BIGINT], VARCHAR)
print(con.execute("select * from tpch_capture").df())
tpch_all = con.execute("""select *, cat(query) as qtype from tpch_capture""").df()
header = tpch_all.columns.tolist()
print(header)
header_unique = ["query","sf", "qtype", "lineage_type", "n_threads"]
metrics = ["runtime", "output"]
g = ','.join(header_unique)
m = ','.join(metrics)
avg_tpch = con.execute("""select {}, avg(runtime) as runtime,
                            avg(output) as output from tpch_all
                            group by {}""".format(g, g)).fetchdf()
header_unique.remove("lineage_type")
g = ','.join(header_unique)

tpch_withbaseline = con.execute(f"""select
                  t1.runtime as base_runtime,
                  t1.output as base_output,
                  t2.* from (select {g}, {m} from avg_tpch where lineage_type='Baseline') as t1
                  join avg_tpch  as t2 using ({g})
                  """).fetchdf()

tpch_metrics = con.execute("""
select {}, lineage_type, n_threads, output / base_output as fanout, output, 
(runtime-base_runtime)*1000 as overhead,
((runtime-base_runtime)/base_runtime)*100 as roverhead,
from tpch_withbaseline order by qtype, query, n_threads, lineage_type
                  """.format(g, g, g)).fetchdf()
print(tpch_metrics)

class_list = type1
class_list.extend(type2)
class_list.extend(type3)
queries_order = [""+str(x)+"" for x in class_list]
queries_order = ','.join(queries_order)

def mktemplate(overheadType, table):
    return f"""
    SELECT '{overheadType}' as overheadType, qtype,
            query as qid, sf, n_threads, output,
           lineage_type as system,
           greatest(0, overhead) as overhead, greatest(0, roverhead) as roverhead
    FROM {table}"""

template = f"""
  WITH temp as (
    {mktemplate('Execute', 'tpch_metrics')}
  ) SELECT * FROM temp {"{}"} ORDER BY overheadType desc """
where = ""
q = template.format(where)
print(q)
data = con.execute(q).fetchdf()
print(data)
data = con.execute("select * from data where system<>'Baseline'").df()
sf = 10
data_sf = con.execute(f"select * from data where system<>'Baseline' and sf={sf} and n_threads=1").df()
if 1:
    y_axis_list = ["roverhead", "overhead"]
    header = ["Relative \nOverhead %", "Overhead (ms)"]
    for idx, y_axis in enumerate(y_axis_list):
        p = ggplot(data, aes(x='qid', ymin=0, ymax=y_axis,  y=y_axis, color='system', fill='system', group='system', shape='overheadType'))
        p += geom_point(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.8), width=0.5, size=2)
        p += geom_linerange(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.8), width=0.8)
        if y_axis == 'overhead':
            p += axis_labels('Query', "{} (log)".format(header[idx]), "discrete", "log10", ykwargs=dict(breaks=[10, 100, 1000], labels=list(map(esc, ['10', '100', '1000']))))
        else:
            p += axis_labels('Query', "{} (log)".format(header[idx]), "discrete", "log10", ykwargs=dict(breaks=[20, 100, 1000], labels=list(map(esc, ['20', '100', '1000']))))
            p += geom_hline(aes(yintercept=20, linetype=esc("dotted")))
            p += geom_hline(aes(yintercept=10, linetype=esc("dotted")))
        p += legend_side
        p += facet_grid(".~sf~qtype", scales=esc("free_x"), space=esc("free_x"))
        postfix = """data$qid= factor(data$qid, levels=c({}))""".format(queries_order)
        ggsave("figures/tpch_{}.png".format(y_axis), p, postfix=postfix,  width=14, height=6, scale=0.8)

        # TODO: plot sf=20
        p = ggplot(data_sf, aes(x='qid', ymin=0, ymax=y_axis,  y=y_axis, color='system', fill='system', group='system', shape='overheadType'))
        p += geom_point(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.8), width=0.5, size=2)
        p += geom_linerange(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.8), width=0.8)
        if y_axis == 'overhead':
            p += axis_labels('Query', "{} (log)".format(header[idx]), "discrete", "log10", ykwargs=dict(breaks=[10, 100, 1000], labels=list(map(esc, ['10', '100', '1000']))))
        else:
            p += axis_labels('Query', "{} (log)".format(header[idx]), "discrete", "log10", ykwargs=dict(breaks=[20, 100, 1000], labels=list(map(esc, ['20', '100', '1000']))))
            p += geom_hline(aes(yintercept=20, linetype=esc("dotted")))
            p += geom_hline(aes(yintercept=10, linetype=esc("dotted")))
        p += legend_side
        p += facet_grid(".~qtype", scales=esc("free_x"), space=esc("free_x"))
        postfix = """data$qid= factor(data$qid, levels=c({}))""".format(queries_order)
        ggsave("figures/tpch_sample_{}.png".format(y_axis), p, postfix=postfix,  width=14, height=2.5, scale=0.8)
    
# TODO summary per system per query per sf per thread
sf_list = [1, 10, 20]
for sf in sf_list:
    for sys in ["Operator-Level"]:
        print(f"=========== {sys} {sf} ===============")
        q = f"""
    select lineage_type, sf, query,
    avg(roverhead) avg_roverhead, max(roverhead) max_roverhead, 
    avg(overhead) avg_overhead, max(overhead) max_overhead, 
    from tpch_metrics
    where  sf={sf} and lineage_type='{sys}' and n_threads=1
    group by sf, lineage_type, query
    order by sf, lineage_type, query
        """
        out = con.execute(q).df()
        print(out)
        print("=================================")


q = f"""
select lineage_type, sf, n_threads, qtype,
avg(roverhead) avg_roverhead, max(roverhead) max_roverhead, 
avg(overhead) avg_overhead, max(overhead) max_overhead, 
from tpch_metrics where lineage_type<>'Baseline' and n_threads=1
group by sf, qtype, lineage_type, n_threads
order by sf, qtype, lineage_type, n_threads
"""
out = con.execute(q).df()
print(out)
