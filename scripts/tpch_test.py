import json
import duckdb
import pandas as pd
import argparse

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--qid', type=int, help="sf scale", default=1)
parser.add_argument('--folder', type=str, help='queries folder', default='queries/')
args = parser.parse_args()


dbname = f'tpch_{args.sf}.db'
con = duckdb.connect(config={'allow_unsigned_extensions' : 'true'})
con.execute("CALL dbgen(sf="+str(args.sf)+");")
con.execute("PRAGMA threads=1")

qfile = f"{args.folder}q{str(args.qid).zfill(2)}.sql"
text_file = open(qfile, "r")
query = text_file.read().strip()
query = ' '.join(query.split())
text_file.close()

print(con.execute("LOAD 'build/release/repository/7f34190f3f/linux_amd64/lineage.duckdb_extension'").df())
print(con.execute("PRAGMA enable_lineage").df())
print(query)

df = con.execute(query).df()
#print(df)
print(con.execute("PRAGMA disable_lineage").df())

print(con.execute("select * from lineage_meta(-1)").df())
# pass provenance type: lineage, prov_poly
print(con.execute("select * from lineage_scan('PHYSICAL_LINEAGE_0', [0])").df())
