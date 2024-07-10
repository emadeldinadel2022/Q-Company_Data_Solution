import pyarrow.parquet as pq

table = pq.read_table('qcomp_ecosystem/data/retention_area/70c8124538b24aee8d5c052b19baa7bf-0.parquet')

print(table.to_pandas().shape)