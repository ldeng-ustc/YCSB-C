# Yahoo! Cloud System Benchmark
# Small KV: ordered small key value pairs
#   Key size: 16B, Value size: 16B
#   load, then read-only

recordcount=1000000
operationcount=1000000
# recordcount=5
# operationcount=5
workload=com.yahoo.ycsb.workloads.CoreWorkload
insertorder=ordered
zeropadding=12


readallfields=true

readproportion=1
updateproportion=0
scanproportion=0
insertproportion=0
readmodifywriteproportion=0

requestdistribution=zipfian

fieldcount=1
fieldlength=16

rocksdb.dir=data/db-smallkv
rocksdb.encodefieldnames=false