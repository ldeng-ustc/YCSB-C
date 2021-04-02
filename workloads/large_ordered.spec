# Yahoo! Cloud System Benchmark
# Workload F: Read-modify-write workload
#   Application example: user database, where user records are read and modified by the user or to record user activity.
#                        
#   Read/read-modify-write ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=10000
operationcount=10000
# recordcount=5
# operationcount=5
workload=site.ycsb.workloads.CoreWorkload
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
fieldlength=10000000

rocksdb.dir=data/db-large_ordered
rocksdb.encodefieldnames=false