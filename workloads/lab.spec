# Yahoo! Cloud System Benchmark
# Workload F: Read-modify-write workload
#   Application example: user database, where user records are read and modified by the user or to record user activity.
#                        
#   Read/read-modify-write ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=1000000
operationcount=1000000
# recordcount=5
# operationcount=5
workload=com.yahoo.ycsb.workloads.CoreWorkload
insertorder=hashed
zeropadding=12


readallfields=true

readproportion=0.05
updateproportion=0
scanproportion=0
insertproportion=0.95
readmodifywriteproportion=0

requestdistribution=zipfian

fieldcount=1
fieldlength=100
