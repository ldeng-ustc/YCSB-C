# Yahoo! Cloud System Benchmark
# Multiple Dimension Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=1000
operationcount=1000
workload=site.ycsb.workloads.CoreWorkload

secondarykeyfieldcount=5

readallfields=true

readproportion=0.25
updateproportion=0
scanproportion=0
insertproportion=0.5
readbysecondaryproportion=0.25

requestdistribution=zipfian

