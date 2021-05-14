# Yahoo! Cloud System Benchmark
# Multiple Dimension Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=10000000
operationcount=100000
workload=site.ycsb.workloads.CoreWorkload

fieldcount=1
fieldlength=1000

zeropadding=14
secondarykeyfieldcount=1
uniquesecondarykeycount=100000000


readallfields=true

readproportion=0
updateproportion=0
scanproportion=0
insertproportion=0
readbysecondaryproportion=1

secondarykeydistribution=zipfian
requestdistribution=zipfian


