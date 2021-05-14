if [ -z "$1" ]; then
    dir=/tmp/db
else
    dir=$1
fi

./ycsbc -db pi -P ./workloads/mdworkloadb.spec -p pi.optionsfile=./options/rocksdb.ini -p pi.dir="$dir" -p pi.threshold=0
du -sh "$dir"

./ycsbc -db pi -P ./workloads/mdworkloadb.spec -p pi.optionsfile=./options/rocksdb.ini -p pi.dir="$dir" -p pi.threshold=20000
du -sh "$dir"

./ycsbc -db pi -P ./workloads/mdworkloadb.spec -p pi.optionsfile=./options/rocksdb.ini -p pi.dir="$dir" -p pi.threshold=40000
du -sh "$dir"

./ycsbc -db pi -P ./workloads/mdworkloadb.spec -p pi.optionsfile=./options/rocksdb.ini -p pi.dir="$dir" -p pi.threshold=80000
du -sh "$dir"

./ycsbc -db pi -P ./workloads/mdworkloadb.spec -p pi.optionsfile=./options/rocksdb.ini -p pi.dir="$dir" -p pi.threshold=160000
du -sh "$dir"

./ycsbc -db pi -P ./workloads/mdworkloadb.spec -p pi.optionsfile=./options/rocksdb.ini -p pi.dir="$dir" -p pi.threshold=320000
du -sh "$dir"

./ycsbc -db pi -P ./workloads/mdworkloadb.spec -p pi.optionsfile=./options/rocksdb.ini -p pi.dir="$dir" -p pi.threshold=1000000000000
du -sh "$dir"
