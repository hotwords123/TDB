#!/bin/bash

function title {
    echo -e "\033[1;34m============ $1 ============\033[0m"
}

test_name=${1:-sample}

title "Cleaning database"
(cd build/tdb/db/sys; rm -f redo.log t_redo.{data,table})

title "Starting test"
(cd build; bin/server -t mvcc &)
sleep 0.5

title "Running test"
python test/multi.py -i test/redo/$test_name.sql -o test/redo/$test_name.out -t 0 -y ./run client -q > /dev/null
sleep 0.5

title "Killing server"
port=6789
pid=$(lsof -t -i:$port)
echo "Port: $port, PID: $pid"
kill -9 $pid
sleep 0.5

title "Restarting server"
(cd build; (echo "select * from t_redo;"; echo "bye") | bin/server -P cli -t mvcc)
