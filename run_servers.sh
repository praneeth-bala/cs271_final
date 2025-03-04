#!/bin/bash

rm ./*.json
rm ./*.out

cargo build

./target/debug/proxy > proxy.out 2>&1 < /dev/null &

sleep 2

for i in {1..9}
do
    RUST_LOG="debug" ./target/debug/server $i > server$i.out 2>&1 < /dev/null &
done

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM
wait
