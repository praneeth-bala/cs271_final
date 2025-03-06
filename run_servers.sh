#!/bin/bash

rm -rf out/*.json
rm -rf out/*.out

cargo build

./target/debug/proxy > out/proxy.out 2>&1 < /dev/null &

sleep 2

for i in {1..9}
do
    RUST_LOG="info" ./target/debug/server $i > out/server$i.out 2>&1 < /dev/null &
done

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM
wait
