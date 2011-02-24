#!/bin/bash

echo "Starting the testcase script for YunTable"

echo "Step 1, Unit Test"
echo "Step 1-1, Compiling Unit Test"
make itemTest -B
make utilTest -B
make listTest -B
make confTest -B
make rpcTest -B
make yfileTest -B
echo -e "\n\n"

echo "Step 2-1, Runing Unit Test"
echo -e "\n"
./itemTest
echo -e "\n"
./utilTest
echo -e "\n"
./listTest
echo -e "\n"
./confTest
echo -e "\n"
./rpcTest
echo -e "\n"
./yfileTest
echo -e "\n\n"

echo "Step 2, Short Test Case"
echo "Step 2-1, Compiling Executable"
sh build.sh
echo -e "\n"

echo "Step 2-2, Set up yuncli"

./startMaster &
master_pid=$!
sleep 2

./startRegion &
region_pid=$!
sleep 2

echo "master pid is "$master_pid
echo "region pid is "$region_pid

./yuncli -cmd add master:127.0.0.1:8301
echo ""
./yuncli -cmd add region:127.0.0.1:8302
echo -e "\n"
echo "Step 2-3 Short Test Case1: People Table"
echo "This Short Test Case is focusing the functionality"
./yuncli -cmd add table:people
echo ""
./yuncli -cmd show master
echo ""
./yuncli -cmd put table:people row:me name:"ike" sex:"male"
echo ""
./yuncli -cmd put table:people row:me1 name:"ikea" sex:"female"
echo ""
./yuncli -cmd get table:people row:me
echo ""
./yuncli -cmd show table:people
echo ""
./yuncli -cmd del table:people row:me
echo ""
./yuncli -cmd del table:people row:me1 sex
echo ""
./yuncli -cmd get table:people

kill -9 $master_pid
kill -9 $region_pid

echo "All the testcases have been "
