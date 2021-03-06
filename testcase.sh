test_routine(){
	echo ""
	echo "Staring the Test Routine"
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
	echo ""
	echo "The Test Routine is end"
	echo ""
}


echo "Starting the testcase script for YunTable"

echo "Step 1, Unit Test"
echo "Step 1-1, Compiling Unit Test"
make itemTest -B
make utilTest -B
make listTest -B
make confTest -B
make rpcTest -B
make yfileTest -B
echo "\n\n"
echo "Step 2-1, Runing Unit Test"
echo "\n"
./itemTest
echo "\n"
./utilTest
echo "\n"
./listTest
echo "\n"
./confTest
echo "\n"
./rpcTest
echo "\n"
./yfileTest
echo "\n\n"
echo "Step 2, Short Test Case"
echo "\n"
echo "Step 2-1, Set up yuncli"
./yuncli -cmd add master:127.0.0.1:8301
echo ""
./yuncli -cmd add region:127.0.0.1:8302
echo "\n"
echo "Step 2-2 Short Test Case1: People Table"
echo "This Short Test Case is focusing the functionality"
./yuncli -cmd add table:people
echo ""
./yuncli -cmd show master

test_routine

echo "Step 3 Long Test Case"

while true;
do
sleep 10
test_routine
done

echo "All the testcases have been "
