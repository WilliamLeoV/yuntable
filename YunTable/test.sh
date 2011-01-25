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
echo "Step 1, Test Case"


echo "All the testcases have been "
