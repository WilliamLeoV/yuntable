echo "Removing everything that generated by YunTable, and includes it's binary"
rm -r tablet*/ 
rm wal.log
rm yuncli
rm startMaster
rm startRegion
rm yfileTest
rm testMaster
rm testRegion
rm confTest
rm itemTest
rm listTest
rm utilTest
rm connTest
rm master.log
rm region.log
rm test.log
rm test.yfile

echo "Replacing the current conf file with the conf templates"
cp conf/cli.conf.template conf/cli.conf
cp conf/master.conf.template conf/master.conf
cp conf/region.conf.template conf/region.conf
