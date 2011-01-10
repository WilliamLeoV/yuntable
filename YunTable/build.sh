echo "Compiling YunTable!!!!!"
make cli -B
make master -B
make region -B
echo "Removing Some Legacy Files!!!!!, it is also fine if reports some err msg."
make clean

cp conf/cli.conf.template conf/cli.conf
cp conf/master.conf.template conf/master.conf
cp conf/region.conf.template conf/region.conf
