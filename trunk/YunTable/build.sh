make cli -B
make master -B
make region -B
make clean

cp conf/cli.conf.template conf/cli.conf
cp conf/master.conf.template conf/master.conf
cp conf/region.conf.template conf/region.conf
