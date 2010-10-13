CC		= gcc -g -Wall
CFLAGS	= -I include/ -lpthread
UTIL_SRC = util/*.c
REGION_SRC = region/*.c
MASTER_SRC = master/*.c
CLI_SRC = cli/*.c

all:

cli:
	$(CC) $(CFLAGS) ${UTIL_SRC} ${CLI_SRC} -o yuncli

master:
	$(CC) $(CFLAGS) ${UTIL_SRC} ${MASTER_SRC} -o startMaster

region:
	$(CC) $(CFLAGS) ${UTIL_SRC} ${REGION_SRC} -o startRegion
	
yfileTest:
	$(CC) $(CFLAGS) ${UTIL_SRC} region/yfile.c -D YFILE_TEST -o yfileTest
	
testMaster:
	$(CC) $(CFLAGS) ${UTIL_SRC} testcase/master_testcase.c -o testMaster
	
testRegion:
	$(CC) $(CFLAGS) ${UTIL_SRC} testcase/region_testcase.c -o testRegion
	
confTest:
	$(CC) $(CFLAGS)	${UTIL_SRC} -D CONF_TEST -o confTest 
	
itemTest:
	$(CC) $(CFLAGS)	${UTIL_SRC} -D ITEM_TEST -o itemTest 

listTest:
	$(CC) $(CFLAGS)	${UTIL_SRC} -D LIST_TEST -o listTest 

utilTest:
	$(CC) $(CFLAGS)	${UTIL_SRC} -D UTILS_TEST -o utilTest

connTest:
	$(CC) $(CFLAGS)	${UTIL_SRC} -D CONN_TEST -o connTest 

clean:
	rm -r tablet*/ wal.log

	