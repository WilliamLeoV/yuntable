CC		= gcc -g -Wall
CFLAGS	= -I./common/include/ -I./cli/include -I./master/include -I./region/include -lpthread
COMMON_SRC = common/src/*.c
REGION_SRC = region/src/*.c
MASTER_SRC = master/src/*.c
CLI_SRC = cli/src/*.c

all:

cli:
	$(CC) $(CFLAGS) ${COMMON_SRC} ${CLI_SRC} -o yuncli

master:
	$(CC) $(CFLAGS) ${COMMON_SRC} ${MASTER_SRC} -o startMaster

region:
	$(CC) $(CFLAGS) ${COMMON_SRC} ${REGION_SRC} -o startRegion
	
yfileTest:
	$(CC) $(CFLAGS) ${COMMON_SRC} region/yfile.c -D YFILE_TEST -o yfileTest
	
confTest:
	$(CC) $(CFLAGS)	${COMMON_SRC} -D CONF_TEST -o confTest 
	
itemTest:
	$(CC) $(CFLAGS)	${COMMON_SRC} -D ITEM_TEST -o itemTest 

listTest:
	$(CC) $(CFLAGS)	${COMMON_SRC} -D LIST_TEST -o listTest 

utilTest:
	$(CC) $(CFLAGS)	${COMMON_SRC} -D UTILS_TEST -o utilTest

rpcTest:
	$(CC) $(CFLAGS)	${COMMON_SRC} -D RPC_TEST -o rpcTest 

	
