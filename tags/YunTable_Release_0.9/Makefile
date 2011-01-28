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
	
confTest:
	$(CC) $(CFLAGS)	${UTIL_SRC} -D CONF_TEST -o confTest 
	
itemTest:
	$(CC) $(CFLAGS)	${UTIL_SRC} -D ITEM_TEST -o itemTest 

listTest:
	$(CC) $(CFLAGS)	${UTIL_SRC} -D LIST_TEST -o listTest 

utilTest:
	$(CC) $(CFLAGS)	${UTIL_SRC} -D UTILS_TEST -o utilTest

rpcTest:
	$(CC) $(CFLAGS)	${UTIL_SRC} -D RPC_TEST -o rpcTest 

	