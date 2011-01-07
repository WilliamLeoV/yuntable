#include "utils.h"
#include "item.h"
#include "list.h"
#include "master.h"
#include "rpc.h"
#include "conf.h"
#include "malloc2.h"

void testsuite(void){
	printf("The testcase rpc for master\n");
	char *table_name = "table1";
	char* master_conn =  "127.0.0.1:8301";
	List* params = generate_charactor_params(1, table_name);
	char* result = connect_conn(master_conn, REQUEST_NEW_REGION_MASTER_CMD, params);
	printf("1:%s\n", result);
	char* region_list_string = connect_conn(master_conn, GET_REGION_CONN_LIST_MASTER_CMD, params);
	printf("2:%s\n", region_list_string);
	List* regionList = string_to_list(region_list_string);
	printf("The size is %d\n", list_size(regionList));
	char* region;
	while((region = list_next(regionList)) != NULL){
		printf("%s\n", region);
	}
}

int main(int argc, char *argv[]){
	testsuite();
	return 1;
}
