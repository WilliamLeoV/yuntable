#include "global.h"
#include "msg.h"
#include "item.h"
#include "utils.h"
#include "master.h"
#include "region.h"
#include "rpc.h"
#include "conf.h"
#include "malloc2.h"
#include "buf.h"
#include "log.h"

#define CONF_MASTER_CONN_KEY "master"

/** cli related constants **/
#define ADD_KEY "add"
#define GET_KEY "get"
#define SHOW_KEY "show"
#define PUT_KEY "put"
#define ROW_KEY "row"
#define QUIT_KEY "quit"
#define TABLE_KEY "table"

#define DEFAULT_YUNTABLE_CLI_PREFIX "yuncli:"
//the unit is MB
#define MIN_DISK_AVAIL_SPACE 10
//the unit of below two items are sec
#define DEFAULT_CLI_DAEMON_SLEEP_INTERVAL 10 * 60
#define CLI_BUF_SIZE 10000

typedef struct _CliCache{
		char* conf_path; /** default place is conf/cli.conf **/
		char* master_conn;
		List* tableInfoList;
}CliCache;

CliCache* cliCacheInst = NULL;

private char* get_table_name(char *string){
        char *table_name = NULL;
        Tokens *fenn_tokens = init_tokens(string, ':');
        if(match(fenn_tokens->tokens[0], TABLE_KEY)) table_name = m_cpy(fenn_tokens->tokens[1]);
        free_tokens(fenn_tokens);
        return table_name;
}

/** If can not get the table info from master, the system will return NULL **/
private TableInfo* get_table_info_from_master(char* master_conn, char* table_name){
        TableInfo* tableInfo = NULL;
        List* params = generate_charactor_params(1, table_name);
        RPCRequest* rpcRequest = create_rpc_request(GET_TABLE_INFO_MASTER_CMD, params);
        RPCResponse* rpcResponse = connect_conn(master_conn, rpcRequest);
        if(get_status_code(rpcResponse) == SUCCESS){
        	char* table_info_string = get_result(rpcResponse);
            if(table_info_string != NULL){
    			List* lines = generate_list_by_token(table_info_string, LINE_SEPARATOR);
    			tableInfo = string_to_table_info(table_name, lines);
    			list_destory(lines, just_free);
            }
        }else{
        	printf("Sad News! The Master node has problem!!!\n");
        }
        destory_rpc_request(rpcRequest);
        destory_rpc_response(rpcResponse);
        return tableInfo;
}

private void update_table_info_list(CliCache* cliCache){
        List* oldTableInfoList = cliCache->tableInfoList;
        int i, size=list_size(oldTableInfoList);
        List* newTableInfoList = list_create();
        for(i=0; i<size; i++){
			TableInfo* oldTableInfo = list_get(oldTableInfoList, i);
			TableInfo* newTableInfo = get_table_info_from_master(cliCache->master_conn, oldTableInfo->table_name);
			if(newTableInfo == NULL) continue;
			list_append(newTableInfoList, newTableInfo);
        }
        cliCache->tableInfoList = newTableInfoList;
        list_destory(oldTableInfoList, destory_table_info);
}

/** if the table info can't not find the tableInfoList, the method will contact master and get new, also will update the local cache **/
private TableInfo* search_and_update_table_info(CliCache* cliCache, char* table_name){
        TableInfo *tableInfo = get_table_info(cliCache->tableInfoList, table_name);
        if(tableInfo == NULL){
			tableInfo = get_table_info_from_master(cliCache->master_conn, table_name);
			if(tableInfo != NULL) list_append(cliCache->tableInfoList, tableInfo);
        }
        return tableInfo;
}

private boolean create_new_table(char* master_conn, char* table_name){
		boolean result = false;
        List* params = generate_charactor_params(1, table_name);
        RPCRequest* rpcRequest = create_rpc_request(CREATE_NEW_TABLE_MASTER_CMD, params);
        RPCResponse* rpcResponse = connect_conn(master_conn, rpcRequest);
	    if(get_status_code(rpcResponse) == SUCCESS || get_result_length(rpcResponse) > 0){
	    	result = stob(get_result(rpcResponse));
	    }else{
	    	printf("Sad News! The Master node has problem!!!\n");
	    }
	    destory_rpc_request(rpcRequest);
	    destory_rpc_response(rpcResponse);
        return result;
}

private boolean add_new_region(char* master_conn, char* region_conn){
		boolean result = false;
		List* params = generate_charactor_params(1, region_conn);
        RPCRequest* rpcRequest = create_rpc_request(ADD_NEW_REGION_MASTER_CMD, params);
		RPCResponse* rpcResponse = connect_conn(master_conn, rpcRequest);
	    if(get_status_code(rpcResponse) == SUCCESS || get_result_length(rpcResponse) > 0){
	    	result = stob(get_result(rpcResponse));
	    }else{
	    	printf("Sad News! The Master node has problem!!!\n");
	    }
		destory_rpc_request(rpcRequest);
		destory_rpc_response(rpcResponse);
		return result;
}

/** Will send a msg to the master node and to check the validity of reported region node **/
private boolean check_problem_region(char* problem_region_conn){
		printf("The Region Node %s may have some problem.", problem_region_conn);
		boolean result = false;
		printf("Now let master check the validity of %s.\n", problem_region_conn);
		List* params = generate_charactor_params(1, problem_region_conn);
	    RPCRequest* rpcRequest = create_rpc_request(CHECK_PROBLEM_REGION_MASTER_CMD, params);
		RPCResponse* rpcResponse = connect_conn(problem_region_conn, rpcRequest);
	    if(get_status_code(rpcResponse) == SUCCESS || get_result_length(rpcResponse) > 0){
	    	return stob(get_result(rpcResponse));
		}else{
			printf("Sad News! The Master node has problem too!!!\n");
		}
		destory_rpc_request(rpcRequest);
		destory_rpc_response(rpcResponse);
		return result;
}

/**
 * sample cmd: add master:172.0.0.1:8301
 * sample cmd: add region:172.0.0.1:8302
 * sample cmd: add table:people
 */
public char* add(Tokens* space_tokens){
		char* msg = NULL;
		Tokens *fenn_tokens = init_tokens(space_tokens->tokens[1], ':');
		if(match(fenn_tokens->tokens[0], MASTER_KEY)){
			char* new_master_conn = m_cats(3, fenn_tokens->tokens[1], ":", fenn_tokens->tokens[2]);
			if(check_node_validity(new_master_conn, MASTER_KEY)){
				cliCacheInst->master_conn = new_master_conn;
				flush_key_value(cliCacheInst->conf_path, CONF_MASTER_CONN_KEY, cliCacheInst->master_conn);
				msg = SUCC_MSG_COMPLETED;
			}else{
				msg = ERR_MSG_WRONG_MASTER;
				free2(new_master_conn);
			}
		}else if(match(fenn_tokens->tokens[0], REGION_KEY)){
			//step 0:check if there is master
			if(cliCacheInst->master_conn == NULL){
				return ERR_MSG_NO_MASTER;
			}
			char* new_region_conn = m_cats(3, fenn_tokens->tokens[1], ":", fenn_tokens->tokens[2]);
			if(add_new_region(cliCacheInst->master_conn, new_region_conn)){
				msg = SUCC_MSG_COMPLETED;
			}else{
				msg = ERR_MSG_WRONG_REGION;
			}
			free2(new_region_conn);
		}else {
			if(cliCacheInst->master_conn == NULL){
				return ERR_MSG_NO_MASTER;
			}
			char* new_table_name = fenn_tokens->tokens[1];
			if(new_table_name == NULL){
				 return ERR_MSG_NO_TABLE_NAME;
			}
			TableInfo* tableInfo = search_and_update_table_info(cliCacheInst, new_table_name);
			if(tableInfo != NULL){
				 return ISSUE_MSG_TABLE_ALREADY_EXISTED;
			}
			if(create_new_table(cliCacheInst->master_conn, new_table_name)){
				msg = SUCC_MSG_TABLE_CREATED;
				//Update the table info
				search_and_update_table_info(cliCacheInst, new_table_name);
			}else{
				msg = ERR_MSG_CLUSTER_FULL;
			}
		}
		//all add method related with table structure, so need to update the table info afte the method
		update_table_info_list(cliCacheInst);
		free_tokens(fenn_tokens);
		return msg;
}

/** the handler methid for put, which will separate the result set by column family **/
private char* batch_put_data_to_table(CliCache* cliCache, TableInfo* tableInfo, ResultSet* resultSet){
		char* msg = SUCC_MSG_COMPLETED;
		int i=0, rq_size=list_size(tableInfo->replicaQueueList);
		for(i=0; i<rq_size; i++){
			ReplicaQueue* replicaQueue = list_get(tableInfo->replicaQueueList, i);
			TabletInfo* tabletInfo = list_get(replicaQueue->tabletInfoList, list_size(replicaQueue->tabletInfoList) -1);
			if(tabletInfo == NULL){
				printf("Can not the working tablet for replicaQueue No'%d.\n",i);
				continue;
			}
			List* params = generate_charactor_params(1, tableInfo->table_name);
			Buf* buf = result_set_to_byte(resultSet);
			add_param(params, get_buf_index(buf), get_buf_data(buf));
			RPCRequest* rpcRequest = create_rpc_request(PUT_DATA_REGION_CMD, params);
		    RPCResponse* rpcResponse = connect_conn(tabletInfo->regionInfo->conn, rpcRequest);
			if(get_status_code(rpcResponse) != SUCCESS || get_result_length(rpcResponse) <= 0
							|| stob(get_result(rpcResponse)) == false ){
				printf("The Connection to Region Node %s have encountered some problems.\n", tabletInfo->regionInfo->conn);
				msg = ERR_MSG_PUT;
				if(check_problem_region(tabletInfo->regionInfo->conn) == true){
					printf("The Region Node %s has some problem, and is fixing now.\n", tabletInfo->regionInfo->conn);
				}
			}
		    destory_rpc_request(rpcRequest);
			destory_rpc_response(rpcResponse);
			free_buf(buf);
		}
		return msg;
}

/** sample cmd:put table:people row:me name:"ike" sex:"male" homeaddress:"sh" **/
public char* put(Tokens* space_tokens){
		char *table_name = get_table_name(space_tokens->tokens[1]);
		if(table_name == NULL) return ERR_MSG_NO_TABLE_NAME;
		TableInfo *tableInfo = search_and_update_table_info(cliCacheInst, table_name);
		if(tableInfo == NULL) return ERR_MSG_TABLE_NOT_CREATED;
		int i=0, j=0;
		if(space_tokens->size < 4) return ERR_MSG_NO_COLUMN;
		int item_size = space_tokens->size - 3;
		Item **items = malloc2(sizeof(Item *) * item_size);
		//get row name
		Tokens *fenn_tokens = init_tokens(space_tokens->tokens[2], ':');
		if(fenn_tokens->size < 2 || !match(fenn_tokens->tokens[0], ROW_KEY))
				return ERR_MSG_NO_ROW_KEY;
		char *rowname = m_cpy(fenn_tokens->tokens[1]);
		free_tokens(fenn_tokens);
		//get column value
		for(i=3, j=0; j<item_size; i++, j++){
			Tokens *inner_fenn_tokens = init_tokens(space_tokens->tokens[i], ':');
			char* column_name = inner_fenn_tokens->tokens[0];
			char *value = trim(inner_fenn_tokens->tokens[1], '"');
			items[j] = m_create_item(rowname, column_name, value);
			free_tokens(inner_fenn_tokens);
		}
		//commit
		ResultSet *resultSet = m_create_result_set(j, items);
		char *msg = batch_put_data_to_table(cliCacheInst, tableInfo, resultSet);
		destory_result_set(resultSet);
		free2(rowname);
		return msg;
}

/* if row_key == NULL, will return all the data belong to the table */
private ResultSet* query_table(CliCache *cliCache, TableInfo* tableInfo, char *row_key){
		ResultSet *resultSet = m_create_result_set(0, NULL);
		//the list for temporary stored the connParams, which will be further freed
		int i=0, rq_size=list_size(tableInfo->replicaQueueList);
		for(i=0; i<rq_size; i++){
			ReplicaQueue* replicaQueue = list_get(tableInfo->replicaQueueList, i);
			int j=0, tt_size=list_size(replicaQueue->tabletInfoList);
			for(j=0; j<tt_size; j++){
				TabletInfo* tabletInfo = list_get(replicaQueue->tabletInfoList, j);
				List* params = generate_charactor_params(2, tableInfo->table_name, row_key);
				RPCRequest* rpcRequest = create_rpc_request(QUERY_ROW_REGION_CMD, params);
				RPCResponse* rpcResponse = connect_conn(tabletInfo->regionInfo->conn, rpcRequest);
				if(get_status_code(rpcResponse) == SUCCESS || get_result_length(rpcResponse) != 0){
					ResultSet* set1 = (ResultSet*) byte_to_result_set(get_result(rpcResponse));
					ResultSet* set2 = m_combine_result_set(resultSet, set1);

					free_result_set(set1);
					free_result_set(resultSet);
					resultSet = set2;
				}else{
					printf("The Connection to Region Node %s have encountered some problems.\n", tabletInfo->regionInfo->conn);
					if(check_problem_region(tabletInfo->regionInfo->conn) == true){
						printf("The Region Node %s has some problem, and is fixing now.\n", tabletInfo->regionInfo->conn);
					}
				}
				destory_rpc_request(rpcRequest);
				destory_rpc_response(rpcResponse);
			}
		}
		return resultSet;
}


/** sample cmd: get table:people row:me **/
public char* get(Tokens* space_tokens){
		char *msg = NULL;
    	char *table_name = get_table_name(space_tokens->tokens[1]);
        if(table_name == NULL) {
			return ERR_MSG_NO_TABLE_NAME;
        }
        TableInfo *tableInfo = search_and_update_table_info(cliCacheInst, table_name);
        if (tableInfo == NULL) {
			return ERR_MSG_TABLE_NOT_CREATED;
        }
        ResultSet* resultSet = NULL;
        if(space_tokens->size <= 2){
        	return ERR_MSG_CMD_NOT_COMPLETE;
        }else{
			Tokens *fenn_tokens = init_tokens(space_tokens->tokens[2],':');
			if(!match(fenn_tokens->tokens[0], ROW_KEY))
				return ERR_MSG_NO_ROW_KEY;
			char *row_key = m_cpy(fenn_tokens->tokens[1]);
			resultSet = query_table(cliCacheInst, tableInfo, row_key);
			free_tokens(fenn_tokens);
        }
        if(resultSet->size == 0){
			msg = ISSUE_MSG_NOTHING_FOUND;
        }else {
			print_result_set_in_nice_format(resultSet);
        }
        destory_result_set(resultSet);

        return msg;
}


private void get_table_metadata(TableInfo* tableInfo){
		printf("The Meta Info about the Table:%s\n", tableInfo->table_name);
		printf("\n");
		int i=0, rq_size=list_size(tableInfo->replicaQueueList);
		for(i=0; i<rq_size; i++){
			printf("The Meta Info from Replica Queue %d.\n", i);
			printf("\n");
			ReplicaQueue* replicaQueue = list_get(tableInfo->replicaQueueList, i);
			int k=0, tt_size=list_size(replicaQueue->tabletInfoList);
			for(k=0; k<tt_size; k++){
				printf("The Meta Info from Tablet %d.\n", k);
				TabletInfo* tabletInfo = list_get(replicaQueue->tabletInfoList, k);
				List* params = generate_charactor_params(1, tableInfo->table_name);
				RPCRequest* rpcRequest = create_rpc_request(GET_METADATA_REGION_CMD, params);
			    RPCResponse* rpcResponse = connect_conn(tabletInfo->regionInfo->conn, rpcRequest);
				if(get_status_code(rpcResponse) == SUCCESS || get_result_length(rpcResponse) > 0){
					printf("%s", get_result(rpcResponse));
				}else{
					printf("Sad News! The Region node has some connection problem!!!\n");
				}
				destory_rpc_request(rpcRequest);
				destory_rpc_response(rpcResponse);
			}
		}
}

/** sample cmd: show table:people **/
public char* show(Tokens* space_tokens){
	   char *table_name = get_table_name(space_tokens->tokens[1]);
		if(table_name == NULL) {
			return ERR_MSG_NO_TABLE_NAME;
		}
		TableInfo *tableInfo = search_and_update_table_info(cliCacheInst, table_name);
		if (tableInfo == NULL) {
			return ERR_MSG_TABLE_NOT_CREATED;
		}
		get_table_metadata(tableInfo);
		return NULL;
}

public char* process(char *cmd){
        if(cmd == NULL && strlen(cmd) < 3)
            return ERR_MSG_NULL_STRING;
        char *msg = NULL;
        Tokens* space_tokens = init_tokens(cmd, ' ');
        if(space_tokens->size < 2) return ERR_MSG_NULL_STRING;
        char* action = m_cpy(space_tokens->tokens[0]);
        if(match(action, ADD_KEY)){
			msg = add(space_tokens);
        }else if(match(action, PUT_KEY)){
			msg = put(space_tokens);
        }else if(match(action, GET_KEY)){
			msg = get(space_tokens);
        }else if(match(action, SHOW_KEY)){
			msg = show(space_tokens);
        }else{
			msg = ERR_MSG_WRONG_ACTION;
        }
        free2(action);
        free_tokens(space_tokens);
        return msg;
}

private void help(void){
        printf("Welcome to YunTable\n");
        printf("Below are some sample commands:\n");
        printf("0) add master:127.0.0.1:8301\n");
        printf("1) add region:127.0.0.1:8302\n");
        printf("2) add table:people\n");
        printf("3) put table:people row:me name:\"ike\" sex:\"male\"\n");
        printf("4) get table:people row:me\n");
        printf("5) show table:people\n");
        printf("6) quit\n");
}

public void load_cli_cache(char* conf_path){
        cliCacheInst = malloc2(sizeof(CliCache));
        cliCacheInst->conf_path = m_cpy(conf_path);
        cliCacheInst->master_conn =  m_get_value_by_key(conf_path, CONF_MASTER_CONN_KEY);
        cliCacheInst->tableInfoList = load_table_info_list(conf_path);
}

private char *cli_normalize_cmd(char *cmd)
{
        cmd = trim(cmd, LINE_SEPARATOR);
        cmd = trim(cmd, ' ');
        return cmd;
}

public void start_cli_daemon(){
		//STep 1. Show the master connection info
        char *cli_str;
        if(cliCacheInst->master_conn == NULL) {
        	printf("No Master Connection has been setup\n");
        } else {
        	printf("The Master connection to %s has been setup\n", cliCacheInst->master_conn);
        }
        //step 2. print help
        help();
        //step 3. start the cli and receive inputted cmd
        char buf[CLI_BUF_SIZE];
        while(1){
			printf(DEFAULT_YUNTABLE_CLI_PREFIX);
			cli_str = fgets(buf, CLI_BUF_SIZE, stdin);
			if (!cli_str) {
				printf("get cmd error %d\r\n", ferror(stdin));
				continue; /* continue or break */
			}
			cli_str = cli_normalize_cmd(cli_str);
			if (strlen(cli_str) == 0) {
				continue;
			}
			if(match(cli_str, QUIT_KEY)) break;
			char* msg = process(cli_str);
			if(msg != NULL && strlen(msg) != 0) printf("%s\n", msg);
        }
}

/**
 * sample cmd: ./startCli -conf conf/cli.conf
 */
int main(int argc, char *argv[]){
		//Disable the logging, since the majority of err msg for cli are just printed
		setup_logging(DISABLE, "");
		char *conf_path = get_conf_path_from_argv(argc, argv, DEFAULT_CLI_CONF_PATH);
        load_cli_cache(conf_path);
        start_cli_daemon();
        return 1;
}
