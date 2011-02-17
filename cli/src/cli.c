/**
 * Copyright 2011 Wu Zhu Hua, Xi Ming Gao, Xue Ying Fei, Li Jun Long
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

#include "global.h"
#include "msg.h"
#include "item.h"
#include "utils.h"
#include "master/master.h"
#include "region.h"
#include "rpc.h"
#include "conf.h"
#include "malloc2.h"
#include "buf.h"
#include "log.h"

/***
 * The Cli is YunTable Console that used mainly for experimenting and testing the YunTable,
 * and not for performance intensive purpose, and add a silent mode for client automate testing.
 **/

#define CONF_MASTER_CONN_KEY "master"

/** cli related constants **/
#define CMD_OPTION_KEY "-cmd" /** used in the silent mode **/
#define ADD_KEY "add"
#define GET_KEY "get"
#define SHOW_KEY "show"
#define PUT_KEY "put"
#define DEL_KEY "del"
#define HELP_KEY "help"
#define ROW_KEY "row"
#define QUIT_KEY "quit"
#define TABLE_KEY "table"

#define DEFAULT_YUNTABLE_CLI_PREFIX "yuncli:"

//the unit of below two items are sec
#define DEFAULT_CLI_DAEMON_SLEEP_INTERVAL 10 * 60

typedef struct _CliCache{
		char* conf_path; /** default place is conf/cli.conf **/
		char* master_conn;
		List* tableInfoList;
}CliCache;

typedef int (*cli_cmd_func_t)(Tokens* space_tokens);
typedef struct cli_action_func_s {
    cli_cmd_func_t func;
    char *action;
    int print_success_log;
} cli_action_func_t;

CliCache* cliCacheInst = NULL;

/**
 * 从命令行获取table的名称
 *
 * @param string  table:name格式的字符串
 *
 * @return char* 返回的名称，调用者不使用后需要free该name
 */
private char* get_table_name(char *string)
{
        char *table_name = NULL;
        Tokens *fenn_tokens = init_tokens(string, ':');
        if(fenn_tokens->size == 2 && match(fenn_tokens->tokens[0], TABLE_KEY)){
        	table_name = strdup(fenn_tokens->tokens[1]);
        }
        free_tokens(fenn_tokens);
        return table_name;
}

/** If can not get the table info from master, the system will return NULL **/
private TableInfo* get_table_info_from_master(char* master_conn, char* table_name){
        TableInfo* tableInfo = NULL;
        List* params = generate_charactor_params(1, table_name);
        RPCRequest* rpcRequest = create_rpc_request(GET_TABLE_INFO_MASTER_CMD, params);
        RPCResponse* rpcResponse = connect_conn(master_conn, rpcRequest);
        int status_code = get_status_code(rpcResponse);
        if(status_code == SUCCESS){
            char* table_info_string = get_result(rpcResponse);
            if(table_info_string != NULL){
    			List* lines = generate_list_by_token(table_info_string, LINE_SEPARATOR);
    			tableInfo = string_to_table_info(table_name, lines);
    			list_destory(lines, just_free);
            }
        }else{
        	printf("Sad News! The Master node has problem: %s\n", get_error_message(status_code));
        }
        destory_rpc_request(rpcRequest);
        destory_rpc_response(rpcResponse);
        return tableInfo;
}

struct table_info_iter_arg_s {
    List *table_list;
    char *master_conn;
};

static int update_table_info_iter(void *oldTableInfo, void *argv)
{
    struct table_info_iter_arg_s *iter = argv;
    TableInfo* tableInfo;

    tableInfo = get_table_info_from_master(iter->master_conn,
        ((TableInfo* )oldTableInfo)->table_name);

    if(tableInfo != NULL) {
        list_append(iter->table_list, tableInfo);
    }
    return LIST_ITER_CONTINUE;
}

private void update_table_info_list(CliCache* cliCache)
{
    struct table_info_iter_arg_s iter;
    List* newTableInfoList = list_create();
    iter.master_conn = cliCache->master_conn;
    iter.table_list = newTableInfoList;
    list_iter(cliCache->tableInfoList, update_table_info_iter, &iter);
    list_destory(cliCache->tableInfoList, destory_table_info);
    cliCache->tableInfoList = newTableInfoList;
}

/** if the table info can't not find the tableInfoList, the method will contact master and get new, also will update the local cache **/
private TableInfo* search_and_update_table_info(CliCache* cliCache, char* table_name)
{
    TableInfo *tableInfo = get_table_info(cliCache->tableInfoList, table_name);
    if(tableInfo == NULL){
        if(cliCache->master_conn == NULL){
            printf("%s.\n", err_to_str(ERR_NO_MASTER));
        } else {
            tableInfo = get_table_info_from_master(cliCache->master_conn, table_name);
            if(tableInfo != NULL) {
                list_append(cliCache->tableInfoList, tableInfo);
            }
        }
    }
    return tableInfo;
}

private boolean create_new_table(char* master_conn, char* table_name){
		boolean result = false;
        List* params = generate_charactor_params(1, table_name);
        RPCRequest* rpcRequest = create_rpc_request(CREATE_NEW_TABLE_MASTER_CMD, params);
        RPCResponse* rpcResponse = connect_conn(master_conn, rpcRequest);
        int status_code = get_status_code(rpcResponse);
	    if(status_code == SUCCESS){
	    	result = stob(get_result(rpcResponse));
	    }else{
	    	printf("Sad News! The Master node has problem:%s\n", get_error_message(status_code));
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
		int status_code = get_status_code(rpcResponse);
	    if(status_code == SUCCESS || get_result_length(rpcResponse) > 0){
	    	result = stob(get_result(rpcResponse));
	    }else{
	    	printf("Sad News! The Master node has problem:%s\n", get_error_message(status_code));
	    }
		destory_rpc_request(rpcRequest);
		destory_rpc_response(rpcResponse);
		return result;
}

/** Will send a msg to the master node and to check the validity of reported region node **/
private boolean check_problem_region(char* problem_region_conn)
{
		printf("The Region Node %s may have some problem.", problem_region_conn);
		boolean result = false;
		printf("Now let master check the validity of %s.\n", problem_region_conn);
		List* params = generate_charactor_params(1, problem_region_conn);
	    RPCRequest* rpcRequest = create_rpc_request(CHECK_PROBLEM_REGION_MASTER_CMD, params);
		RPCResponse* rpcResponse = connect_conn(problem_region_conn, rpcRequest);
		int status_code = get_status_code(rpcResponse);
	    if(status_code == SUCCESS){
	    	return stob(get_result(rpcResponse));
		}else{
			printf("Sad News! The Master node has problem:%s!\n", get_error_message(status_code));
		}
		destory_rpc_request(rpcRequest);
		destory_rpc_response(rpcResponse);
		return result;
}

/**
 * sample cmd:
 * 		add master:172.0.0.1:8301
 * 		add region:172.0.0.1:8302
 *  	add table:people //The input table name shouldn't contain space
 */
public int add(Tokens* space_tokens)
{
		int ret = OK;
		//The add normally has two space token
		if (space_tokens->size > 2){
			ret = ERR_WRONG_ADD_CMD;
		}else{
			Tokens *fenn_tokens = init_tokens(space_tokens->tokens[1], ':');
			if (match(fenn_tokens->tokens[0], MASTER_KEY)){
				char* new_master_conn = m_cats(3, fenn_tokens->tokens[1], ":", fenn_tokens->tokens[2]);
				if(check_node_validity(new_master_conn, MASTER_KEY)){
					cliCacheInst->master_conn = new_master_conn;
					flush_key_value(cliCacheInst->conf_path, CONF_MASTER_CONN_KEY, cliCacheInst->master_conn);
					ret = OK;
				}else{
					ret = ERR_WRONG_MASTER;
				}
			} else if (match(fenn_tokens->tokens[0], REGION_KEY)){
				//step 0:check if there is master
				if(cliCacheInst->master_conn == NULL){
					ret = ERR_NO_MASTER;
				} else {
					char* new_region_conn = m_cats(3, fenn_tokens->tokens[1], ":", fenn_tokens->tokens[2]);
					if(add_new_region(cliCacheInst->master_conn, new_region_conn)){
						ret = OK;
					}else{
						ret = ERR_WRONG_REGION;
					}
					free2(new_region_conn);
				}
			} else {
				if(cliCacheInst->master_conn == NULL){
					ret = ERR_NO_MASTER;
				}else{
					char* new_table_name = fenn_tokens->tokens[1];
					if(new_table_name == NULL){
						ret = ERR_NO_TABLE_NAME;
					}else{
						TableInfo* tableInfo = search_and_update_table_info(cliCacheInst, new_table_name);
						if(tableInfo != NULL){
							 ret = ERR_EXIST;
						}else{
							if(create_new_table(cliCacheInst->master_conn, new_table_name)){
                                ret = OK;
                                //Update the table info
                                search_and_update_table_info(cliCacheInst, new_table_name);
							}else{
								ret = ERR_CLUSTER_FULL;
							}
						}
					}
				}
			}
			//all add method related with table structure, so need to update the table info afte the method
			update_table_info_list(cliCacheInst);
			free_tokens(fenn_tokens);
		}
		return ret;
}

/** the handler methid for put, which will separate the result set by column family **/
private int batch_put_data_to_table(CliCache* cliCache, TableInfo* tableInfo, ResultSet* resultSet){
		int ret = OK;
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
		    int status_code = get_status_code(rpcResponse);
			if(status_code != SUCCESS || stob(get_result(rpcResponse)) == false ){
				printf("The Connection to Region Node %s have encountered some problem:%s.\n",
						tabletInfo->regionInfo->conn, get_error_message(status_code));
				ret = ERR_PUT;
				if(status_code == CONN_FAIL && check_problem_region(tabletInfo->regionInfo->conn) == true){
					printf("The Region Node %s has some problem, and is fixing now.\n", tabletInfo->regionInfo->conn);
				}
			}
		    destory_rpc_request(rpcRequest);
			destory_rpc_response(rpcResponse);
			free_buf(buf);
		}
		return ret;
}

/** sample cmd: put table:people row:me name:"ike" sex:"male" homeaddress:"sh" **/
public int put(Tokens* space_tokens){
		char *table_name = get_table_name(space_tokens->tokens[1]);
		if (table_name == NULL) {
			 return ERR_NO_TABLE_NAME;
		}
		TableInfo *tableInfo = search_and_update_table_info(cliCacheInst, table_name);
        free2(table_name);
		if(tableInfo == NULL){
			return ERR_TABLE_NOT_CREATE;
		}
		int i=0, j=0;
		if(space_tokens->size < 4){
			return ERR_NO_COLUMN;
		}
		int item_size = space_tokens->size - 3;
		Item **items = malloc2(sizeof(Item *) * item_size);
		//get row name
		Tokens *fenn_tokens = init_tokens(space_tokens->tokens[2], ':');
        int ret = OK;
		if (fenn_tokens->size < 2 || !match(fenn_tokens->tokens[0], ROW_KEY)){
			ret = ERR_NO_ROW_KEY;
		} else {
			//get column value
			for (i=3, j=0; j<item_size; i++, j++){
				Tokens *inner_fenn_tokens = init_tokens(space_tokens->tokens[i], ':');
				char* column_name = inner_fenn_tokens->tokens[0];
				char *value = trim(inner_fenn_tokens->tokens[1], '"');
				items[j] = m_create_item(fenn_tokens->tokens[1], column_name, value);
				free_tokens(inner_fenn_tokens);
			}
			//commit
			ResultSet *resultSet = m_create_result_set(j, items);
			ret = batch_put_data_to_table(cliCacheInst, tableInfo, resultSet);
			destory_result_set(resultSet);
		}
		free_tokens(fenn_tokens);
		return ret;
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
				RPCRequest* rpcRequest = NULL;
				if(row_key != NULL){
					List* params = generate_charactor_params(2, tableInfo->table_name, row_key);
					rpcRequest = create_rpc_request(QUERY_ROW_REGION_CMD, params);
				}else{
					List* params = generate_charactor_params(1, tableInfo->table_name);
					rpcRequest = create_rpc_request(QUERY_ALL_REGION_CMD, params);
				}
				RPCResponse* rpcResponse = connect_conn(tabletInfo->regionInfo->conn, rpcRequest);
				int status_code = get_status_code(rpcResponse);
				if(status_code == SUCCESS || get_result_length(rpcResponse) != 0){
					ResultSet* set1 = (ResultSet*) byte_to_result_set(get_result(rpcResponse));
					//The m_combine_result_set method will remove all duplicated sets
					ResultSet* set2 = m_combine_result_set(resultSet, set1);
					//Cleansing some deleted stuff
					cleansing(set2);
					free_result_set(set1);
					free_result_set(resultSet);
					resultSet = set2;
				}else{
					printf("The Connection to Region Node %s have encountered some problems:%s.\n",
							tabletInfo->regionInfo->conn, get_error_message(status_code));
					if(status_code == CONN_FAIL && check_problem_region(tabletInfo->regionInfo->conn) == true){
						printf("The Region Node %s has some problem, and is fixing now.\n", tabletInfo->regionInfo->conn);
					}
				}
				destory_rpc_request(rpcRequest);
				destory_rpc_response(rpcResponse);
			}
		}
		return resultSet;
}

/** sample cmd:
 * get table:people row:me **/
public int get(Tokens* space_tokens){
		int ret = OK;
    	char *table_name = get_table_name(space_tokens->tokens[1]);
        if(table_name == NULL) {
			return ERR_NO_TABLE_NAME;
        }
        TableInfo *tableInfo = search_and_update_table_info(cliCacheInst, table_name);
        free2(table_name);
        if (tableInfo == NULL) {
			return ERR_TABLE_NOT_CREATE;
        }
        ResultSet* resultSet = NULL;
        //Goes to get all method
        if(space_tokens->size == 2){
        	resultSet = query_table(cliCacheInst, tableInfo, NULL);
        }
        //Goes to get by row key method
        else{
			Tokens *fenn_tokens = init_tokens(space_tokens->tokens[2],':');
			if(!match(fenn_tokens->tokens[0], ROW_KEY)){
				ret = ERR_NO_ROW_KEY;
			}else{
				char *row_key = strdup(fenn_tokens->tokens[1]);
				resultSet = query_table(cliCacheInst, tableInfo, row_key);
				free2(row_key);
			}
			free_tokens(fenn_tokens);
        }
        if(resultSet->size == 0){
			ret = ERR_NOTHING_FOUND;
        }else {
			print_result_set_in_nice_format(resultSet);
        }
        destory_result_set(resultSet);
        return ret;
}

private void show_table_metadata(TableInfo* tableInfo){
    int i;
    int rq_size=list_size(tableInfo->replicaQueueList);
    printf("The Meta Info about the Table:%s\n", tableInfo->table_name);
    printf("The Number of replica queue:%d\n", rq_size);

    //Iterating the Replica Queues
    for (i=0; i<rq_size; i++) {
        printf("The Meta Info from Replica Queue %d.\n", i);
        printf("\n");
        ReplicaQueue* replicaQueue = list_get(tableInfo->replicaQueueList, i);
        int k=0, tt_size=list_size(replicaQueue->tabletInfoList);

        //Iterating the Tablet Infos
        for (k=0; k<tt_size; k++) {
            TabletInfo* tabletInfo = list_get(replicaQueue->tabletInfoList, k);
            printf("The Meta Info from Tablet %d at Region %s.\n", k, tabletInfo->regionInfo->conn);
            List* params = generate_charactor_params(1, tableInfo->table_name);
            RPCRequest* rpcRequest = create_rpc_request(GET_METADATA_REGION_CMD, params);
            RPCResponse* rpcResponse = connect_conn(tabletInfo->regionInfo->conn, rpcRequest);
            int status_code = get_status_code(rpcResponse);
            if (status_code == SUCCESS) {
                char* result = get_result(rpcResponse);
                printf("%s", result);
            } else {
                printf("Sad News! The Region node has some problem:%s.\n", get_error_message(status_code));
            }
            destory_rpc_request(rpcRequest);
            destory_rpc_response(rpcResponse);
        }
    }
}

private void show_master_metadata(CliCache* cliCache){
    if(cliCache->master_conn == NULL){
        printf("%s.\n", err_to_str(ERR_NO_MASTER));
    } else {
        RPCRequest* rpcRequest = create_rpc_request(GET_METADATA_MASTER_CMD, NULL);
        RPCResponse* rpcResponse = connect_conn(cliCache->master_conn, rpcRequest);
        int status_code = get_status_code(rpcResponse);
        if(status_code == SUCCESS || get_result_length(rpcResponse) > 0){
            char* result = get_result(rpcResponse);
            printf("%s", result);
        }else{
            printf("Sad News! The Master node has problem:%s.\n", get_error_message(status_code));
        }
        destory_rpc_request(rpcRequest);
        destory_rpc_response(rpcResponse);
    }
}

/** sample cmd:
 *   1) show table:people // which retrieve the table's metadata
 *   2) show master  //which retrieve the master's metadata
 * **/
public int show(Tokens* space_tokens){
    //Go to Show Master procedure or Show Table
    int ret = OK;
    if(match(space_tokens->tokens[1], MASTER_KEY)){
        show_master_metadata(cliCacheInst);
    } else {
        char *table_name = get_table_name(space_tokens->tokens[1]);
        if (table_name == NULL) {
            return ERR_CMD_NOT_COMPLETE;
        }
        TableInfo *tableInfo = search_and_update_table_info(cliCacheInst, table_name);
        free2(table_name);
        if (tableInfo == NULL) {
            return ERR_TABLE_NOT_CREATE;
        }
        show_table_metadata(tableInfo);
    }
    return ret;
}


/** Sample cmd:
 * 		del table:people row:me //delete the row key is "me" the data
 *		del table:people row:me name sex //delete the inputted columns if row key is "me"
 * **/
public int del(Tokens* space_tokens){
		int ret;
		char *table_name = get_table_name(space_tokens->tokens[1]);
		if(table_name == NULL) {
			return ERR_NO_TABLE_NAME;
		}
		TableInfo *tableInfo = search_and_update_table_info(cliCacheInst, table_name);
        free2(table_name);
		if (tableInfo == NULL) {
			return ERR_TABLE_NOT_CREATE;
		}
		Tokens *fenn_tokens = init_tokens(space_tokens->tokens[2],':');
		if(!match(fenn_tokens->tokens[0], ROW_KEY)){
			ret = ERR_NO_ROW_KEY;
		}else{
			//get row name
			char *row_key = strdup(fenn_tokens->tokens[1]);
			ResultSet* resultSet = NULL;
			//If space tokens size bigger than 3, means has inputed some columns
			if(space_tokens->size == 3){
				//Generate a row deletion mark
				Item* item = m_create_item(row_key, NULL, NULL);
				Item** items = malloc2(sizeof(Item*) * 1);
				items[0] = item;
				resultSet = m_create_result_set(1, items);
			}else{
				int i=0;
				int column_size = space_tokens->size - 3;
				Item** items = malloc2(sizeof(Item*) * column_size);
				for(i=0; i<column_size; i++){
					char* column_name = space_tokens->tokens[i+3];
					items[i] = m_create_item(row_key, column_name, NULL);
				}
				resultSet = m_create_result_set(column_size, items);
			}
			ret = batch_put_data_to_table(cliCacheInst, tableInfo, resultSet);
			destory_result_set(resultSet);
		}
		free_tokens(fenn_tokens);
		return ret;
}

public int help(Tokens* space_tokens){
        printf("Below are some sample commands:\n");
        printf("0)  Add A New Master to the cli.\n");
        printf("    Sample cmd: add master:127.0.0.1:8301\n");
        printf("1)  Add A New Region to the master.\n");
        printf("    Sample cmd: add region:127.0.0.1:8302\n");
        printf("2)  Add a new table to the master.\n");
        printf("    Sample cmd: add table:people\n");
        printf("3)  Put data to the target table.\n");
        printf("    Sample cmd: put table:people row:me name:\"ike\" sex:\"male\"\n");
        printf("4)  Get all data from target table.\n");
        printf("    Sample cmd: get table:people\n");
        printf("5)  Get data from target table by row key.\n");
        printf("    Sample cmd: get table:people row:me\n");
        printf("6)  Delete data from tagret table by row key.\n");
        printf("    Sample cmd: del table:people row:me\n");
        printf("7)  Delete Column from tagret table by row key.\n");
        printf("    Sample cmd: del table:people row:me sex name\n");
        printf("8)  Show table metadata information.\n");
        printf("    Sample cmd: show table:people\n");
        printf("9)  Show master metadata information.\n");
        printf("    Sample cmd: show master\n");
        printf("10) Quit the console.\n");
        printf("    Sample cmd: quit\n");
        return 0;
}


cli_action_func_t cli_action_func[] = {
    {add, ADD_KEY, true},
    {put, PUT_KEY, true},
    {get, GET_KEY, false},
    {del, DEL_KEY, true},
    {show, SHOW_KEY, false},
    {help, HELP_KEY, false},
    {NULL, NULL, false},
};

static cli_action_func_t* cli_get_cmd(const char *action)
{
    int i;

    for (i = 0; cli_action_func[i].func != NULL; i++) {
        if (match(cli_action_func[i].action, action)) {
            return &cli_action_func[i];
        }
    }
    return NULL;
}


public int process(char *cmd)
{
    int ret = OK;

    if(cmd == NULL && strlen(cmd) < 3){
        return ERR_NULL_STRING;
    }
    Tokens* space_tokens = init_tokens(cmd, ' ');
    if(space_tokens->size < 1) {
        return ERR_NULL_STRING;
    }
    cli_action_func_t *action = cli_get_cmd(space_tokens->tokens[0]);
    if (action == NULL) {
        ret = ERR_WRONG_ACTION;
    } else {
        ret = action->func(space_tokens);
        if (ret == 0 && action->print_success_log) {
            printf("The Action has been completed successfully.\r\n");
        }
    }
    free_tokens(space_tokens);
    return ret;
}

public void load_cli_cache(char* conf_path){
        cliCacheInst = malloc2(sizeof(CliCache));
        cliCacheInst->conf_path = strdup(conf_path);
        cliCacheInst->master_conn =  m_get_value_by_key(conf_path, CONF_MASTER_CONN_KEY);
        cliCacheInst->tableInfoList = load_table_info_list(conf_path);
}

private char *cli_normalize_cmd(char *cmd)
{
    cmd = trim(cmd, LINE_SEPARATOR);
    cmd = trim(cmd, ' ');
    return cmd;
}

public int start_cli_daemon()
{
    int ret = 0;
    char *cli_str;
	printf("Welcome to YunTable\n");
    //Step 1. Show the master connection info
    if(cliCacheInst->master_conn == NULL) {
        printf("No Master Connection has been setup\n");
    } else {
        printf("The Master connection to %s has been setup\n", cliCacheInst->master_conn);
    }

    //step 2. print help
    printf("Please type \"help\" if you need some further information.\n");

    //step 3. start the cli and receive inputted cmd
    char buf[LINE_BUF_SIZE];
    while (1) {
        printf(DEFAULT_YUNTABLE_CLI_PREFIX);
        cli_str = fgets(buf, LINE_BUF_SIZE, stdin);
        if (!cli_str) {
            ret = ferror(stdin);
            fprintf(stderr, "get cmd error %d\n", ret);
            break; /* continue or break */
        }
        cli_str = cli_normalize_cmd(cli_str);
        if (strlen(cli_str) == 0) {
            continue;
        }
        if(match(cli_str, QUIT_KEY)) {
            break;
        }
        ret = process(cli_str);
        if(ret != 0) {
            printf("%s\n", err_to_str(ret));
        }
    }
    return ret;
}

public int silent_mode(int argc, char** argv)
{
    int ret = 0;
    if(argc == 2){
        printf("%s\n", err_to_str(ERR_NO_CMD_INPUT));
        ret = ERR_NO_CMD_INPUT;
    } else {
        char* cmd = array_to_string(argv, 2, argc-1);
        printf("%s\n",cmd);
        ret = process(cmd);
        if(ret != 0){
            printf("%s\n", err_to_str(ret));
        }
    }
    return ret;
}

/**
 * sample cmd:
 *     1. Starting cli ./yuncli
 *     2. Starting cli with conf file ./yuncli -conf conf/cli.conf
 *     3. Silent mode for testing ./yuncli -cmd add master:127.0.0.1:8301
 *
 * Return: on success, return 0; others, return error code;
 */
int main(int argc, char **argv)
{
    int ret = 0;
    //Disable the logging, since the majority of err msg for cli are just printed
    setup_logging(DISABLE, "");
    //Init the cli cache
    char *conf_path = get_conf_path_from_argv(argc, argv, DEFAULT_CLI_CONF_PATH);
    load_cli_cache(conf_path);
    if (argc >= 2 && match(argv[1], CMD_OPTION_KEY)) {
        ret = silent_mode(argc, argv);
    } else {
        ret = start_cli_daemon();
    }
    return ret;
}
