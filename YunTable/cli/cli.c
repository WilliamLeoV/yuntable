#include "global.h"
#include "msg.h"
#include "item.h"
#include "utils.h"
#include "master.h"
#include "region.h"
#include "rpc.h"
#include "conf.h"

#define DEFAULT_CLI_CONF_PATH "conf/cli.conf"
#define CONF_MASTER_CONN_KEY "master"

/** cli related constants **/
#define ADD_KEY "add"
#define GET_KEY "get"
#define PUT_KEY "put"
#define ROW_KEY "row"
#define ALL_KEY "all"
#define QUIT_KEY "quit"
#define COLUMN_FAMILY_KEY "column_family"
#define TABLE_KEY "table"
#define UNIX_EOL_KEY "\n"
#define DOS_EOL_KEY "\r\n"


#define DEFAULT_YUNTABLE_CLI_PREFIX "yuncli:"
//the unit is MB
#define MIN_DISK_AVAIL_SPACE 10
//the unit of below two items are sec
#define DEFAULT_CLI_DAEMON_SLEEP_INTERVAL 10 * 60
#define MAX_TIME_OUT 5

typedef struct _CliCache{
                char* conf_path; /** default place is conf/cli.conf **/
                char* master_conn;
                List* tableInfoList;
                List* threadIdList; /** only for hosting the currently running pthread id**/
}CliCache;

CliCache* cliCacheInst = NULL;

private void refresh_cli_cache_thread_id_list(CliCache* cliCache){
        //TODO handle the int free
        list_destory(cliCache->threadIdList, only_free_struct);
        cliCache->threadIdList = list_create();
}

private char* get_table_name(char *string){
        char *table_name = NULL;
        Tokens *fenn_tokens = init_tokens(string, ':');
        if(match(fenn_tokens->tokens[0], TABLE_KEY)) table_name = m_cpy(fenn_tokens->tokens[1]);
        free_tokens(fenn_tokens);
        return table_name;
}

//TODO need to add 5sec signal
void cancel_all_runing_threads(int sig){
        logg("Some threads have reached the timeout\n",NULL);
        //TODO check which one has failed
        int thread_size = list_size(cliCacheInst->threadIdList), i=0;
        for(i=0; i<thread_size; i++)
                pthread_cancel((int)list_get(cliCacheInst->threadIdList, i));
}

private TableInfo* get_table_info_from_master(char* master_conn, char* table_name){
        TableInfo* tableInfo = NULL;
        List* params = generate_charactor_params(1, table_name);
        char* table_info_string = connect_conn(master_conn, GET_TABLE_INFO_MASTER_CMD, params);
        if(!match(table_info_string, NULL_STRING)){
                List* lines = generate_list_by_token(table_info_string, LINE_SEPARATOR);
                tableInfo = string_to_table_info(table_name, lines);
                list_destory(lines, just_free);
        }
        list_destory(params, just_free);
        free(table_info_string);
        return tableInfo;
}

private void update_table_info_list(CliCache* cliCache){
        List* oldTableInfoList = cliCache->tableInfoList;
        int i, size=list_size(oldTableInfoList);
        List* newTableInfoList = list_create();
        for(i=0; i<size; i++){
                TableInfo* oldTableInfo = list_get(oldTableInfoList, i);
                TableInfo* newTableInfo = get_table_info_from_master(cliCache->master_conn, oldTableInfo->table_name);
                list_append(newTableInfoList, newTableInfo);
        }
        cliCache->tableInfoList = newTableInfoList;
        list_destory(oldTableInfoList, destory_table_info);
}

/** if the table info can't not find the tableInfoList, the method will contact master and get new**/
private TableInfo* search_table_info(CliCache* cliCache, char* table_name){
        TableInfo *tableInfo = get_table_info(cliCache->tableInfoList, table_name);
        if(tableInfo == NULL){
                tableInfo = get_table_info_from_master(cliCache->master_conn, table_name);
                if(tableInfo != NULL) list_append(cliCache->tableInfoList, tableInfo);
        }
        return tableInfo;
}

private boolean add_new_column_family(char* master_conn, char* table_name, char* column_family){
        List* params = generate_charactor_params(2, table_name, column_family);
        boolean result = connect_conn_boolean(master_conn, ADD_NEW_COLUMN_FAMILY_MASTER_CMD, params);
        list_destory(params, just_free);
        return result;
}

private boolean create_new_table(char* master_conn, char* table_name){
        List* params = generate_charactor_params(1, table_name);
        boolean result = connect_conn_boolean(master_conn, CREATE_NEW_TABLE_MASTER_CMD, params);
        list_destory(params, just_free);
        return result;
}

private boolean add_new_region(char* master_conn, char* region_conn){
        List* params = generate_charactor_params(1, region_conn);
        boolean result = connect_conn_boolean(master_conn, ADD_NEW_REGION_MASTER_CMD, params);
        list_destory(params, just_free);
        return result;
}

/**
 * sample cmd: add master:172.0.0.1:8301
 * sample cmd: add region:172.0.0.1:8302
 * sample cmd: add table:people
 * sample cmd: add table:people column_family:address
 */
public char* add(Tokens* space_tokens){
        char* msg = NULL;
        Tokens *fenn_tokens = init_tokens(space_tokens->tokens[1], ':');
        if(match(fenn_tokens->tokens[0], MASTER_KEY)){
                if (fenn_tokens->size < 3) {
                        return ERR_MSG_NULL_STRING;
                }
                char* new_master_conn = m_cats(3, fenn_tokens->tokens[1], ":", fenn_tokens->tokens[2]);
                if(check_node_validity(new_master_conn, MASTER_KEY)){
                        cliCacheInst->master_conn = new_master_conn;
                        flush_key_value(cliCacheInst->conf_path, CONF_MASTER_CONN_KEY, cliCacheInst->master_conn);
                        msg = SUCC_MSG_COMPLETED;
                }else{
                        msg = ERR_MSG_WRONG_MASTER;
                        free(new_master_conn);
                }
        }else if(match(fenn_tokens->tokens[0], REGION_KEY)){
                if (fenn_tokens->size < 3) {
                        return ERR_MSG_NULL_STRING;
                }
                //step 0:check if there is master
                if(cliCacheInst->master_conn == NULL) return ERR_MSG_NO_MASTER;
                char* new_region_conn = m_cats(3, fenn_tokens->tokens[1], ":", fenn_tokens->tokens[2]);
                if(add_new_region(cliCacheInst->master_conn, new_region_conn)) msg = SUCC_MSG_COMPLETED;
                else msg = ERR_MSG_WRONG_REGION;
                free(new_region_conn);
        }else {
                if(cliCacheInst->master_conn == NULL) return ERR_MSG_NO_MASTER;
                char* new_table_name = fenn_tokens->tokens[1];
                if(new_table_name == NULL) return ERR_MSG_NO_TABLE_NAME;
                TableInfo* tableInfo = search_table_info(cliCacheInst, new_table_name);
                if(space_tokens->size == 2) {
                        if(tableInfo != NULL) return ISSUE_MSG_TABLE_ALREADY_EXISTED;
                         if(create_new_table(cliCacheInst->master_conn, new_table_name)) msg = SUCC_MSG_TABLE_CREATED;
                         else msg = ERR_MSG_CLUSTER_FULL;
                }else{
                        if(tableInfo == NULL) return ERR_MSG_TABLE_NOT_CREATED;
                        Tokens *inner_fenn_tokens = init_tokens(space_tokens->tokens[2], ':');
                        if(inner_fenn_tokens->size >= 2 && match(inner_fenn_tokens->tokens[0], COLUMN_FAMILY_KEY)){
                                char* new_column_family = inner_fenn_tokens->tokens[1];
                                if(add_new_column_family(cliCacheInst->master_conn, new_table_name, new_column_family))
                                        msg = SUCC_MSG_NEW_COLUMN_FAMILY_CREATED;
                                else msg = ERR_MSG_NEW_COLUMN_FAMILY_NOT_CREATED;
                        }else{
                                msg = ERR_MSG_NO_COLUMN_FAMILY;
                        }
                        free_tokens(inner_fenn_tokens);
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
        List* unique_column_familys = get_unique_column_familys(resultSet);
        List* connParams = list_create();
        int i=0, cl_size=list_size(unique_column_familys);
        for(i=0;i<cl_size;i++){
                char* column_family = list_get(unique_column_familys, i);
                ResultSet* set = filter_result_sets_by_colume_family(resultSet, column_family);
                List* params = generate_charactor_params(2, tableInfo->table_name, column_family);
                Buf* buf = result_set_to_byte(set);
                add_param(params, get_buf_index(buf), get_buf_data(buf));
                ColumnFamilyInfo* columnFamilyInfo = get_column_family_info(tableInfo->columnFamilyInfoList, column_family);
                int j=0, rq_size=list_size(columnFamilyInfo->replicaQueueList);
                for(j=0; j<rq_size; j++){
                        ReplicaQueue* replicaQueue = list_get(columnFamilyInfo->replicaQueueList, j);
                        //Last the region is the working region, TODO may be need to some check whether working
                        TabletInfo* tabletInfo = list_get(replicaQueue->tabletInfoList, list_size(replicaQueue->tabletInfoList) -1);
                        ConnParam* connParam = create_conn_param(tabletInfo->regionInfo->conn, PUT_DATA_REGION_CMD, params);
                        pthread_t tid;
                        pthread_create(&tid, NULL, connect_thread, (void*)connParam);
                        list_append(cliCache->threadIdList, (void*)tid);
                        list_append(connParams, connParam);
                }
                free_buf(buf);
                free_result_set(set);
        }
        int k=0, thread_size = list_size(cliCache->threadIdList);
        for(k=0; k<thread_size; k++){
                int tid = (int)list_get(cliCache->threadIdList, k);
                void* ret;
                pthread_join(tid, &ret);
                if((int)ret == -1)continue;
                boolean result = stob((char*)ret);
                if(result == false) {
                        msg = ERR_MSG_PUT;
                        break;
                }
                free(ret);
        }
        list_destory(unique_column_familys, just_free);
        list_destory(connParams, destory_conn_param);
        refresh_cli_cache_thread_id_list(cliCache);
        return msg;
}

/** sample cmd:put table:people row:me name:"ike" sex:"male" address.homeaddress:"sh" **/
public char* put(Tokens* space_tokens){
        char *table_name = get_table_name(space_tokens->tokens[1]);
        if(table_name == NULL) {
                return ERR_MSG_NO_TABLE_NAME;
        }
        TableInfo *tableInfo = search_table_info(cliCacheInst, table_name);
        if(tableInfo == NULL) {
                return ERR_MSG_TABLE_NOT_CREATED;
        }
        int i=0, j=0;
        if(space_tokens->size < 4) {
                return ERR_MSG_NO_COLUMN;
        }
        int item_size = space_tokens->size - 3;
        Item **items = malloc(sizeof(Item *) * item_size);
        //get row name
        Tokens *fenn_tokens = init_tokens(space_tokens->tokens[2], ':');
        if(fenn_tokens->size < 2 || !match(fenn_tokens->tokens[0], ROW_KEY))
                        return ERR_MSG_NO_ROW_KEY;
        char *rowname = m_cpy(fenn_tokens->tokens[1]);
        free_tokens(fenn_tokens);
        //get column value
        for(i=3, j=0; j<item_size; i++, j++){
                Tokens *inner_fenn_tokens = init_tokens(space_tokens->tokens[i], ':');
                Tokens *comma_tokens = init_tokens(inner_fenn_tokens->tokens[0], MID_SEPARATOR);
                //init the default column family and column qualifier
                char* column_family = DEFAULT_COLUMN_FAMILY;
                char* column_name = inner_fenn_tokens->tokens[0];
                //if has column family
                if(comma_tokens->size>1) {
                        column_family = comma_tokens->tokens[0];
                        column_name = comma_tokens->tokens[1];
                }
                char *value = trim(inner_fenn_tokens->tokens[1], '"');
                items[j] = m_create_item(rowname, column_family, column_name, value);
                free_tokens(comma_tokens);
                free_tokens(inner_fenn_tokens);
        }
        //commit
        ResultSet *resultSet = m_create_result_set(j, items);
        char *msg = batch_put_data_to_table(cliCacheInst, tableInfo, resultSet);
        destory_result_set(resultSet);
        free(rowname);
        return msg;
}

/* if row_key == NULL, will return all the data belong to the table */
private ResultSet* query_table(CliCache *cliCache, TableInfo* tableInfo, char *row_key){
        ResultSet *resultSet = m_create_result_set(0, NULL);
        //the list for temporary stored the connParams, which will be further freed
        List* connParams = list_create();
        int i=0, cl_size=list_size(tableInfo->columnFamilyInfoList);
        for(i=0; i<cl_size; i++){
                ColumnFamilyInfo* columnFamilyInfo = list_get(tableInfo->columnFamilyInfoList, i);
                int j=0, rq_size=list_size(columnFamilyInfo->replicaQueueList);
                for(j=0; j<rq_size; j++){
                        ReplicaQueue* replicaQueue = list_get(columnFamilyInfo->replicaQueueList, j);
                        int k=0, tt_size=list_size(replicaQueue->tabletInfoList);
                        for(k=0; k<tt_size; k++){
                                TabletInfo* tabletInfo = list_get(replicaQueue->tabletInfoList, k);
                                List* params = NULL;
                                ConnParam* connParam = NULL;
                                if(row_key != NULL){
                                        params = generate_charactor_params(3, tableInfo->table_name,
                                                columnFamilyInfo->column_family, row_key);
                                        connParam = create_conn_param(tabletInfo->regionInfo->conn,
                                                        QUERY_ROW_REGION_CMD, params);
                                }else{
                                        params = generate_charactor_params(2, tableInfo->table_name,
                                                        columnFamilyInfo->column_family);
                                        connParam = create_conn_param(tabletInfo->regionInfo->conn,
                                                        QUERY_ALL_REGION_CMD, params);
                                }
                                pthread_t tid;
                                pthread_create(&tid, NULL, connect_thread, (void*)connParam);
                                list_append(cliCache->threadIdList, (void*)tid);
                                list_append(connParams, connParam);
                        }
                }
        }
        int k=0, thread_size = list_size(cliCache->threadIdList);
        for(k=0; k<thread_size; k++){
                int tid = (int)list_get(cliCache->threadIdList, k);
                void* ret;
                pthread_join(tid, &ret);
                if((int)ret == -1)continue;
                ResultSet* set1 = (ResultSet*) byte_to_result_set(ret);;
                ResultSet* set2 = m_combine_result_set(resultSet, set1);

                free(ret);
                free_result_set(set1);
                free_result_set(resultSet);
                resultSet = set2;
        }

        list_destory(connParams, destory_conn_param);
        refresh_cli_cache_thread_id_list(cliCache);
        return resultSet;
}

private void get_all(TableInfo* tableInfo){
        printf("The Meta Info about the table:%s\n", tableInfo->table_name);
        int i=0, size=list_size(tableInfo->columnFamilyInfoList);
        printf("The Column Familys that belong to this table:");
        for(i=0;i<size;i++){
                ColumnFamilyInfo* columnFamilyInfo = list_get(tableInfo->columnFamilyInfoList, i);
                printf("%s ", columnFamilyInfo->column_family);
        }
        printf("\n");
        //printf("The Number of Region Nodes that currently running:%d\n", list_size(tableInfo->regionInfoList));
}


/** sample cmd:
 *  1) get table:people row:me
 *  2) get table:people //all data and schema
 *  */
public char* get(Tokens* space_tokens){
        char *table_name = get_table_name(space_tokens->tokens[1]);
        if(table_name == NULL) {
                return ERR_MSG_NO_TABLE_NAME;
        }
        char *msg = EMPTY_STRING;
        TableInfo *tableInfo = search_table_info(cliCacheInst, table_name);
        //if space tokens's size == 2, means will go to cmd 2
        if (tableInfo == NULL) {
                return ERR_MSG_NO_TABLE_NAME;
        }

        ResultSet* resultSet = NULL;
        if(space_tokens->size == 2){
                get_all(tableInfo);
                resultSet = query_table(cliCacheInst, tableInfo, NULL);
        }else{
                Tokens *fenn_tokens = init_tokens(space_tokens->tokens[2],':');
                if(!match(fenn_tokens->tokens[0], ROW_KEY))
                                return ERR_MSG_NO_ROW_KEY;
                char *row_key = m_cpy(fenn_tokens->tokens[1]);
                resultSet = query_table(cliCacheInst, tableInfo, row_key);
                free_tokens(fenn_tokens);
        }
        remove_legacy_items(resultSet);
        if(resultSet->size == 0){
                msg = ISSUE_MSG_NOTHING_FOUND;
        }else {
                print_result_set_in_nice_format(resultSet);
        }
        destory_result_set(resultSet);


        return msg;
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
        }else{
                msg = ERR_MSG_WRONG_ACTION;
        }
        free(action);
        free_tokens(space_tokens);
        return msg;
}

private void help(void){
        printf("Welcome to YunTable\n");
        printf("Below are some sample commands:\n");
        printf("0) add master:127.0.0.1:8301\n");
        printf("1) add region:127.0.0.1:8302\n");
        printf("2) add table:people\n");
        printf("3) add table:people column_family:address\n");
        printf("4) put table:people row:me name:\"ike\" sex:\"male\" address.homeaddress:\"sh\"\n");
        printf("5) get table:people row:me\n");
        printf("6) get table:people\n");
        printf("7) quit\n");
}

public void load_cli_cache(char* conf_path){
        cliCacheInst = malloc(sizeof(CliCache));
        if (cliCacheInst == NULL) {
                exit(1);
        }
        /* master 和 table 可能为NULL */
        cliCacheInst->conf_path = strdup(conf_path);
        cliCacheInst->master_conn =  m_get_value_by_key(conf_path, CONF_MASTER_CONN_KEY);
        cliCacheInst->tableInfoList = load_table_info_list(conf_path);
        cliCacheInst->threadIdList = list_create();
        if (cliCacheInst->conf_path == NULL
            || cliCacheInst->threadIdList == NULL) {
                exit(1);
        }
}

void* cli_daemon_thread(void* nul){
        while(1){
                sleep(DEFAULT_CLI_DAEMON_SLEEP_INTERVAL);
                logg("The interval is up, checking region status now.\n",NULL);
                update_table_info_list(cliCacheInst);
                logg("Finish the checking.\n",NULL);
        }
}

private char *cli_normalize_cmd(char *cmd)
{
        cmd = trim(cmd, '\n');
        cmd = trim(cmd, '\r');
        cmd = trim(cmd, ' ');
        return cmd;
}

public void start_cli_daemon(){
        int ret;
        char *cli_str;
        int BUF_SIZE = 10000;

        if(cliCacheInst->master_conn == NULL) {
                logg("No Master Connection has been setup\n",NULL);
        } else {
                logg("The Master connection to %s has been setup\n", cliCacheInst->master_conn);
        }

        pthread_t fresh_thread_id;
        ret = pthread_create(&fresh_thread_id, NULL, cli_daemon_thread, (void*)NULL);
        if (ret != 0) {
                exit(ret);
        }
        logg("flush cli daemon thread has started\n", NULL);
        //step 0. print help
        help();
        //step 2. start the daemon, which can get
        char buf[BUF_SIZE];
        while(1){
                printf(DEFAULT_YUNTABLE_CLI_PREFIX);
                cli_str = fgets(buf, BUF_SIZE, stdin);
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

private void usage(char *program_name)
{
        printf("usage:\r\n");
        printf("        %s [-conf path]\r\n", program_name);
        exit(0);
}

/**
 * sample cmd: ./startCli -conf conf/cli.conf
 */
int main(int argc, char *argv[]){
        char *conf_path = get_conf_path_from_argv(argc, argv, DEFAULT_CLI_CONF_PATH);
        if (conf_path == NULL) {
                usage(argv[0]);
        }
        load_cli_cache(conf_path);
        start_cli_daemon();
        return 1;
}
