#include "global.h"
#include "utils.h"
#include "conf.h"

/** the output: key = value **/
public char* key_and_value_to_line(char* key, char* value){
	Buf* buf = init_buf();
	buf_cat(buf, key, strlen(key));
	buf_cat(buf, CONF_SEPARATOR_STRING, strlen(CONF_SEPARATOR_STRING));
	buf_cat(buf, value, strlen(value));
	char* result = get_buf_data(buf);
	free_buf(buf);
	return result;
}

public char* m_get_key(char* line){
	Tokens *tokens = init_tokens(line, CONF_SEPARATOR);
	char* key = m_cpy(trim(tokens->tokens[0], ' '));
	free_tokens(tokens);
	return key;
}

public char* m_get_value(char* line){
	char* value = NULL;
	Tokens *tokens = init_tokens(line, CONF_SEPARATOR);
	if(tokens->size > 1){
		value = m_cpy(trim(tokens->tokens[1], ' '));
	}
	free_tokens(tokens);
	return value;
}

public char* m_get_value_by_key(char *file_path, char* target_key){
	char* value = NULL;
	List* lines = get_lines_from_file(file_path);
	char* line = NULL;
	while((line = list_next(lines)) != NULL){
		if(count(line, CONF_SEPARATOR) > 0){
			char* key = m_get_key(line);
			if(match(key, target_key))
				value = m_get_value(line);
			free(key);
		}
	}
	list_destory(lines, just_free);
	if(value != NULL)trim(value, ' ');
	return value;
}

/* if the value can not be get, the method will return 0*/
public int get_int_value_by_key(char *file_path, char* target_key){
	int int_value = 0;
	char *value = m_get_value_by_key(file_path,target_key);
	if(value != NULL)int_value = atoi(value);
	free(value);
	return int_value;
}

/** if the key exists, the old value will be replaced by the new value **/
public void flush_key_value(char* file_path, char* key, char* value){
	Buf* buf = init_buf();
	List* lines = get_lines_from_file(file_path);
	char* line = NULL;
	while((line = list_next(lines)) != NULL){
		//ignore table info list line, which will append at the ned
		char* temp_key = m_get_key(line);
		if(match(key, temp_key)) continue;
		buf_cat(buf, line, strlen(line));
		buf_cat(buf, LINE_SEPARATOR_STRING, strlen(LINE_SEPARATOR_STRING));

		free(temp_key);
	}

	char* new_line = m_cats(3, key, CONF_SEPARATOR_STRING, value);
	buf_cat(buf, new_line, strlen(new_line));

	//printf("%s\n", bytes);
	create_or_rewrite_file(file_path, get_buf_data(buf));

	free(new_line);
	destory_buf(buf);
}

private char* key_and_value_list_to_line(char *key, List* valueList){
	char* value = list_to_string_by_token(valueList, STRING_SEPARATOR);
	return key_and_value_to_line(key, value);
}

public SyncJob* create_sync_job(char* target_conn, char* table_name, char* column_family, int begin_timestamp, int end_timestamp){
	SyncJob* syncJob = malloc(sizeof(SyncJob));
	syncJob->target_conn = m_cpy(target_conn);
	syncJob->table_name = m_cpy(table_name);
	syncJob->column_family = m_cpy(column_family);
	syncJob->begin_timestamp = begin_timestamp;
	syncJob->end_timestamp = end_timestamp;
	return syncJob;
}

private boolean match_region(void *destRegionInfo, void *srcRegionConn){
	RegionInfo* regionInfo = (RegionInfo*)destRegionInfo;
	if(match(regionInfo->conn, (char*)srcRegionConn)) return true;
	else return false;
}

public RegionInfo* get_region_info(List* regionInfoList, char* region_conn){
	return list_find(regionInfoList, region_conn, match_region);
}


public RegionInfo* create_region_info(char* region_conn){
	RegionInfo* regionInfo = malloc(sizeof(RegionInfo));
	regionInfo->conn = m_cpy(region_conn);
	regionInfo->connecting = true;
	regionInfo->serving = true;
	regionInfo->avail_space = 0;
	return regionInfo;
}

/* sample string 127.0.0.1:8302 */
private char* m_region_info_to_string(RegionInfo *regionInfo){
	Buf* buf = init_buf();
	buf_cat(buf, regionInfo->conn, strlen(regionInfo->conn));
	char* result = get_buf_data(buf);
	free_buf(buf);
	return result;
}

private RegionInfo* string_to_region_info(char* string){
	RegionInfo *regionInfo = create_region_info(string);
	return regionInfo;
}

/* sample input: 127.0.0.1:8301,127.0.0.3:8302 */
private List* string_to_region_info_list(char* string){
	List* regionInfoList = list_create();
	if(string != NULL){
		Tokens* tokens = init_tokens(string, STRING_SEPARATOR);
		int i=0;
		for(i=0; i<tokens->size; i++){
			RegionInfo* regionInfo = string_to_region_info(tokens->tokens[i]);
			list_append(regionInfoList, regionInfo);
		}
		free_tokens(tokens);
	}
	return regionInfoList;
}

/* sample output: 127.0.0.1:8301,127.0.0.3:8302 */
private char* m_region_info_list_to_string(List* regionInfoList){
	Buf* buf = init_buf();
	RegionInfo* regionInfo = NULL;
	while((regionInfo = list_next(regionInfoList)) != NULL){
		char* regionInfoString =  m_region_info_to_string(regionInfo);
		buf_cat(buf, regionInfoString, strlen(regionInfoString));
		buf_cat(buf, STRING_SEPARATOR_STRING, strlen(STRING_SEPARATOR_STRING));

		free(regionInfoString);
	}
	char* result = get_buf_data(buf);
	list_rewind(regionInfoList);
	trim(result, STRING_SEPARATOR);
	free_buf(buf);
	return result;
}

public List* load_region_info_list(char* file_path){
	List* regionInfoList = NULL;
	char* region_info_list_string = m_get_value_by_key(file_path, CONF_REGION_LIST_KEY);
	if(region_info_list_string != NULL) regionInfoList = string_to_region_info_list(region_info_list_string);
	else regionInfoList = list_create();

	free(region_info_list_string);
	return regionInfoList;
}

public void flush_region_info_list(char* file_path, List* regionInfoList){
	char* region_info_list_string = m_region_info_list_to_string(regionInfoList);
	flush_key_value(file_path, CONF_REGION_LIST_KEY, region_info_list_string);
	free(region_info_list_string);
}

private boolean match_column_family_info(void *columnFamilyInfo_void, void *column_family){
	ColumnFamilyInfo* columnFamilyInfo = (ColumnFamilyInfo*)columnFamilyInfo_void;
	if(match(columnFamilyInfo->column_family, (char*)column_family)) return true;
	else return false;
}

public ColumnFamilyInfo* get_column_family_info(List* columnFamilyInfoList, char* column_family){
	return list_find(columnFamilyInfoList, column_family, match_column_family_info);
}

public ColumnFamilyInfo* create_column_family_info(char* column_family){
	ColumnFamilyInfo* columnFamilyInfo = malloc(sizeof(ColumnFamilyInfo));
	columnFamilyInfo->column_family = m_cpy(column_family);
	columnFamilyInfo->replicaQueueList = list_create();
	return columnFamilyInfo;
}

public TabletInfo* create_tablet_info(RegionInfo *regionInfo, int begin_timestamp, int end_timestamp){
	TabletInfo* tabletInfo = malloc(sizeof(TabletInfo));
	tabletInfo->regionInfo = regionInfo;
	tabletInfo->begin_timestamp = begin_timestamp;
	tabletInfo->end_timestamp = end_timestamp;
	return tabletInfo;
}

public boolean match_tablet_info(void *tabletInfo_void, void *regionInfo){
	TabletInfo* tabletInfo = (TabletInfo*)tabletInfo_void;
	if(tabletInfo->regionInfo == regionInfo) return true;
	else return false;
}

public TabletInfo* get_tablet_info(List* tabletInfoList, RegionInfo* regionInfo){
	return list_find(tabletInfoList, regionInfo, match_tablet_info);
}

/* sample string: 127.0.0.1:8302,120000,12000 */
public char* m_tablet_info_to_string(TabletInfo* tabletInfo){
	Buf* buf = init_buf();
	char* region_info_string = m_region_info_to_string(tabletInfo->regionInfo);
	buf_cat(buf, region_info_string, strlen(region_info_string));
	free(region_info_string);
	buf_cat(buf, STRING_SEPARATOR_STRING, strlen(STRING_SEPARATOR_STRING));
	char* begin_timestamp_string = m_itos(tabletInfo->begin_timestamp);
	buf_cat(buf, begin_timestamp_string, strlen(begin_timestamp_string));
	free(begin_timestamp_string);
	buf_cat(buf, STRING_SEPARATOR_STRING, strlen(STRING_SEPARATOR_STRING));
	char* end_timestamp_string = m_itos(tabletInfo->end_timestamp);
	buf_cat(buf, end_timestamp_string, strlen(end_timestamp_string));
	free(end_timestamp_string);
	char *result = get_buf_data(buf);
	free_buf(buf);
	return result;
}

/* sample string: 127.0.0.1:8302,120000,12000 */
public TabletInfo* string_to_tablet_info(char* string){
	Tokens* tokens = init_tokens(string, STRING_SEPARATOR);
	RegionInfo* regionInfo = string_to_region_info(tokens->tokens[0]);
	int begin_timestamp = atoi(tokens->tokens[1]);
	int end_timestamp = atoi(tokens->tokens[2]);
	free_tokens(tokens);
	return create_tablet_info(regionInfo, begin_timestamp, end_timestamp);
}

public ReplicaQueue* create_replica_queue(int id){
	ReplicaQueue* replicaQueue = malloc(sizeof(ReplicaQueue));
	replicaQueue->id = id;
	replicaQueue->tabletInfoList = list_create();
	return replicaQueue;
}

public void free_replica_queue(void* replicaQueue_void){
	ReplicaQueue* replicaQueue = (ReplicaQueue*)replicaQueue_void;
	list_destory(replicaQueue->tabletInfoList, just_free);
	free(replicaQueue);
}

private boolean match_replica_queue(void *replicaQueue_void, void *id){
	ReplicaQueue* replicaQueue = (ReplicaQueue*)replicaQueue_void;
	if(replicaQueue->id == (int)id) return true;
	else return false;
}

public ReplicaQueue* get_replica_queue(List* replicaQueueList, int id){
	return list_find(replicaQueueList, (void*)id, match_replica_queue);
}

/* sample string: 127.0.0.1:8302,120000,12000;127.0.0.1:8303,120000,12000 */
public char* m_replica_queue_to_string(ReplicaQueue* replicaQueue){
	Buf* buf = init_buf();
	TabletInfo* tabletInfo = NULL;
	while((tabletInfo = list_next(replicaQueue->tabletInfoList)) != NULL){
		char* tablet_info_string = m_tablet_info_to_string(tabletInfo);
		buf_cat(buf, tablet_info_string, strlen(tablet_info_string));
		buf_cat(buf, ITEM_SEPARATOR_STRING, strlen(ITEM_SEPARATOR_STRING));
	}
	list_rewind(replicaQueue->tabletInfoList);
	char* result = get_buf_data(buf);
	trim(result, ITEM_SEPARATOR);
	free_buf(buf);
	return result;
}

/* sample string: 127.0.0.1:8302,120000,12000;127.0.0.1:8303,120000,12000 */
public ReplicaQueue* string_to_replica_queue(int id, char* string){
	ReplicaQueue* replicaQueue = create_replica_queue(id);
	Tokens* tokens = init_tokens(string, ITEM_SEPARATOR);
	int i=0;
	for(i=0; i<tokens->size; i++){
		TabletInfo* tabletInfo = string_to_tablet_info(tokens->tokens[i]);
		list_append(replicaQueue->tabletInfoList, tabletInfo);
	}
	free_tokens(tokens);
	return replicaQueue;
}

public TableInfo* create_table_info(char *table_name){
	TableInfo *tableInfo = malloc(sizeof(TableInfo));
	tableInfo->table_name = m_cpy(table_name);
	tableInfo->columnFamilyInfoList = list_create();
	return tableInfo;
}

private boolean match_table_info(void *tableInfo_void, void *table_name){
	TableInfo* table_info = (TableInfo*)tableInfo_void;
	if(match(table_info->table_name, (char*)table_name)) return true;
	else return false;
}

public TableInfo* get_table_info(List* tableInfoList, char* table_name){
	return list_find(tableInfoList, table_name, match_table_info);
}

/***
 * sample:
 * tableInfoList=people
 * people.default_cf.0=127.0.0.1:8302,120000,12000;127.0.0.1:8303,120000,12000
 * people.default_cf.1=127.0.0.1:8302,120000,12000;127.0.0.1:8303,120000,12000
 */
public List* load_table_info_list(char *file_path){
	List* tableInfoList = list_create();
	char* tableListString = m_get_value_by_key(file_path, CONF_TABLE_INFO_LIST_KEY);
	if(tableListString == NULL || strlen(tableListString) == 0) return tableInfoList;
	List* tableList = generate_list_by_token(tableListString, STRING_SEPARATOR);
	char* table_name;
	while((table_name = list_next(tableList)) != NULL){
		List* lines = get_lines_from_file_base_on_prefix(file_path, table_name);
		TableInfo* tableInfo = string_to_table_info(table_name, lines);
		list_append(tableInfoList, tableInfo);
		list_destory(lines, just_free);
	}
	list_destory(tableList, just_free);
	return tableInfoList;
}

public void destory_table_info(void* tableInfo_void){
	TableInfo* tableInfo = (TableInfo*)tableInfo_void;
	int i=0, size=list_size(tableInfo->columnFamilyInfoList);
	for(i=0; i<size; i++){
		ColumnFamilyInfo *columnFamilyInfo = list_get(tableInfo->columnFamilyInfoList, i);
		list_destory(columnFamilyInfo->replicaQueueList, free_replica_queue);
		free(columnFamilyInfo->column_family);
	}
	list_destory(tableInfo->columnFamilyInfoList, just_free);
	free(tableInfo->table_name);
	free(tableInfo);
}

/* Sample String:
 * people.default_cf.0=127.0.0.1:8302,120000,12000;127.0.0.1:8303,120000,12000;
 * people.default_cf.1=127.0.0.1:8302,120000,12000;127.0.0.1:8303,120000,12000;
 */
public TableInfo* string_to_table_info(char* table_name, List* lines){
	TableInfo* tableInfo = create_table_info(table_name);
	char* line = NULL;
	while((line = list_next(lines)) != NULL){
		char* key = m_get_key(line);
		Tokens* keyTokens = init_tokens(key, MID_SEPARATOR);
		if(keyTokens->size == 3){
			if(match(keyTokens->tokens[0], table_name)){
				ColumnFamilyInfo* columnFamilyInfo = get_column_family_info(tableInfo->columnFamilyInfoList, keyTokens->tokens[1]);
				if(columnFamilyInfo == NULL){
					columnFamilyInfo = create_column_family_info(keyTokens->tokens[1]);
					list_append(tableInfo->columnFamilyInfoList, columnFamilyInfo);
				}
				int id = atoi(keyTokens->tokens[2]);
				char* value = m_get_value(line);
				ReplicaQueue* replicaQueue = string_to_replica_queue(id, value);
				list_append(columnFamilyInfo->replicaQueueList, replicaQueue);
				free(value);
			}
		}
		free(key);
		free_tokens(keyTokens);
	}
	list_rewind(lines);
	return tableInfo;
}

/* Sample String:
 * people.default_cf.0=127.0.0.1:8302,120000,12000;127.0.0.1:8303,120000,12000;
 * people.default_cf.1=127.0.0.1:8302,120000,12000;127.0.0.1:8303,120000,12000;
 */
public char* table_info_to_string(TableInfo *tableInfo){
	Buf* buf = init_buf();
	int i=0, column_family_size=list_size(tableInfo->columnFamilyInfoList);
	for(i=0; i<column_family_size; i++){
		ColumnFamilyInfo* columnFamilyInfo = list_get(tableInfo->columnFamilyInfoList, i);
		int j=0, replica_queue_size=list_size(columnFamilyInfo->replicaQueueList);
		for(j=0;j<replica_queue_size;j++){
			ReplicaQueue* replicaQueue = list_get(columnFamilyInfo->replicaQueueList, j);
			char* int_string = m_itos(replicaQueue->id);
			char* key = m_cats(5, tableInfo->table_name, MID_SEPARATOR_STRING,
					columnFamilyInfo->column_family ,MID_SEPARATOR_STRING, int_string);
			char* value = m_replica_queue_to_string(replicaQueue);
			char* line = key_and_value_to_line(key, value);
			buf_cat(buf, line, strlen(line));
			buf_cat(buf, LINE_SEPARATOR_STRING, strlen(LINE_SEPARATOR_STRING));

			free(key);
			free(value);
			free(int_string);
		}
	}
	char* result = get_buf_data(buf);
	free_buf(buf);
	return result;
}

private char* table_info_list_to_string(List* tableInfoList){
	Buf* buf = init_buf();
	List* tableList = list_create();
	TableInfo *tableInfo = NULL;
	while((tableInfo = list_next(tableInfoList)) != NULL){
		char* tableInfoString = table_info_to_string(tableInfo);
		buf_cat(buf, tableInfoString, strlen(tableInfoString));
		list_append(tableList, m_cpy(tableInfo->table_name));

		free(tableInfoString);
	}
	list_rewind(tableInfoList);
	char* tableListStirng = key_and_value_list_to_line(CONF_TABLE_INFO_LIST_KEY, tableList);
	buf_cat(buf, tableListStirng, strlen(tableListStirng));
	buf_cat(buf, LINE_SEPARATOR_STRING, strlen(LINE_SEPARATOR_STRING));

	list_destory(tableList, just_free);
	free(tableListStirng);

	char* result = get_buf_data(buf);
	free_buf(buf);
	return result;
}

public void flush_table_info_list(char* file_path, List* tableInfoList){
	Buf* buf = init_buf();
	List* lines = get_lines_from_file(file_path);
	char* line = NULL;
	while((line = list_next(lines)) != NULL){
		//ignore table info list line, which will append at the ned
		char* key = m_get_key(line);
		if(match(key, CONF_TABLE_INFO_LIST_KEY)) continue;
		//check if the table related with table config
		Tokens* keyTokens = init_tokens(key, MID_SEPARATOR);
		if(keyTokens->size == 3 && get_table_info(tableInfoList, keyTokens->tokens[0]) != NULL) {
			free_tokens(keyTokens);
			continue;
		}
		free_tokens(keyTokens);
		buf_cat(buf, line, strlen(line));
		buf_cat(buf, LINE_SEPARATOR_STRING, strlen(LINE_SEPARATOR_STRING));

		free(key);
	}
	//flush the table info part
	byte* tableInfoListString = table_info_list_to_string(tableInfoList);
	buf_cat(buf, tableInfoListString, strlen(tableInfoListString));

	//printf("%s\n", bytes);
	create_or_rewrite_file(file_path, get_buf_data(buf));

	list_destory(lines, just_free);
	free(tableInfoListString);
	destory_buf(buf);
}

public char* get_conf_path_from_argv(int argc, char *argv[], char* default_conf_path){
	char *conf_path = NULL;
	if(argc==3){
		if(match(argv[1], CONF_OPTION_KEY))
			conf_path = argv[2];
	}else{
		if(get_file_size(default_conf_path) != -1)
			conf_path = default_conf_path;
	}
	return conf_path;
}

#ifdef CONF_TEST
void testcase_for_table_info_list(void){
	char* file_path = "conf/master.conf";
	List* tableInfoList = load_table_info_list(file_path);
	int i=0, table_info_size=list_size(tableInfoList);
	for(i=0; i<table_info_size; i++){
		TableInfo* tableInfo = list_get(tableInfoList, i);
		printf("%s\n",tableInfo->table_name);
		int j=0, column_family_size=list_size(tableInfo->columnFamilyInfoList);
		for(j=0; j<column_family_size; j++){
			ColumnFamilyInfo* columnFamilyInfo = list_get(tableInfo->columnFamilyInfoList, j);
			printf("%s\n",columnFamilyInfo->column_family);
			int k=0, replica_queue_size=list_size(columnFamilyInfo->replicaQueueList);
			for(k=0; k<replica_queue_size; k++){
				ReplicaQueue* replicaQueue = list_get(columnFamilyInfo->replicaQueueList, k);
				printf("%d\n",replicaQueue->id);
				int l=0, tablet_info_size=list_size(replicaQueue->tabletInfoList);
				for(l=0; l<tablet_info_size; l++){
					TabletInfo* tabletInfo = list_get(replicaQueue->tabletInfoList, l);
					printf("%s,", tabletInfo->regionInfo->conn);
					printf("%d,", tabletInfo->begin_timestamp);
					printf("%d\n", tabletInfo->end_timestamp);
				}
			}
		}
	}
	flush_table_info_list(file_path, tableInfoList);
}

void testcase_for_region_list(void){
	char* file_path = "conf/master.conf";
	List* regionInfoList = load_region_info_list(file_path);
	RegionInfo* region = NULL;
	while((region = list_next(regionInfoList)) != NULL){
		printf("%s\n", region->conn);
	}
}

void testcast_for_conf_list_to_string(void){
	List* list = list_create();
	list_append(list, m_cpy("test1"));
	list_append(list, m_cpy("test2"));
	list_append(list, m_cpy("test3"));
	char* key = "test";
	char* result = key_and_value_list_to_line(key, list);
	printf("%s\n", result);
	list_destory(list, just_free);
	free(result);
}

void testcase_for_m_get_target_value(void){
	char *file_path = "conf/region.conf";
	char *target_key = CONF_PORT_KEY;
	char *value = m_get_value_by_key(file_path, target_key);
	if(value != NULL) printf("%s\n", value);
	free(value);
}

void add_table_info(List* tableInfoList){
	TableInfo* tableInfo = create_table_info("test");
	list_append(tableInfoList, tableInfo);
}

void testcase_for_add_table_info(void){
	List* tableInfoList = list_create();
	add_table_info(tableInfoList);
	printf("The Size of table info list:%d\n", list_size(tableInfoList));
}

void test_suite(void){
	printf("The Test Suit for conf is starting\n");
	printf("0) testcase_for_table_info_list\n");
	testcase_for_table_info_list();
	printf("1) testcase_for_region_list\n");
	testcase_for_region_list();
	printf("2) testcast_for_conf_list_to_string\n");
	testcast_for_conf_list_to_string();
	printf("3) testcase_for_m_get_target_value\n");
	testcase_for_m_get_target_value();
	printf("4) testcase_for_add_table_info\n");
	testcase_for_add_table_info();
	printf("Completed Successfully\n");
}

int main(int argc, char *argv[]){
	test_suite();
	return 1;
}
#endif
