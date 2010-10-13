#ifndef CONF_H_
#define CONF_H_

#include "global.h"

/***
 * The conf file is serving two purposes at yuntable
 * 1) let user input some important configuration, such port number, max disk usage and etc.
 * 2) let user persist some important data, such as table info list, region list and etc.
 */

#define CONF_OPTION_KEY "-conf"
#define CONF_PORT_KEY "port"
#define CONF_DUPLCATE_NUM_KEY "duplicate_num"
#define CONF_REGION_LIST_KEY "regionList"
#define CONF_TABLE_INFO_LIST_KEY "tableInfoList"
#define CONF_TABLE_REGION_LIST_KEY "regions"

typedef struct _TableInfo{
	char* table_name;
	List* columnFamilyInfoList;
}TableInfo;

typedef struct _RegionInfo{
	char* conn;
	int avail_space;
	boolean connecting;
	boolean serving;
}RegionInfo;

typedef struct _TabletInfo{
	RegionInfo *regionInfo; /* a pointer */
	int begin_timestamp; /* The start of timestamp, if it is 0, means nothing has been set */
	int end_timestamp; /* The end of timestamp, if it is 0, means nothing has been set */
}TabletInfo;

typedef struct _ColumnFamilyInfo{
	char* column_family;
	List* replicaQueueList;
}ColumnFamilyInfo;

typedef struct _ReplicaQueue{
	int id;
	List* tabletInfoList;
}ReplicaQueue;

typedef struct _SyncJob{
	char* target_conn;
	char* table_name;
	char* column_family;
	int begin_timestamp;
	int end_timestamp;
}SyncJob;

/** if the target key has not been found, the method will return NULL **/
public char* m_get_value_by_key(char *file_path, char* target_key);

/* if the value can be find, it will return 0 */
public int get_int_value_by_key(char *file_path, char* target_key);

/** if the key exists, the old value will be replaced by the new value **/
public void flush_key_value(char* file_path, char* key, char* value);

public SyncJob* create_sync_job(char* target_conn, char* table_name, char* column_family, int begin_timestamp, int end_timestamp);

public RegionInfo* create_region_info(char* region_conn);

public RegionInfo* get_region_info(List* regionInfoList, char* region_conn);

public TabletInfo* create_tablet_info(RegionInfo *regionInfo, int begin_timestamp, int end_timestamp);

public boolean match_tablet_info(void *tabletInfo_void, void *regionInfo);

public TabletInfo* get_tablet_info(List* tabletInfoList, RegionInfo* regionInfo);

public ReplicaQueue* create_replica_queue(int id);

public ColumnFamilyInfo* create_column_family_info(char* column_family);

public ColumnFamilyInfo* get_column_family_info(List* columnFamilyInfoList, char* column_family);

public TableInfo* create_table_info(char *table_name);

public TableInfo* get_table_info(List* tableInfoList, char* table_name);

public void destory_table_info(void* tableInfo);

public TableInfo* create_table_info_with_region_conn_list(char *table_name, List* regionConnList);

public List* load_table_info_list(char *file_path);

public TableInfo* string_to_table_info(char* table_name, List* lines);

public char* table_info_to_string(TableInfo *tableInfo);

public void flush_table_info_list(char* file_path, List* tableInfoList);

public List* load_region_info_list(char* file_path);

public void flush_region_info_list(char* file_path, List* regionList);

public char* get_conf_path_from_argv(int argc, char *argv[], char* default_conf_path);

#endif /* CONF_H_ */
