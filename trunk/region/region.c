/**
 * Copyright 2011 Wu Zhu Hua
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

#include "utils.h"
#include "list.h"
#include "tablet.h"
#include "conf.h"
#include "rpc.h"
#include "region.h"
#include "wal.h"
#include "malloc2.h"
#include "log.h"
#include "buf.h"

#define DEFAULT_REGION_FLUSH_CHECK_INTERVAL 600 //10min

#define DEFAULT_HOTNESS_VALUE 3600 //One Hour

#define DEFAULT_WAL_FILE_PATH "wal.log"

#define REGION_FOLDER "."
#define TIME_STAMP_DIV 60 * 60 //Used at sync job

/** This struct will also be used at cli side**/
typedef struct _Region{
		char* conf_path;
		int port;
		int max_size; /** The UNIT is MB, the max size of disk usage has been defined in the conf file**/
		int used_size; /** The UNIT is MB, the current disk usage **/
		List* tabletList; /**only used in region server**/
		int flush_check_interval;
		int hotness_value; /** Defined the memory duration of data block **/
		Wal* wal;
		long long incr_item_id; /** the id for putted item in the memstore and wal **/
}Region;

/**
 * Sample region file folder hierarchy:
 * wal.log
 * tablet0/
 * tablet1/
 */

/** global singleton **/
Region *regionInst = NULL;

public long long get_incr_item_id(){
		long long id = regionInst->incr_item_id;
		regionInst->incr_item_id++;
		return id;
}

private Tablet* get_tablet(List* tabletList, char *table_name){
		Tablet *found = NULL, *tablet = NULL;
		while((tablet = list_next(tabletList)) != NULL){
			if(match_tablet(tablet, table_name)){
				 found = tablet;
				 break;
			}
		}
		list_rewind(tabletList);
		return found;
}

private boolean tablet_exist(List* tabletList, char *table_name){
		if(get_tablet(tabletList, table_name) == NULL){
			return false;
		}else{
			return true;
		}
}

/** If the tablet has been found, the method will return NULL **/
private Tablet* get_tablet_by_id(List* tabletList, int tablet_id){
		Tablet *tablet = NULL, *temp = NULL;
		while((temp = list_next(tabletList)) != NULL){
			if(get_tablet_id(temp) == tablet_id){
				 tablet = temp;
			}
		}
		list_rewind(tabletList);
		return tablet;
}

/** If the last flushed id can not be found at conf file, the method will return 0 **/
private long long get_last_flushed_id_from_conf(char* conf_path, char* tablet_folder){
		long long last_flushed_id = 0;
		char* key = m_cats(3, tablet_folder, MID_SEPARATOR_STRING, CONF_LAST_FLUSHED_ID_KEY);
		char* value = m_get_value_by_key(conf_path, key);
		if(value != NULL){
			 //Since it is string format, it will use atoll
			 last_flushed_id = atoll(value);
		}
		frees(2, key, value);
		return last_flushed_id;
}

private void flush_tablets_info(char* file_path, List* tabletList){
		logg(INFO, "Flushing the all tablets Info to file %s.", file_path);
		int i=0;
		int tablet_size = list_size(tabletList);
		for(i=0; i<tablet_size; i++){
			Tablet* tablet = list_get(tabletList, i);
			char* key = m_cats(3, get_tablet_folder(tablet), MID_SEPARATOR_STRING ,CONF_LAST_FLUSHED_ID_KEY);
			char* value = m_lltos(get_last_flushed_id(tablet));
			flush_key_value(file_path, key, value);
			frees(2, key, value);
		}
}

private Region* init_region_struct(char *conf_path){
		//init the local region
		Region* region = malloc2(sizeof(Region));
		region->conf_path = m_cpy(conf_path);
		region->used_size = 0;
		region->tabletList = list_create();
		region->incr_item_id = 0;
		int port = get_int_value_by_key(conf_path, CONF_PORT_KEY);
		if(port != INDEFINITE){
			region->port = port;
		}else{
			region->port = DEFAULT_LOCAL_REGION_PORT;
		}
		int flush_check_interval = get_int_value_by_key(conf_path, CONF_FLUSH_CHECK_INTERVAL_KEY);
		if(flush_check_interval != INDEFINITE){
			region->flush_check_interval = flush_check_interval;
		}else{
			region->flush_check_interval = DEFAULT_REGION_FLUSH_CHECK_INTERVAL;
		}
		int hotness_value =  get_int_value_by_key(conf_path, CONF_HOTNESS_VALUE_KEY);
		if(hotness_value != INDEFINITE){
			region->hotness_value = hotness_value;
		}else{
			region->hotness_value = DEFAULT_HOTNESS_VALUE;
		}
		return region;
}

/**
 * The method will reload wal log to tablet, and will return the max item id in wal log **/
private int reload_wal_to_tablet(Wal* wal, List* tabletList){
		logg(INFO, "Reloading the Wal Items to tablet.");
		List* newWalItems = list_create();
		int max = 0;
		List* walItems = load_log_wal(wal);
		WalItem* walItem = NULL;
		while((walItem = list_next(walItems)) != NULL){
			short tablet_id = get_tablet_id_wal_item(walItem);
			long item_id = get_item_id_wal_item(walItem);
			Tablet* tablet = get_tablet_by_id(tabletList, tablet_id);
			if(tablet == NULL){
				logg(EMERG, "The Tablet %d not found!!!");
				abort();
			}
			long last_flushed_id = get_last_flushed_id(tablet);
			//if last_flushed_id == 0, means nothing has been flushed
			if(item_id > last_flushed_id || last_flushed_id == 0){
				put_tablet(tablet, item_id, get_item_wal_item(walItem));
				list_append(newWalItems, walItem);
			}
			max = item_id;
		}
		//clean wal file by creating a wal life only has fresh data
		refresh_wal(wal, newWalItems);
		list_destory(walItems, free_wal_item_void);
		list_destory(newWalItems, only_free_struct);
		return max;
}

public void load_local_region(char *conf_path){
		logg(INFO, "Loading the local region, the conf path is %s.", conf_path);
		regionInst = init_region_struct(conf_path);
		DIR *regionFolder = opendir(REGION_FOLDER);
  		struct dirent *dp = NULL;
   		while ((dp = readdir(regionFolder)) != NULL) {
			//if d_type is 4, means it is a dir
  			if(dp->d_type == 4){
 				if(cmp(dp->d_name, TABLET_FOLDER_PREFIX, strlen(TABLET_FOLDER_PREFIX))){
					Tablet *tablet = load_tablet(dp->d_name);
					list_append(regionInst->tabletList, tablet);
					long last_flushed_id = get_last_flushed_id_from_conf(regionInst->conf_path, dp->d_name);
					set_last_flushed_id(tablet, last_flushed_id);
					if(last_flushed_id > regionInst->incr_item_id)
						regionInst->incr_item_id = last_flushed_id;
					//Set the last flush id as the tablet's max item id
					set_max_item_id(tablet, last_flushed_id);
				}
			}
		}
		regionInst->wal = load_wal(DEFAULT_WAL_FILE_PATH);
		closedir(regionFolder);
		if(need_to_reload_wal(regionInst->wal)){
			int max_item_id = reload_wal_to_tablet(regionInst->wal, regionInst->tabletList);
			if(max_item_id > regionInst->incr_item_id){
				regionInst->incr_item_id = max_item_id;
			}
		}
}

public int available_space_region(void){
		return get_local_partition_free_space();
}

public boolean add_new_tablet_region(char *table_name){
		logg(INFO, "Creating a new tablet at the local region for table %s.", table_name);
		int tablet_id = list_size(regionInst->tabletList);
		char* next_tablet_folder = m_cats(2, TABLET_FOLDER_PREFIX, m_itos(tablet_id));
		Tablet *tablet = create_tablet(tablet_id, next_tablet_folder, table_name);
		list_append(regionInst->tabletList, tablet);
		logg(INFO, "The new tablet for table %s has been created.", table_name);
		return true;
}

public boolean put_data_region(char *table_name, ResultSet* resultSet){
		Tablet *tablet = get_tablet(regionInst->tabletList, table_name);
		int i=0;
		short tablet_id = get_tablet_id(tablet);
		for(i=0; i<resultSet->size; i++){
			long incr_item_id = get_incr_item_id();
			Item* item = resultSet->items[i];
			WalItem* walItem = create_wal_item(tablet_id, incr_item_id, item);
			append_wal_item(regionInst->wal, walItem);
			put_tablet(tablet, incr_item_id, resultSet->items[i]);
		}
		return true;
}

public ResultSet* query_row_region(char* table_name, char* row_key){
		ResultSet* resultSet = NULL;
		Tablet *tablet = get_tablet(regionInst->tabletList, table_name);
		if(tablet == NULL){
			resultSet = m_create_result_set(0, NULL);
		}else{
			resultSet = query_tablet_row_key(tablet, row_key);
		}
		return resultSet;
}

public ResultSet* query_all_region(char* table_name){
		ResultSet* resultSet = NULL;
		Tablet *tablet = get_tablet(regionInst->tabletList, table_name);
		if(tablet == NULL){
			resultSet = m_create_result_set(0, NULL);
		}else{
			resultSet = query_tablet_all(tablet);
		}
		return resultSet;
}

public char* get_metadata_region(char* table_name){
	    Tablet* tablet = get_tablet(regionInst->tabletList, table_name);
	    return get_metadata_tablet(tablet);
}

void* sync_job(void* syncJob_void){
		SyncJob* syncJob = (SyncJob*)syncJob_void;
		Tablet* tablet = get_tablet(regionInst->tabletList, syncJob->table_name);
		long long diff = syncJob->end_timestamp - syncJob->begin_timestamp;
		//TODO may need some interval for avoiding some contention
		//will retrieve certain amount of items base on time(default is one hour), and send the region node
		int i=0, size=diff/TIME_STAMP_DIV + 1;
		for(i=0; i<size; i++){
			long long begin_timestamp = syncJob->begin_timestamp + i * TIME_STAMP_DIV;
			long long end_timestamp = syncJob->begin_timestamp + (i+1)* TIME_STAMP_DIV;
			if(end_timestamp > syncJob->end_timestamp) end_timestamp = syncJob->end_timestamp;
			ResultSet* resultSet = query_tablet_by_timestamp(tablet, begin_timestamp, end_timestamp);
			if(resultSet->size > 0){
				List* params = generate_charactor_params(1, syncJob->table_name);
				Buf* buf = result_set_to_byte(resultSet);
				add_param(params, get_buf_index(buf), get_buf_data(buf));
				RPCRequest* rpcRequest = create_rpc_request(PUT_DATA_REGION_CMD, params);
				RPCResponse* rpcResponse = connect_conn(syncJob->target_conn, rpcRequest);
			    if(get_status_code(rpcResponse) == SUCCESS || get_result_length(rpcResponse) > 0){
			    	boolean result = stob(get_result(rpcResponse));
			    	if(!result){
			    		logg(ISSUE, "The sync job to region node %s has met some problems.", syncJob->target_conn);
			    	}
				}else{
					logg(ISSUE, "The target r tablet = temp;egion node %s has problem during the syn job.", syncJob->target_conn);
					//TODO handle the failure situation
				}
				destory_rpc_request(rpcRequest);
				destory_rpc_response(rpcResponse);
			}
			free_result_set(resultSet);
		}
		return NULL;
}

/* if this cmd returns 0, means the table has not been created */
public int tablet_used_size_region(char* table_name){
		int used_size = 0;
		Tablet *tablet = get_tablet(regionInst->tabletList, table_name);
		if(tablet!=NULL) used_size = get_used_size_tablet(tablet);
		return used_size;
}

public boolean start_sync_region(char* target_conn, char* table_name, long long begin_timestamp, long long end_timestamp){
		logg(INFO, "The new sync job for %s is starting.", target_conn);
		SyncJob* syncJob = create_sync_job(target_conn, table_name, begin_timestamp, end_timestamp);
		pthread_t thread_id;
		pthread_create(&thread_id, NULL, sync_job, (void*)syncJob);
		return true;
}

private void check_and_flush_region(void){
		int used_size = 0;
		int tablet_size = list_size(regionInst->tabletList);
		int i=0;
		for(i=0; i<tablet_size; i++){
			Tablet* tablet = list_get(regionInst->tabletList, i);
			used_size += get_used_size_tablet(tablet);
			refresh_tablet(tablet, regionInst->hotness_value);
		}
		regionInst->used_size = used_size;
		flush_tablets_info(regionInst->conf_path, regionInst->tabletList);
}

void* flush_region_daemon_thread(void* nul){
		while(1){
			sleep(regionInst->flush_check_interval);
			logg(INFO, "The interval is up, check and flush region now.");
			check_and_flush_region();
			logg(INFO, "Finish the checking.");
		}
}

public RPCResponse* handler_region_request(char *cmd, List* params){
		RPCResponse *rpcResponse = NULL;
		if(match(ADD_NEW_TABLET_REGION_CMD, cmd)){
			char* table_name = get_param(params, 0);
			if(table_name == NULL){
				rpcResponse = create_rpc_response(ERROR_NO_PARAM, 0, NULL);
			}else{
				boolean bool = add_new_tablet_region(table_name);
				rpcResponse = create_rpc_response(SUCCESS, strlen(bool_to_str(bool)), m_cpy(bool_to_str(bool)));
			}
		}else if(match(PUT_DATA_REGION_CMD, cmd)){
			char* table_name = get_param(params, 0);
			byte* result_set_bytes = get_param(params, 1);
			if(table_name == NULL || result_set_bytes == NULL){
				rpcResponse = create_rpc_response(ERROR_NO_PARAM, 0, NULL);
			}else if(!tablet_exist(regionInst->tabletList, table_name)){
				rpcResponse = create_rpc_response(ERROR_TABLET_NOT_EXIST, 0, NULL);
			}else{
				ResultSet* resultSet = byte_to_result_set(result_set_bytes);
				boolean bool = put_data_region(table_name, resultSet);
				rpcResponse = create_rpc_response(SUCCESS, strlen(bool_to_str(bool)), m_cpy(bool_to_str(bool)));
			}
		}else if(match(QUERY_ROW_REGION_CMD, cmd)){
			char* table_name = get_param(params, 0);
			char* row_key = get_param(params, 1);
			if(table_name == NULL || row_key == NULL){
				rpcResponse = create_rpc_response(ERROR_NO_PARAM, 0, NULL);
			}else if(!tablet_exist(regionInst->tabletList, table_name)){
				rpcResponse = create_rpc_response(ERROR_TABLET_NOT_EXIST, 0, NULL);
			}else{
				ResultSet* resultSet = query_row_region(table_name, row_key);
				Buf* buf = result_set_to_byte(resultSet);
				rpcResponse = create_rpc_response(SUCCESS, get_buf_index(buf), get_buf_data(buf));
				free_result_set(resultSet);
			}
		}else if(match(QUERY_ALL_REGION_CMD, cmd)){
			char* table_name = get_param(params, 0);
			if(table_name == NULL){
				rpcResponse = create_rpc_response(ERROR_NO_PARAM, 0, NULL);
			}else if(!tablet_exist(regionInst->tabletList, table_name)){
				rpcResponse = create_rpc_response(ERROR_TABLET_NOT_EXIST, 0, NULL);
			}else{
				ResultSet* resultSet = query_all_region(table_name);
				Buf* buf = result_set_to_byte(resultSet);
				rpcResponse = create_rpc_response(SUCCESS, get_buf_index(buf), get_buf_data(buf));
				free_result_set(resultSet);
			}
		}else if(match(GET_METADATA_REGION_CMD, cmd)){
			char* table_name = get_param(params, 0);
			if(table_name == NULL){
				rpcResponse = create_rpc_response(ERROR_NO_PARAM, 0, NULL);
			}else if(!tablet_exist(regionInst->tabletList, table_name)){
				rpcResponse = create_rpc_response(ERROR_TABLET_NOT_EXIST, 0, NULL);
			}else{
				char* metadata = get_metadata_region(table_name);
				rpcResponse = create_rpc_response(SUCCESS, strlen(metadata), metadata);
			}
		}else if(match(AVAILABLE_SPACE_REGION_CMD, cmd)){
			int avail_space = available_space_region();
			char* result = m_itos(avail_space);
			rpcResponse = create_rpc_response(SUCCESS, strlen(result), result);
		}else if(match(TABLET_USED_SIZE_REGION_CMD, cmd)){
			char* table_name = get_param(params, 0);
			if(table_name == NULL){
				rpcResponse = create_rpc_response(ERROR_NO_PARAM, 0, NULL);
			}else if(!tablet_exist(regionInst->tabletList, table_name)){
				rpcResponse = create_rpc_response(ERROR_TABLET_NOT_EXIST, 0, NULL);
			}else{
				int used_size = tablet_used_size_region(table_name);
				char* string = m_itos(used_size);
				rpcResponse = create_rpc_response(SUCCESS, strlen(string), string);
			}
		}else if(match(START_SYNC_REGION_CMD, cmd)){
			char* target_conn = get_param(params, 0);
			char* table_name = get_param(params, 1);
			byte* begin_timestamp_bytes = get_param(params, 2);
			byte* end_timestamp_bytes = get_param(params, 3);
			if(target_conn == NULL || table_name == NULL || begin_timestamp_bytes == NULL
					|| end_timestamp_bytes == NULL){
				rpcResponse = create_rpc_response(ERROR_NO_PARAM, 0, NULL);
			}else if(!tablet_exist(regionInst->tabletList, table_name)){
				rpcResponse = create_rpc_response(ERROR_TABLET_NOT_EXIST, 0, NULL);
			}else{
				long long begin_timestamp = btoll(begin_timestamp_bytes);
				long long end_timestamp = btoll(end_timestamp_bytes);
				boolean bool =  start_sync_region(target_conn, table_name, begin_timestamp, end_timestamp);
				rpcResponse = create_rpc_response(SUCCESS, strlen(bool_to_str(bool)), m_cpy(bool_to_str(bool)));
			}
		}else if(match(GET_ROLE_CMD, cmd)){
			rpcResponse = create_rpc_response(SUCCESS, strlen(REGION_KEY), m_cpy(REGION_KEY));
		}else{
			rpcResponse = create_rpc_response(ERROR_WRONG_CMD, 0, NULL);
		}
		return rpcResponse;
}

public void start_server_region(){
		//Step 1. Start Flushing Thread
		pthread_t flush_thread_id;
		pthread_create(&flush_thread_id, NULL, flush_region_daemon_thread, (void*)NULL);
		logg(INFO, "The Region flushing thread has started.");
		//Step 2. Startup Region Server
		logg(INFO, "The Region server is starting at %d and will handle request.", regionInst->port);
		logg(INFO, "The Region Conf File is at %s.", regionInst->conf_path);
		logg(INFO, "The Region Flushing Checking Interval is %d.", regionInst->flush_check_interval);
		logg(INFO, "The Region Hotness Value is %d.", regionInst->hotness_value);

		startup(regionInst->port, handler_region_request);
}

/**
 * sample cmd: ./startRegion -conf conf/region.conf
 */
int main(int argc, char *argv[]){
		setup_logging(INFO, REGION_LOG_FILE);
		logg(INFO, "The YunTable Version is %s.", VERSION);
  		char *conf_path = get_conf_path_from_argv(argc, argv, DEFAULT_REGION_CONF_PATH);
		load_local_region(conf_path);
		start_server_region();
		return 1;
}
