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

#define DEFAULT_FLUSH_CHECK_INTERVAL 10 * 60
#define DEFAULT_REGION_MAX_SIZE 10000

#define DEFAULT_WAL_FILE_PATH "wal.log"

#define REGION_FOLDER "."
#define LAST_FLUSHED_ID_KEY "last_flushed_id"
#define TIME_STAMP_DIV 60 * 60


/** This struct will also be used at cli side**/
typedef struct _Region{
	char* conf_path;
	int port;
	int max_size; /** The UNIT is MB, the max size of disk usage has been defined in the conf file**/
	int used_size; /** The UNIT is MB, the current disk usgae **/
	List* tabletList; /**only used in region server**/
	Wal* wal;
	long incr_item_id; /** the id for putted item in the memstore and wal **/
}Region;

/**
 * Sample region file folder hierarchy:
 * wal.log
 * tablet0/
 * tablet1/
 */

/** global singleton **/
Region *regionInst = NULL;

public long get_incr_item_id(){
		long id = regionInst->incr_item_id;
		regionInst->incr_item_id++;
		return id;
}

private Tablet* get_tablet(List* tabletList, char *table_name){
		Tablet *found = NULL, *tablet = NULL;
		while((tablet = list_next(tabletList)) != NULL){
			if(match_tablet(tablet, table_name)) found = tablet;
		}
		list_rewind(tabletList);
		return found;
}

private Tablet* get_tablet_by_id(List* tabletList, int tablet_id){
		Tablet *found = NULL, *tablet = NULL;
		while((tablet = list_next(tabletList)) != NULL){
			if(get_tablet_id(tablet) == tablet_id) found = tablet;
		}
		list_rewind(tabletList);
		return found;
}

private long get_last_flushed_id_from_conf(char* conf_path, char* tablet_folder){
		long last_flushed_id = 0;
		char* key = m_cats(3, tablet_folder, MID_SEPARATOR_STRING, LAST_FLUSHED_ID_KEY);
		char* value = m_get_value_by_key(conf_path, key);
		if(value != NULL) last_flushed_id = strtol(value, NULL, 10);

		frees(2, key, value);
		return last_flushed_id;
}

private void flush_tablets_info(char* file_path, List* tabletList){
		Tablet* tablet = NULL;
		while((tablet = list_next(tabletList)) != NULL){
			char* key = m_cats(3, get_tablet_folder(tablet), MID_SEPARATOR_STRING ,LAST_FLUSHED_ID_KEY);
			char* value = m_lltos(get_last_flushed_id(tablet));
			flush_key_value(file_path, key, value);

			frees(2, key, value);
		}
		list_rewind(tabletList);
}

private Region* init_region_struct(char *conf_path){
		//init the local region
		Region* region = malloc2(sizeof(Region));
		region->port = DEFAULT_LOCAL_REGION_PORT;
		region->used_size = 0;
		region->tabletList = list_create();
		region->incr_item_id = 0;
		region->conf_path = conf_path;

		int port = get_int_value_by_key(conf_path, CONF_PORT_KEY);
		if(port != 0) region->port = port;

		return region;
}

/** the method will reload wal log to tablet, and will return the max item id in wal log **/
private int reload_wal_to_tablet(Wal* wal, List* tabletList){
		List* newWalItems = list_create();
		int max = 0;
		List* walItems = load_log_wal(wal);
		WalItem* walItem = NULL;
		while((walItem = list_next(walItems)) != NULL){
			short tablet_id = get_tablet_id_wal_item(walItem);
			long item_id = get_item_id_wal_item(walItem);
			Tablet* tablet = get_tablet_by_id(tabletList, tablet_id);
			//TODO pls handle if tablet has not been found
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
		regionInst = init_region_struct(conf_path);
		DIR *regionFolder = opendir(REGION_FOLDER);
  		struct dirent *dp = NULL;
   		while ((dp = readdir(regionFolder)) != NULL) {
			//if d_type is 4, means it is a dir
  			if(dp->d_type == 4){
 				if(cmp(dp->d_name, TABLET_FOLDER_PREFIX, strlen(TABLET_FOLDER_PREFIX))){
					Tablet *tablet = load_tablet(dp->d_name);
					list_append(regionInst->tabletList, tablet);
					long last_flushed_id =  get_last_flushed_id_from_conf(regionInst->conf_path, dp->d_name);
					set_last_flushed_id(tablet, last_flushed_id);
					if(last_flushed_id > regionInst->incr_item_id)
						regionInst->incr_item_id = last_flushed_id;
				}
			}
		}
		regionInst->wal = load_wal(DEFAULT_WAL_FILE_PATH);
		closedir(regionFolder);
		if(need_to_reload_wal(regionInst->wal)){
			int max_item_id = reload_wal_to_tablet(regionInst->wal, regionInst->tabletList);
			regionInst->incr_item_id = max_item_id;
		}
}

public int available_space_region(void){
		return get_local_partition_free_space();
}

public boolean add_new_tablet_region(char *table_name){
		int tablet_id = list_size(regionInst->tabletList);
		char* next_tablet_folder = m_cats(2, TABLET_FOLDER_PREFIX, m_itos(tablet_id));
		Tablet *tablet = create_tablet(tablet_id, next_tablet_folder, table_name);
		list_append(regionInst->tabletList, tablet);
		return true;
}

public boolean put_data_region(char *table_name, ResultSet* resultSet){
		Tablet *tablet = get_tablet(regionInst->tabletList, table_name);
		int i=0;
		short tablet_id = get_tablet_id(tablet);
		for(i=0; i<resultSet->size;i++){
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
		int diff = syncJob->end_timestamp - syncJob->begin_timestamp;
		//TODO may need some interval for avoiding some contention
		//will retrieve certain amount of items base on time(default is one hour), and send the region node
		int i=0, size=diff/TIME_STAMP_DIV + 1;
		for(i=0; i<size; i++){
			int begin_timestamp = syncJob->begin_timestamp + i * TIME_STAMP_DIV;
			int end_timestamp = syncJob->begin_timestamp + (i+1)* TIME_STAMP_DIV;
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
					logg(ISSUE, "The target region node %s has problem during the syn job.", syncJob->target_conn);
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
		Tablet *tablet = NULL;
		while((tablet = list_next(regionInst->tabletList)) != NULL){
			used_size += get_used_size_tablet(tablet);
			refresh_tablet(tablet);
		}
		list_rewind(regionInst->tabletList);
		regionInst->used_size = used_size;
		flush_tablets_info(regionInst->conf_path, regionInst->tabletList);
}

void* flush_region_daemon_thread(void* nul){
		while(1){
			sleep(DEFAULT_FLUSH_CHECK_INTERVAL);
			logg(INFO, "The interval is up, check and flush region now.");
			check_and_flush_region();
			logg(INFO, "Finish the checking.");
		}
}

public RPCResponse* handler_region_request(char *cmd, List* params){
		RPCResponse *rpcResponse = NULL;
		if(match(ADD_NEW_TABLET_REGION_CMD, cmd)){
			char* table_name = get_param(params, 0);
			boolean bool = add_new_tablet_region(table_name);
			rpcResponse = create_rpc_response(SUCCESS, strlen(bool_to_str(bool)), m_cpy(bool_to_str(bool)));
		}else if(match(PUT_DATA_REGION_CMD, cmd)){
			char* table_name = get_param(params, 0);
			ResultSet* resultSet = byte_to_result_set(get_param(params, 1));
			boolean bool = put_data_region(table_name, resultSet);
			rpcResponse = create_rpc_response(SUCCESS, strlen(bool_to_str(bool)), m_cpy(bool_to_str(bool)));
		}else if(match(QUERY_ROW_REGION_CMD, cmd)){
			char* table_name = get_param(params, 0);
			char* row_key = get_param(params, 1);
			ResultSet* resultSet = query_row_region(table_name, row_key);
			Buf* buf = result_set_to_byte(resultSet);
			rpcResponse = create_rpc_response(SUCCESS, get_buf_index(buf), get_buf_data(buf));
			free_result_set(resultSet);
		}else if(match(QUERY_ALL_REGION_CMD, cmd)){
			char* table_name = get_param(params, 0);
			ResultSet* resultSet = query_all_region(table_name);
			Buf* buf = result_set_to_byte(resultSet);
			rpcResponse = create_rpc_response(SUCCESS, get_buf_index(buf), get_buf_data(buf));
			free_result_set(resultSet);
		}else if(match(GET_METADATA_REGION_CMD, cmd)){
			char* table_name = get_param(params, 0);
			char* metadata = get_metadata_region(table_name);
			rpcResponse = create_rpc_response(SUCCESS, strlen(metadata), metadata);
		}else if(match(AVAILABLE_SPACE_REGION_CMD, cmd)){
			int avail_space = available_space_region();
			char* result = m_itos(avail_space);
			rpcResponse = create_rpc_response(SUCCESS, strlen(result), result);
		}else if(match(TABLET_USED_SIZE_REGION_CMD, cmd)){
			char* table_name = get_param(params, 0);
			int used_size = tablet_used_size_region(table_name);
			char* string = m_itos(used_size);
			rpcResponse = create_rpc_response(SUCCESS, strlen(string), string);
		}else if(match(START_SYNC_REGION_CMD, cmd)){
			char* target_conn = get_param(params, 0);
			char* table_name = get_param(params, 1);
			long long begin_timestamp = get_param_int(params, 2);
			long long end_timestamp = get_param_int(params, 3);
			boolean bool =  start_sync_region(target_conn, table_name, begin_timestamp, end_timestamp);
			rpcResponse = create_rpc_response(SUCCESS, strlen(bool_to_str(bool)), m_cpy(bool_to_str(bool)));
		}else if(match(GET_ROLE_CMD, cmd)){
			rpcResponse = create_rpc_response(SUCCESS, strlen(REGION_KEY), m_cpy(REGION_KEY));
		}
		return rpcResponse;
}

public void start_server_region(){
		//Step 1. Start Flushing Thread
		pthread_t flush_thread_id;
		pthread_create(&flush_thread_id, NULL, flush_region_daemon_thread, (void*)NULL);
		//Step 2. Startup Region Server
		//TODO if falied, will try another port
		logg(INFO, "The region server is starting and will handle request.");
		startup(regionInst->port, handler_region_request);
}

/**
 * sample cmd: ./startRegion -conf conf/region.conf
 */
int main(int argc, char *argv[]){
		setup_logging(INFO, REGION_LOG_FILE);
  		char *conf_path = get_conf_path_from_argv(argc, argv, DEFAULT_REGION_CONF_PATH);
		load_local_region(conf_path);
		start_server_region();
		return 1;
}
