#include "utils.h"
#include "list.h"
#include "item.h"
#include "msg.h"
#include "master.h"
#include "region.h"
#include "conf.h"
#include "rpc.h"
#include "malloc2.h"
#include "log.h"

#define DEFAULT_DUPLICATE_NUM 3
#define MIN_REGION_AVAILABLE_SIZE 1000
#define DEFAULT_MASTER_DAEMON_SLEEP_INTERVAL 10 * 60

typedef struct _Master{
	char* conf_path;
	int port;
	int duplicate_num;
	List* regionInfoList;
	List* tableInfoList;
}Master;

/**global singleton **/
Master *masterInst = NULL;

/* currently only update the available size of region info */
private void update_region_info(RegionInfo* regionInfo){
		//TODO handle the failure situation
		regionInfo->connecting = true;
		RPCRequest* rpcRequest = create_rpc_request(AVAILABLE_SPACE_REGION_CMD, NULL);
		RPCResponse* rpcResponse = connect_conn(regionInfo->conn, rpcRequest);
	    if(get_status_code(rpcResponse) == SUCCESS || get_result_length(rpcResponse) > 0){
	    	regionInfo->avail_space = atoi(get_result(rpcResponse));
	    }else{
	    	//TODO need to use the async mode
	    	check_problem_region_master(regionInfo->conn);
	    }
	    destory_rpc_request(rpcRequest);
	    destory_rpc_response(rpcResponse);
}
//TODO check the effectiveness
private int cmp_region_info(void* regionInfo1_void, void* regionInfo2_void){
		RegionInfo* regionInfo1 = (RegionInfo*)regionInfo1_void;
		RegionInfo* regionInfo2 = (RegionInfo*)regionInfo2_void;
		if(regionInfo1->connecting == regionInfo2->connecting){
			return regionInfo1->avail_space > regionInfo2->avail_space;
		}else
			return regionInfo1->connecting > regionInfo2->connecting;
}

private RegionInfo* get_region_and_update(List* regionInfoList, int index){
		//TODO may need to update region's avail size and sort
		return list_get(regionInfoList, index);
}

/** If the method can not get correct tablet used size, the system will return INDEFINITE(-1) **/
private int get_tablet_used_size(RegionInfo* regionInfo, char* table_name){
		int tablet_used_size = INDEFINITE;
		List* params = generate_charactor_params(1, table_name);
		RPCRequest* rpcRequest = create_rpc_request(TABLET_USED_SIZE_REGION_CMD, params);
		RPCResponse* rpcResponse = connect_conn(regionInfo->conn, rpcRequest);
	    if(get_status_code(rpcResponse) == SUCCESS || get_result_length(rpcResponse) > 0){
	    	tablet_used_size = atoi(get_result(rpcResponse));
	    }else{
	    	//TODO need to use the async mode
	    	check_problem_region_master(regionInfo->conn);
	    }
	    destory_rpc_request(rpcRequest);
	    destory_rpc_response(rpcResponse);
		return tablet_used_size;
}

private List* sort_region_info_list(List* regionInfoList){
		int size = list_size(regionInfoList);
		RegionInfo** regionInfos = (RegionInfo**)list_to_array(regionInfoList);
		qsort(regionInfos, size, sizeof(RegionInfo*),  cmp_region_info);
		list_destory(regionInfoList, only_free_struct);
		regionInfoList = array_to_list((void**)regionInfos, size);
		return regionInfoList;
}

/* This cmd for registering a new region node */
public boolean add_new_region_master(char* region_conn){
		//check if the region has existed
		RegionInfo* regionInfo = get_region_info(masterInst->regionInfoList, region_conn);
		if(regionInfo != NULL) return false;
		if(!check_node_validity(region_conn, REGION_KEY)) return false;
		RegionInfo* newRegionInfo = create_region_info(region_conn);
		update_region_info(newRegionInfo);
		list_append(masterInst->regionInfoList, newRegionInfo);
		flush_region_info_list(masterInst->conf_path, masterInst->regionInfoList);
		return true;
}

/** if new tablet can not be created, the method will return NULL **/
private TabletInfo* create_new_tablet(RegionInfo *regionInfo, char* table_name){
		TabletInfo* tabletInfo = NULL;
		List* params = generate_charactor_params(1, table_name);
		RPCRequest* rpcRequest = create_rpc_request(ADD_NEW_TABLET_REGION_CMD, params);
		RPCResponse* rpcResponse = connect_conn(regionInfo->conn, rpcRequest);
		if(get_status_code(rpcResponse) == SUCCESS || get_result_length(rpcResponse) > 0){
			boolean bool = stob(get_result(rpcResponse));
			if(bool){
				int begin_timestamp = time(0);
				tabletInfo = create_tablet_info(regionInfo, begin_timestamp, 0);
			}
		}else{
			//TODO need to use the async mode
			check_problem_region_master(regionInfo->conn);
		}
	    destory_rpc_request(rpcRequest);
		destory_rpc_response(rpcResponse);
		return tabletInfo;
}

public boolean create_new_table_master(char* table_name){
		boolean result = false;
		if(get_table_info(masterInst->tableInfoList, table_name) != NULL) return false;
		//Step 1. Create New Table Info Struct
		TableInfo* tableInfo = create_table_info(table_name);
		list_append(masterInst->tableInfoList, tableInfo);
		//Step 2. Create Replica Queue
		//TODO create Replica Queue base on duplicate number
		ReplicaQueue* replicaQueue = create_replica_queue(0);
		list_append(tableInfo->replicaQueueList, replicaQueue);
		RegionInfo* regionInfo = get_region_and_update(masterInst->regionInfoList, 0);
		TabletInfo* tabletInfo = create_new_tablet(regionInfo, table_name);
		if(tabletInfo != NULL){
			list_append(replicaQueue->tabletInfoList, tabletInfo);
			result = true;
		}
		flush_table_info_list(masterInst->conf_path, masterInst->tableInfoList);
		return result;
}

private boolean region_has_problem(RegionInfo* regionInfo){
		if(regionInfo->connecting == false) return true;
		if(regionInfo->serving ==true && regionInfo->avail_space < MIN_REGION_AVAILABLE_SIZE) return true;
		return false;
}

/* check if the replica queue is good or whether the probelsm region has been used in this queue  */
private boolean is_good_replica_queue(ReplicaQueue* replicaQueue, List* problemRegions){
		int i=0, size=list_size(replicaQueue->tabletInfoList);
		for(i=0; i<size; i++){
			TabletInfo* tabletInfo = list_get(replicaQueue->tabletInfoList, i);
			if(get_region_info(problemRegions, tabletInfo->regionInfo->conn)!=NULL) return false;
		}
		return true;
}

/* find the tablets from good replica queue that matchs infectedTablet's begin_timestamp and end_timestamp,  */
private List* find_src_tablets(TableInfo* tableInfo, TabletInfo* infectedTabletInfo, List* problemRegions){
		List* tabletList = list_create();
		int i=0, replica_queue_size=list_size(tableInfo->replicaQueueList);
		for(i=0; i<replica_queue_size; i++){
			ReplicaQueue* replicaQueue = list_get(tableInfo->replicaQueueList, i);
			if(!is_good_replica_queue(replicaQueue, problemRegions)) continue;
			int j=0, tablet_info_size=list_size(replicaQueue->tabletInfoList);
			for(j=0;j<tablet_info_size;j++){
				TabletInfo* tabletInfo = list_get(replicaQueue->tabletInfoList, j);
				if(match_by_timestamps(tabletInfo->begin_timestamp, tabletInfo->end_timestamp,
						infectedTabletInfo->begin_timestamp, infectedTabletInfo->end_timestamp))
					list_append(tabletList, tabletInfo);
			}
		}
		return tabletList;
}

private boolean start_sync_job(char* src_conn, char* target_conn, char* table_name,
		int begin_timestamp, int end_timestamp){
		boolean result = false;
		List* params = generate_charactor_params(2, target_conn, table_name);
		add_int_param(params, begin_timestamp);
		add_int_param(params, end_timestamp);
		RPCRequest* rpcRequest = create_rpc_request(START_SYNC_REGION_CMD, params);
		RPCResponse* rpcResponse = connect_conn(src_conn, rpcRequest);
	    if(get_status_code(rpcResponse) == SUCCESS || get_result_length(rpcResponse) > 0){
		    result = stob(get_result(rpcResponse));
	    }else{
	    	//TODO need to use the async mode
	    	check_problem_region_master(src_conn);
	    }
	    destory_rpc_request(rpcRequest);
		destory_rpc_response(rpcResponse);
		return result;
}

private boolean handle_infected_tablet(TabletInfo* infectedTabletInfo, ReplicaQueue* replicaQueue,
		TableInfo* tableInfo, List* problemRegions, List* regionInfoList){
		RegionInfo* firstRegionInfo = get_region_and_update(regionInfoList, 0);
		if(region_has_problem(firstRegionInfo)) return false;
		TabletInfo* firstTabletInfo = create_new_tablet(firstRegionInfo, tableInfo->table_name);
		//if region info is connecting, which means it is full, will just assign new tablet to serving
		if(infectedTabletInfo->regionInfo->connecting == true){
			infectedTabletInfo->end_timestamp = time(0);
			//needs to new region that worksing
			infectedTabletInfo->regionInfo->serving = false;
			list_append(replicaQueue->tabletInfoList, firstTabletInfo);
		}else{
			//found the src region conn from the, this is for stroing old data
			List* src_tablets = find_src_tablets(tableInfo, infectedTabletInfo, problemRegions);
			list_replace(replicaQueue->tabletInfoList, infectedTabletInfo, firstTabletInfo);
			//if the infectedTabletInfo is serving now, will add one more
			if(infectedTabletInfo->end_timestamp == 0){
				infectedTabletInfo->end_timestamp = time(0);
				RegionInfo* secondRegionInfo = get_region_and_update(regionInfoList, 1);
				if(region_has_problem(secondRegionInfo)) return false;
				TabletInfo* secondTabletInfo = create_new_tablet(firstRegionInfo, tableInfo->table_name);
				list_append(replicaQueue->tabletInfoList, secondTabletInfo);
			}
			firstTabletInfo->begin_timestamp = infectedTabletInfo->begin_timestamp ;
			firstTabletInfo->end_timestamp = infectedTabletInfo->end_timestamp;
			//start sync
			int i=0, stt_size=list_size(src_tablets);
			for(i=0; i<stt_size; i++){
				TabletInfo* srcTabletInfo = list_get(src_tablets, i);
				//this node is for serving
				int tablet_used_size = get_tablet_used_size(srcTabletInfo->regionInfo, tableInfo->table_name);
				if(firstRegionInfo->avail_space < tablet_used_size) return false;
				list_append(replicaQueue->tabletInfoList, firstTabletInfo);
				//send the data from src tablet to the target tablet
				start_sync_job(srcTabletInfo->regionInfo->conn, firstRegionInfo->conn, tableInfo->table_name,
						infectedTabletInfo->begin_timestamp, infectedTabletInfo->end_timestamp);
			}
			//remove the infectedTabletInfo from the replicaQueue
			list_remove(replicaQueue->tabletInfoList, infectedTabletInfo, just_free);
		}

		return true;
}

private void handle_problem_regions(List* problemRegions, Master* master){
		RegionInfo* regionInfo = NULL;
		while((regionInfo = list_next(problemRegions))){
			//iterator table, column family and replica queue
			int i=0, table_info_size=list_size(master->tableInfoList);
			for(i=0; i<table_info_size; i++){
				TableInfo* tableInfo = list_get(master->tableInfoList, i);
				int k=0, replica_queue_size=list_size(tableInfo->replicaQueueList);
				for(k=0; k<replica_queue_size; k++){
					ReplicaQueue* replicaQueue = list_get(tableInfo->replicaQueueList, k);
					TabletInfo* infectedTabletInfo = get_tablet_info(replicaQueue->tabletInfoList, regionInfo);
					if(infectedTabletInfo!=NULL) handle_infected_tablet(infectedTabletInfo, replicaQueue,
						tableInfo, problemRegions, master->regionInfoList);
				}
			}
		}
}

/* This cmd will ask the master to check the problem node immediately,
 * if the method return false means the node is working properly,
 * if the method return true means the node got some problem, it is fixing now.
 *  */
public boolean check_problem_region_master(char* problem_region_conn){
		boolean result = false;
		logg(ISSUE, "The Region Node %s may have some problem.", problem_region_conn);
		logg(ISSUE, "This routine has not been tested.");
		RegionInfo* regionInfo = get_region_info(masterInst->regionInfoList, problem_region_conn);
		update_region_info(regionInfo);
		sort_region_info_list(masterInst->regionInfoList);
		if(region_has_problem(regionInfo)){
			result = true;
			List* problem_regions = list_create();
			list_append(problem_regions, regionInfo);
			handle_problem_regions(problem_regions, masterInst);
		}
		flush_table_info_list(masterInst->conf_path, masterInst->tableInfoList);
		return result;
}

private void update_master_info(Master* master){
		List* problem_regions = list_create();
		//Update Region Info
		int i=0, size=list_size(master->regionInfoList);
		for(i=0; i<size; i++){
			RegionInfo* regionInfo = list_get(master->regionInfoList, i);
			update_region_info(regionInfo);
			if(region_has_problem(regionInfo)) list_append(problem_regions, regionInfo);
		}
		//sort region info list
		sort_region_info_list(master->regionInfoList);
		if(list_size(problem_regions)>0) handle_problem_regions(problem_regions, masterInst);
		flush_table_info_list(master->conf_path, master->tableInfoList);
}

private Master* init_master_struct(){
		Master* master = malloc2(sizeof(Master));
		master->conf_path = m_cpy(DEFAULT_MASTER_CONF_PATH);
		master->port = DEFAULT_MASTER_PORT;
		master->duplicate_num = DEFAULT_DUPLICATE_NUM;
		master->regionInfoList = list_create();
		master->tableInfoList = list_create();
		return master;
}

public void load_master(char *conf_path){
		masterInst = init_master_struct();
		if(conf_path != NULL){
			masterInst->conf_path = m_cpy(conf_path);
			int port = get_int_value_by_key(conf_path, CONF_PORT_KEY);
			if(port != 0) masterInst->port = port;
			int	duplicate_num = get_int_value_by_key(conf_path, CONF_DUPLCATE_NUM_KEY);
			if(duplicate_num != 0) masterInst->duplicate_num = duplicate_num;
			masterInst->tableInfoList = load_table_info_list(conf_path);
			masterInst->regionInfoList = load_region_info_list(conf_path);
		}
		update_master_info(masterInst);
}

void* master_daemon_thread(void* nul){
		while(1){
			sleep(DEFAULT_MASTER_DAEMON_SLEEP_INTERVAL);
			logg(INFO, "The interval is up, checking region status now.");
			update_master_info(masterInst);
			logg(INFO, "Finish the checking.");
		}
}

/**
 *  In this method, result is be part of RPCResponse, will be free by later RPC procedure
 **/
public RPCResponse* handler_master_request(char *cmd, List* params){
		RPCResponse *rpcResponse = NULL;
		if(match(GET_TABLE_INFO_MASTER_CMD, cmd)){
			char* table_name = get_param(params, 0);
			TableInfo* tableInfo = get_table_info(masterInst->tableInfoList, table_name);
			if(tableInfo!=NULL){
				char* result = table_info_to_string(tableInfo);
				rpcResponse = create_rpc_response(SUCCESS, strlen(result), result);
			}else{
				rpcResponse = create_rpc_response(SUCCESS, 0, NULL);
			}
		}else if(match(ADD_NEW_REGION_MASTER_CMD, cmd)){
			char* region_conn = get_param(params, 0);
			boolean bool = add_new_region_master(region_conn);
			rpcResponse = create_rpc_response(SUCCESS, strlen(bool_to_str(bool)), m_cpy(bool_to_str(bool)));
		}else if(match(CREATE_NEW_TABLE_MASTER_CMD, cmd)){
			char* table_name = get_param(params, 0);
			boolean bool = create_new_table_master(table_name);
			rpcResponse = create_rpc_response(SUCCESS, strlen(bool_to_str(bool)), m_cpy(bool_to_str(bool)));
		}else if(match(CHECK_PROBLEM_REGION_MASTER_CMD, cmd)){
			char* problem_region_conn = get_param(params, 0);
			boolean bool = check_problem_region_master(problem_region_conn);
			rpcResponse = create_rpc_response(SUCCESS, strlen(bool_to_str(bool)), m_cpy(bool_to_str(bool)));
		}else if(match(GET_ROLE_CMD, cmd)){
			rpcResponse = create_rpc_response(SUCCESS, strlen(MASTER_KEY), m_cpy(MASTER_KEY));
		}
		return rpcResponse;
}

public void start_server_master(){
		//Step 1. Start Master Daemon
		pthread_t fresh_thread_id;
		pthread_create(&fresh_thread_id, NULL, master_daemon_thread, (void*)NULL);
		logg(INFO, "The daemon for master side flushing has started.");
		logg(INFO, "The master server is starting and will handle request.");
		//Step 2. Startup Master Server
		startup(masterInst->port, handler_master_request);
}

/**
 * sample cmd: ./startMaster -conf conf/master.conf
 */
int main(int argc, char *argv[]){
		setup_logging(INFO, MASTER_LOG_FILE);
		char *conf_path = get_conf_path_from_argv(argc, argv, DEFAULT_MASTER_CONF_PATH);
		load_master(conf_path);
		start_server_master();
		return 1;
}