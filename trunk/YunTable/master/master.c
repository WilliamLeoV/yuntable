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

#define DEFAULT_DUPLICATE_NUM 1
#define MIN_REGION_AVAILABLE_SIZE 100 //Unit is MB
#define DEFAULT_MASTER_FLUSH_CHECK_INTERVAL 600 //10Min

typedef struct _Master{
	char* conf_path;
	int port;
	int duplicate_num;
	List* regionInfoList;
	List* tableInfoList;
	int flush_check_interval;
}Master;

/**global singleton **/
Master *masterInst = NULL;

/* Currently only update the available size of region info */
private void update_region_info(RegionInfo* regionInfo){
		regionInfo->connecting = true;
		RPCRequest* rpcRequest = create_rpc_request(AVAILABLE_SPACE_REGION_CMD, NULL);
		RPCResponse* rpcResponse = connect_conn(regionInfo->conn, rpcRequest);
		int status_code = get_status_code(rpcResponse);
	    if(status_code == SUCCESS){
	    	regionInfo->avail_space = atoi(get_result(rpcResponse));
	    }else{
	    	logg(ISSUE, "The target region %s have some problem:%s.", regionInfo->conn,
	    			get_error_message(status_code));
	    	regionInfo->connecting = false;
	    	//TODO need to handle region fail situation?
	    }
	    destory_rpc_request(rpcRequest);
	    destory_rpc_response(rpcResponse);
}

/** Currently only used at qsort **/
private int cmp_region_info_void(void const* regionInfo1_void, void const* regionInfo2_void){
	   //first load 4byte of address, then converted to item pointer, and only tested at 32bit
		RegionInfo const* regionInfo1 = (RegionInfo*)(*(long*)regionInfo1_void);
		RegionInfo const* regionInfo2 = (RegionInfo*)(*(long*)regionInfo2_void);
		if(regionInfo1->connecting == regionInfo2->connecting){
			return regionInfo1->avail_space > regionInfo2->avail_space;
		}else{
			return regionInfo1->connecting > regionInfo2->connecting;
		}
}

/** If the method can not get correct tablet used size, the system will return INDEFINITE(-1) **/
private int get_tablet_used_size(RegionInfo* regionInfo, char* table_name){
		int tablet_used_size = INDEFINITE;
		List* params = generate_charactor_params(1, table_name);
		RPCRequest* rpcRequest = create_rpc_request(TABLET_USED_SIZE_REGION_CMD, params);
		RPCResponse* rpcResponse = connect_conn(regionInfo->conn, rpcRequest);
	    if(get_status_code(rpcResponse) == SUCCESS){
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
		qsort(regionInfos, size, sizeof(RegionInfo*),  cmp_region_info_void);
		list_destory(regionInfoList, only_free_struct);
		regionInfoList = array_to_list((void**)regionInfos, size);
		return regionInfoList;
}

/* This cmd for registering a new region node */
public boolean add_new_region_master(char* region_conn){
		logg(INFO, "Adding a new Region %s.", region_conn);
		//check if the region has existed
		RegionInfo* regionInfo = get_region_info(masterInst->regionInfoList, region_conn);
		if(regionInfo != NULL){
			logg(ISSUE, "The Region %s has existed at the master!", region_conn);
			return false;
		}
		if(!check_node_validity(region_conn, REGION_KEY)){
			logg(ISSUE, "The Region %s can not be connected.", region_conn);
			return false;
		}
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
		int status_code = get_status_code(rpcResponse);
		if(status_code == SUCCESS && stob(get_result(rpcResponse)) == true){
			long long begin_timestamp = get_current_time_stamp();
			tabletInfo = create_tablet_info(regionInfo, begin_timestamp, 0);
		}else{
			logg(ISSUE, "The tablet for table %s can not be created at region %s.", table_name, regionInfo->conn);
			if(status_code != SUCCESS){
				logg(ISSUE, "The Potential cause: %s.", get_error_message(status_code));
			}
			//TODO need to use the async mode
			check_problem_region_master(regionInfo->conn);
		}
	    destory_rpc_request(rpcRequest);
		destory_rpc_response(rpcResponse);
		return tabletInfo;
}

/** If the method return true, means the region has some problem **/
private boolean region_has_problem(RegionInfo* regionInfo){
		if(regionInfo->connecting == false){
			return true;
		}else if(regionInfo->serving ==true && regionInfo->avail_space < MIN_REGION_AVAILABLE_SIZE){
			return true;
		}else{
			return false;
		}
}

public boolean create_new_table_master(char* table_name){
		logg(INFO, "Create a new Table %s.", table_name);
		if(get_table_info(masterInst->tableInfoList, table_name) != NULL){
			logg(ISSUE, "The Table %s has been created before.", table_name);
			return false;
		}
		//Step 1. Create New Table Info Struct
		TableInfo* tableInfo = create_table_info(table_name);
		//Step 2. Create Replica Queue
		int i=0;
		for(i=0; i<masterInst->duplicate_num; i++){
			logg(INFO, "Creating the replica queue %d for table %s.", i, table_name);
			ReplicaQueue* replicaQueue = create_replica_queue(i);
			list_append(tableInfo->replicaQueueList, replicaQueue);
			RegionInfo* regionInfo = list_get(masterInst->regionInfoList, i);
			if(regionInfo == NULL || region_has_problem(regionInfo) == true){
				logg(ISSUE, "Can not find good region node for table %s.", table_name);
				return false;
			}
			TabletInfo* tabletInfo = create_new_tablet(regionInfo, table_name);
			if(tabletInfo == NULL){
				logg(ISSUE, "Can not create a tablet at region %s for table %s.", regionInfo->conn, table_name);
				return false;
			}
			list_append(replicaQueue->tabletInfoList, tabletInfo);
		}
		//If everything goes smoothly, the table will be added to master inst, and will be flushed
		list_append(masterInst->tableInfoList, tableInfo);
		flush_table_info_list(masterInst->conf_path, masterInst->tableInfoList);
		return true;
}

/* check if the replica queue is good or whether the problem region has been used in this queue  */
private boolean is_good_replica_queue(ReplicaQueue* replicaQueue, List* problemRegions){
		int i=0, size=list_size(replicaQueue->tabletInfoList);
		for(i=0; i<size; i++){
			TabletInfo* tabletInfo = list_get(replicaQueue->tabletInfoList, i);
			if(get_region_info(problemRegions, tabletInfo->regionInfo->conn)!=NULL){
				return false;
			}
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
		long long begin_timestamp, long long end_timestamp){
		logg(INFO, "Starting the Sync job about table %s from %s to %s begin:%lld end:%lld.",
				table_name, src_conn, target_conn, begin_timestamp, end_timestamp);
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
		RegionInfo* firstRegionInfo = list_get(regionInfoList, 0);
		update_region_info(firstRegionInfo);
		if(region_has_problem(firstRegionInfo)) return false;
		TabletInfo* firstTabletInfo = create_new_tablet(firstRegionInfo, tableInfo->table_name);
		//if region info is connecting, which means it is full, will just assign new tablet to serving
		if(infectedTabletInfo->regionInfo->connecting == true){
			infectedTabletInfo->end_timestamp = get_current_time_stamp();
			//needs to new region that worksing
			infectedTabletInfo->regionInfo->serving = false;
			list_append(replicaQueue->tabletInfoList, firstTabletInfo);
		}else{
			//found the src region conn from the, this is for stroing old data
			List* src_tablets = find_src_tablets(tableInfo, infectedTabletInfo, problemRegions);
			list_replace(replicaQueue->tabletInfoList, infectedTabletInfo, firstTabletInfo);
			//if the infectedTabletInfo is serving now, will add one more
			if(infectedTabletInfo->end_timestamp == 0){
				infectedTabletInfo->end_timestamp = get_current_time_stamp();
				RegionInfo* secondRegionInfo = list_get(regionInfoList, 1);
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
		logg(ISSUE, "This routine has not been tested, and stops now.");
		return;
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

/** This method will update all regions' info **/
private void update_master_info(Master* master){
		List* problem_regions = list_create();
		//Update Region Info
		int i=0;
        int size=list_size(master->regionInfoList);
        if (size == 0) {
            return;
        }
		for(i=0; i<size; i++){
			RegionInfo* regionInfo = list_get(master->regionInfoList, i);
			update_region_info(regionInfo);
			if(region_has_problem(regionInfo)) {
                list_append(problem_regions, regionInfo);
            }
		}
		//sort region info list
        if (size > 1) {
            sort_region_info_list(master->regionInfoList);
        }
		//handle founded problem regions
		if(list_size(problem_regions)>0){
			handle_problem_regions(problem_regions, masterInst);
		}
        list_destory(problem_regions, just_free);
		//flushing the table information
		flush_table_info_list(master->conf_path, master->tableInfoList);
}

/** The metadata of master server, include all table and region info **/
public char* get_metadata_master(){
		Buf* buf = init_buf();
		char* line1 = mallocs(LINE_BUF_SIZE);
		sprintf(line1, "The Master Default Duplication Number: %d\n", masterInst->duplicate_num);
		buf_cat(buf, line1, strlen(line1));
		int i=0, region_size = list_size(masterInst->regionInfoList);
		char* line2 = mallocs(LINE_BUF_SIZE);
		sprintf(line2, "The number of region node: %d.\n", region_size);
		buf_cat(buf, line2, strlen(line2));

		//Iterating all region node info
		for(i=0; i<region_size; i++){
			RegionInfo* regionInfo = list_get(masterInst->regionInfoList, i);
			char* region_line1 = mallocs(LINE_BUF_SIZE);
			sprintf(region_line1, "The information about Region No'%d.\n", i);
			buf_cat(buf, region_line1, strlen(region_line1));
			char* region_line2 = mallocs(LINE_BUF_SIZE);
			sprintf(region_line2, "	The Connection Info: %s.\n", regionInfo->conn);
			buf_cat(buf, region_line2, strlen(region_line2));
			char* region_line3 = mallocs(LINE_BUF_SIZE);
			sprintf(region_line3, "	The Available Size(MB): %d.\n", regionInfo->avail_space);
			buf_cat(buf, region_line3, strlen(region_line3));
			char* region_line4 = mallocs(LINE_BUF_SIZE);
			sprintf(region_line4, "	Is Connecting Now: %s.\n", bool_to_str(regionInfo->connecting));
			buf_cat(buf, region_line4, strlen(region_line4));
			char* region_line5 = mallocs(LINE_BUF_SIZE);
			sprintf(region_line5, "	Is Serving Now: %s.\n", bool_to_str(regionInfo->serving));
			buf_cat(buf, region_line5, strlen(region_line5));
			frees(5, region_line1, region_line2, region_line3, region_line4, region_line5);
		}

		int j=0, table_size = list_size(masterInst->tableInfoList);
		char* line3 = mallocs(LINE_BUF_SIZE);
		sprintf(line3, "The number of Tables: %d\n", table_size);
		buf_cat(buf, line3, strlen(line3));
		char* table_line = "Tables are:";
		buf_cat(buf, table_line, strlen(table_line));
		//Iterating all table info
		for(j=0; j<table_size; j++){
			TableInfo* tableInfo = list_get(masterInst->tableInfoList, j);
			char* table_name_line = mallocs(LINE_BUF_SIZE);
			sprintf(table_name_line, " %s.", tableInfo->table_name);
			buf_cat(buf, table_name_line, strlen(table_name_line));
			free2(table_name_line);
		}
		buf_cat(buf, LINE_SEPARATOR_STRING, strlen(LINE_SEPARATOR_STRING));

		frees(3, line1, line2, line3);
		char* metadata = m_get_buf_string(buf);
		destory_buf(buf);
		return metadata;
}

/** Make sure the conf path is valid **/
public void load_master(char *conf_path){
		logg(INFO, "Loading the master info from conf path is %s.", conf_path);
		masterInst = malloc2(sizeof(Master));
		masterInst->conf_path = strdup(conf_path);
		int port = get_int_value_by_key(conf_path, CONF_PORT_KEY);
		if(port != INDEFINITE){
			masterInst->port = port;
		}else{
			masterInst->port = DEFAULT_MASTER_PORT;
		}
		int	duplicate_num = get_int_value_by_key(conf_path, CONF_DUPLCATE_NUM_KEY);
		if(duplicate_num != INDEFINITE){
			masterInst->duplicate_num = duplicate_num;
		}else{
			masterInst->duplicate_num = DEFAULT_DUPLICATE_NUM;
		}
		int flush_check_interval = get_int_value_by_key(conf_path, CONF_FLUSH_CHECK_INTERVAL_KEY);
		if(flush_check_interval != INDEFINITE){
			masterInst->flush_check_interval = flush_check_interval;
		}else{
			masterInst->flush_check_interval = DEFAULT_MASTER_FLUSH_CHECK_INTERVAL;
		}
		masterInst->tableInfoList = load_table_info_list(conf_path);
		masterInst->regionInfoList = load_region_info_list(conf_path);
		update_master_info(masterInst);
}

void* master_daemon_thread(void* nul){
		while(1){
			sleep(masterInst->flush_check_interval);
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
			if(table_name == NULL){
				rpcResponse = create_rpc_response(ERROR_NO_PARAM, 0, NULL);
			}else{
				TableInfo* tableInfo = get_table_info(masterInst->tableInfoList, table_name);
				if(tableInfo!=NULL){
					char* result = table_info_to_string(tableInfo);
					rpcResponse = create_rpc_response(SUCCESS, strlen(result), result);
				}else{
					rpcResponse = create_rpc_response(SUCCESS, 0, NULL);
				}
			}
		}else if(match(ADD_NEW_REGION_MASTER_CMD, cmd)){
			char* region_conn = get_param(params, 0);
			if(region_conn == NULL){
				rpcResponse = create_rpc_response(ERROR_NO_PARAM, 0, NULL);
			}else{
				boolean bool = add_new_region_master(region_conn);
				rpcResponse = create_rpc_response(SUCCESS, strlen(bool_to_str(bool)), strdup(bool_to_str(bool)));
			}
		}else if(match(CREATE_NEW_TABLE_MASTER_CMD, cmd)){
			char* table_name = get_param(params, 0);
			if(table_name == NULL){
				rpcResponse = create_rpc_response(ERROR_NO_PARAM, 0, NULL);
			}else{
				boolean bool = create_new_table_master(table_name);
				rpcResponse = create_rpc_response(SUCCESS, strlen(bool_to_str(bool)), strdup(bool_to_str(bool)));
			}
		}else if(match(CHECK_PROBLEM_REGION_MASTER_CMD, cmd)){
			char* problem_region_conn = get_param(params, 0);
			if(problem_region_conn == NULL){
				rpcResponse = create_rpc_response(ERROR_NO_PARAM, 0, NULL);
			}else{
				boolean bool = check_problem_region_master(problem_region_conn);
				rpcResponse = create_rpc_response(SUCCESS, strlen(bool_to_str(bool)), strdup(bool_to_str(bool)));
			}
		}else if(match(GET_METADATA_MASTER_CMD, cmd)){
			char* metadata = get_metadata_master();
			rpcResponse = create_rpc_response(SUCCESS, strlen(metadata), strdup(metadata));
		}else if(match(GET_ROLE_CMD, cmd)){
			rpcResponse = create_rpc_response(SUCCESS, strlen(MASTER_KEY), strdup(MASTER_KEY));
		}else{
			rpcResponse = create_rpc_response(ERROR_WRONG_CMD, 0, NULL);
		}
		return rpcResponse;
}

public void start_server_master(){
		//Step 1. Start Master Daemon
		pthread_t fresh_thread_id;
		pthread_create(&fresh_thread_id, NULL, master_daemon_thread, (void*)NULL);
		logg(INFO, "The daemon for master side flushing has started.");
		logg(INFO, "The Master server is starting at %d and will handle request.", masterInst->port);
		logg(INFO, "The Master Conf File is at %s.", masterInst->conf_path);
		logg(INFO, "The Master Duplication Number is %d.", masterInst->duplicate_num);
		logg(INFO, "The Master Flushing Checking Interval is %d.", masterInst->flush_check_interval);
		//Step 2. Startup Master Server
		startup(masterInst->port, handler_master_request);
}

/**
 * sample cmd: ./startMaster -conf conf/master.conf
 */
int main(int argc, char *argv[]){
		setup_logging(INFO, MASTER_LOG_FILE);
		logg(INFO,"------- Welcome to YunTable [%s] -------\n",VERSION);
		char *conf_path = get_conf_path_from_argv(argc, argv, DEFAULT_MASTER_CONF_PATH);
		load_master(conf_path);
		start_server_master();
		return 1;
}
