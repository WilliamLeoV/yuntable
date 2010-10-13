#include "global.h"
#include "utils.h"
#include "list.h"
#include "item.h"
#include "msg.h"
#include "master.h"
#include "region.h"
#include "conf.h"
#include "rpc.h"

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
	regionInfo->avail_space = connect_conn_int(regionInfo->conn, AVAILABLE_SPACE_REGION_CMD, NULL);
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

private int get_tablet_used_size(RegionInfo* regionInfo, char* table_name, char* column_family){
	List* params = generate_charactor_params(2, table_name, column_family);
	int tablet_used_size = connect_conn_int(regionInfo->conn, AVAILABLE_SPACE_REGION_CMD, params);
	list_destory(params, just_free);
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

private TabletInfo* create_new_tablet(RegionInfo *regionInfo, char* table_name, char* column_family){
	List* params = generate_charactor_params(2, table_name, column_family);
	boolean result = connect_conn_boolean(regionInfo->conn, ADD_NEW_TABLET_REGION_CMD, params);
	if(result == false) return NULL;
	int begin_timestamp = time(0);
	TabletInfo* tabletInfo = create_tablet_info(regionInfo, begin_timestamp, 0);
	list_destory(params, just_free);
	return tabletInfo;
}

private ColumnFamilyInfo* create_new_column_family(char* table_name, char* column_family){
	ColumnFamilyInfo* columnFamilyInfo = create_column_family_info(column_family);
	//create replica queue base on the duplicate num
	//TODO may need some transaction
	int i=0;
	for(i=0; i<masterInst->duplicate_num; i++){
		ReplicaQueue* replicaQueue = create_replica_queue(i);
		//will get next region, since the pre region has been used in one replicaQueue
		RegionInfo* regionInfo = list_get(masterInst->regionInfoList, i);
		if(regionInfo==NULL || regionInfo->avail_space < MIN_REGION_AVAILABLE_SIZE){
			return NULL;
		}
		TabletInfo* tabletInfo = create_new_tablet(regionInfo, table_name, column_family);
		if(tabletInfo == NULL) return NULL;
		list_append(replicaQueue->tabletInfoList, tabletInfo);
		list_append(columnFamilyInfo->replicaQueueList, replicaQueue);
	}
	return columnFamilyInfo;
}

public boolean create_new_table_master(char* table_name){
	if(get_table_info(masterInst->tableInfoList, table_name) != NULL) return false;
	TableInfo* tableInfo = create_table_info(table_name);
	ColumnFamilyInfo* columnFamilyInfo = create_new_column_family(table_name, DEFAULT_COLUMN_FAMILY);
	if(columnFamilyInfo == NULL){
		destory_table_info(tableInfo);
		return false;
	}
	list_append(tableInfo->columnFamilyInfoList, columnFamilyInfo);
	list_append(masterInst->tableInfoList, tableInfo);
	flush_table_info_list(masterInst->conf_path, masterInst->tableInfoList);
	return true;
}

public boolean add_new_column_family_master(char* table_name, char* column_family){
	TableInfo* tableInfo = get_table_info(masterInst->tableInfoList, table_name);
	if(tableInfo == NULL) return false;
	ColumnFamilyInfo* columnFamilyInfo = create_new_column_family(table_name, column_family);
	if(columnFamilyInfo == NULL) return false;
	list_append(tableInfo->columnFamilyInfoList, columnFamilyInfo);
	flush_table_info_list(masterInst->conf_path, masterInst->tableInfoList);
	return true;
}

private boolean region_has_problem(RegionInfo* regionInfo){
	if(regionInfo->connecting == false) return true;
	if(regionInfo->serving ==true && regionInfo->avail_space < MIN_REGION_AVAILABLE_SIZE) return true;
	return false;
}

private RegionInfo* get_region_and_update(List* regionInfoList, int index){
	//TODO may need to update region's avail size and sort
	return list_get(regionInfoList, index);
}

/* check if the replica queue is good or whether the probelsm region has been used in this queue  */
private boolean good_replica_queue(ReplicaQueue* replicaQueue, List* problemRegions){
	int i=0, size=list_size(replicaQueue->tabletInfoList);
	for(i=0; i<size; i++){
		TabletInfo* tabletInfo = list_get(replicaQueue->tabletInfoList, i);
		if(get_region_info(problemRegions, tabletInfo->regionInfo->conn)!=NULL) return false;
	}
	return true;
}

/* find the tablets from good replica queue that matchs infectedTablet's begin_timestamp and end_timestamp,  */
private List* find_src_tablets(ColumnFamilyInfo* columnFamilyInfo, TabletInfo* infectedTabletInfo, List* problemRegions){
	List* tabletList = list_create();
	int i=0, replica_queue_size=list_size(columnFamilyInfo->replicaQueueList);
	for(i=0; i<replica_queue_size; i++){
		ReplicaQueue* replicaQueue = list_get(columnFamilyInfo->replicaQueueList, i);
		if(!good_replica_queue(replicaQueue, problemRegions)) continue;
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
		char* column_family, int begin_timestamp, int end_timestamp){
	List* params = generate_charactor_params(3, target_conn, table_name, column_family);
	add_int_param(params, begin_timestamp);
	add_int_param(params, end_timestamp);
	boolean result = connect_conn_boolean(src_conn, START_SYNC_REGION_CMD, params);
	list_destory(params, just_free);
	return result;
}

private boolean handle_infected_tablet(TabletInfo* infectedTabletInfo, ReplicaQueue* replicaQueue, ColumnFamilyInfo* columnFamilyInfo,
					char* table_name, List* problemRegions, List* regionInfoList){
	RegionInfo* firstRegionInfo = get_region_and_update(regionInfoList, 0);
	if(region_has_problem(firstRegionInfo)) return false;
	TabletInfo* firstTabletInfo = create_new_tablet(firstRegionInfo, table_name, columnFamilyInfo->column_family);
	//if region info is connecting, which means it is full, will just assign new tablet to serving
	if(infectedTabletInfo->regionInfo->connecting == true){
		infectedTabletInfo->end_timestamp = time(0);
		//needs to new region that worksing
		infectedTabletInfo->regionInfo->serving = false;
		list_append(replicaQueue->tabletInfoList, firstTabletInfo);
	}else{
		//found the src region conn from the, this is for stroing old data
		List* src_tablets = find_src_tablets(columnFamilyInfo, infectedTabletInfo, problemRegions);
		list_replace(replicaQueue->tabletInfoList, infectedTabletInfo, firstTabletInfo);
		//if the infectedTabletInfo is serving now, will add one more
		if(infectedTabletInfo->end_timestamp == 0){
			infectedTabletInfo->end_timestamp = time(0);
			RegionInfo* secondRegionInfo = get_region_and_update(regionInfoList, 1);
			if(region_has_problem(secondRegionInfo)) return false;
			TabletInfo* secondTabletInfo = create_new_tablet(firstRegionInfo, table_name, columnFamilyInfo->column_family);
			list_append(replicaQueue->tabletInfoList, secondTabletInfo);
		}
		firstTabletInfo->begin_timestamp = infectedTabletInfo->begin_timestamp ;
		firstTabletInfo->end_timestamp = infectedTabletInfo->end_timestamp;
		//start sync
		int i=0, stt_size=list_size(src_tablets);
		for(i=0; i<stt_size; i++){
			TabletInfo* srcTabletInfo = list_get(src_tablets, i);
			//this node is for serving
			int tablet_used_size = get_tablet_used_size(srcTabletInfo->regionInfo,
					table_name, columnFamilyInfo->column_family);
			if(firstRegionInfo->avail_space < tablet_used_size) return false;
			list_append(replicaQueue->tabletInfoList, firstTabletInfo);
			//send the data from src tablet to the target tablet
			start_sync_job(srcTabletInfo->regionInfo->conn, firstRegionInfo->conn, table_name, columnFamilyInfo->
					column_family, infectedTabletInfo->begin_timestamp, infectedTabletInfo->end_timestamp);
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
			int j=0, column_family_size=list_size(tableInfo->columnFamilyInfoList);
			for(j=0; j<column_family_size; j++){
				ColumnFamilyInfo* columnFamilyInfo = list_get(tableInfo->columnFamilyInfoList, j);
				int k=0, replica_queue_size=list_size(columnFamilyInfo->replicaQueueList);
				for(k=0; k<replica_queue_size; k++){
					ReplicaQueue* replicaQueue = list_get(columnFamilyInfo->replicaQueueList, k);
					TabletInfo* infectedTabletInfo = get_tablet_info(replicaQueue->tabletInfoList, regionInfo);
					if(infectedTabletInfo!=NULL) handle_infected_tablet(infectedTabletInfo, replicaQueue,
							columnFamilyInfo, tableInfo->table_name, problemRegions, master->regionInfoList);
				}
			}
		}
	}

}

public boolean refresh_region_status_now_master(char* problem_region_conn){
	RegionInfo* regionInfo = get_region_info(masterInst->regionInfoList, problem_region_conn);
	update_region_info(regionInfo);
	sort_region_info_list(masterInst->regionInfoList);
	if(region_has_problem(regionInfo)){
		List* problem_regions = list_create();
		list_append(problem_regions, regionInfo);
		handle_problem_regions(problem_regions, masterInst);
	}
	flush_table_info_list(masterInst->conf_path, masterInst->tableInfoList);
	return true;
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
	Master* master = malloc(sizeof(Master));
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
		logg("The interval is up, checking region status now.\n",NULL);
		update_master_info(masterInst);
		logg("Finish the checking.\n",NULL);
	}
}

public Buf* handler_master_request(char *cmd, List* params){
	Buf *ret = NULL;
	if(match(GET_TABLE_INFO_MASTER_CMD, cmd)){
		char* table_name = get_param(params, 0);
		TableInfo* tableInfo = get_table_info(masterInst->tableInfoList, table_name);
		char* result = NULL;
		if(tableInfo!=NULL) result = table_info_to_string(tableInfo);
		else result = m_cpy(NULL_STRING);
		ret = create_buf(strlen(result), result);
	}else if(match(ADD_NEW_REGION_MASTER_CMD, cmd)){
		char* region_conn = get_param(params, 0);
		boolean bool = add_new_region_master(region_conn);
		ret = create_buf(strlen(bool_to_str(bool)), m_cpy(bool_to_str(bool)));
	}else if(match(CREATE_NEW_TABLE_MASTER_CMD, cmd)){
		char* table_name = get_param(params, 0);
		boolean bool = create_new_table_master(table_name);
		ret = create_buf(strlen(bool_to_str(bool)), m_cpy(bool_to_str(bool)));
	}else if(match(ADD_NEW_COLUMN_FAMILY_MASTER_CMD, cmd)){
		char* table_name = get_param(params, 0);
		char* column_family = get_param(params, 1);
		boolean bool = add_new_column_family_master(table_name, column_family);
		ret = create_buf(strlen(bool_to_str(bool)), m_cpy(bool_to_str(bool)));
	}else if(match(REFRESH_TABLE_INFO_NOW_MASTER_CMD, cmd)){
		char* problem_region_conn = get_param(params, 0);
		boolean bool = refresh_region_status_now_master(problem_region_conn);
		ret = create_buf(strlen(bool_to_str(bool)), m_cpy(bool_to_str(bool)));
	}else if(match(GET_ROLE_CMD, cmd)){
		ret = create_buf(strlen(MASTER_KEY), m_cpy(MASTER_KEY));
	}
	return ret;
}

public void start_server_master(){
	pthread_t fresh_thread_id;
	pthread_create(&fresh_thread_id, NULL, master_daemon_thread, (void*)NULL);
	logg("flush master daemon thread has started\n", NULL);
	logg("The master server is starting and will handle request.\n", NULL);
	startup(masterInst->port, handler_master_request);
}

/**
 * sample cmd: ./startMaster -conf conf/master.conf
 */
int main(int argc, char *argv[]){
	char *conf_path = get_conf_path_from_argv(argc, argv, DEFAULT_MASTER_CONF_PATH);
	load_master(conf_path);
	start_server_master();
	return 1;
}


