#include "global.h"
#include "utils.h"
#include "list.h"
#include "yfile.h"
#include "memstore.h"
#include "wal.h"
#include "tablet.h"

/**
 * The sample files that insides the tablet folder
 * tablet0/
 *  table_name1.table //a touch file for metadata purpose
 *  default_cf.columnfamily
 *  default_cf_0.yfile
 *  default_cf_1.yfile
 **/

//one tablet per column family at one region server, but one column family may cross a lot of region servers
struct _Tablet{
	short id; /** will use for wal **/
	char* folder; /** will use the tablet name as the folder name **/
	char* table_name;
	char* column_family;
	Memstore *memstore;
	List *yfileList;
	int used_size; /**the size will preallocted the max size for wal log, such as 64MB**/
	long long last_flushed_id; /** the it of last item has been flushed to yfile **/
	long long max_item_id; /** current max item id in the tablet and memstore **/
	int begin_timestamp;
	int end_timestamp;
	pthread_mutex_t flushing_mutex;
};

#define YFILE_EXT ".yfile"
#define TABLE_EXT ".table"
#define COLUMN_FAMILY_EXT ".columnfamily"
#define MB		(1024 * 1024)

public short get_tablet_id(Tablet* tablet){
	return tablet->id;
}

public void set_last_flushed_id(Tablet* tablet, long last_flushed_id){
	tablet->last_flushed_id = last_flushed_id;
}

public long get_last_flushed_id(Tablet* tablet){
	return tablet->last_flushed_id;
}

public char* get_tablet_folder(Tablet* tablet){
	return tablet->folder;
}

private Tablet* init_tablet_struct(char *tablet_folder){
	Tablet *tablet = malloc(sizeof(Tablet));
	tablet->folder = m_cpy(tablet_folder);
	//init memstore and timestamp
	tablet->memstore = init_memstore();
	tablet->yfileList = list_create();
	tablet->used_size = 0;
	tablet->last_flushed_id = 0;
	tablet->max_item_id = 0;
	tablet->begin_timestamp = 0;
	tablet->end_timestamp = 0;
	pthread_mutex_init(&tablet->flushing_mutex, NULL);
	return tablet;
}

public Tablet* load_tablet(char *tablet_folder){
	Tablet *tablet = init_tablet_struct(tablet_folder);
	tablet->table_name = m_get_file_name_by_ext(tablet->folder, TABLE_EXT);
	tablet->column_family = m_get_file_name_by_ext(tablet->folder, COLUMN_FAMILY_EXT);
	List* yfiles = get_files_path_by_ext(tablet->folder, YFILE_EXT);
	char* yfile_path = NULL;
	while((yfile_path = list_next(yfiles))!= NULL){
		YFile *yfile = loading_yfile(yfile_path);
		list_append(tablet->yfileList, yfile);
		tablet->used_size += get_file_size(yfile_path) / MB;
	}
	short tablet_id = atoi(move_pointer(tablet_folder, sizeof(TABLET_FOLDER_PREFIX)-1));
	tablet->id = tablet_id;
	list_destory(yfiles, just_free);
	return tablet;
}

public Tablet* create_tablet(int tablet_id, char *tablet_folder, char *table_name, char* column_family){
	Tablet *tablet = init_tablet_struct(tablet_folder);
	tablet->id = tablet_id;
	tablet->table_name = m_cpy(table_name);
	tablet->column_family = m_cpy(column_family);
	mkdir(tablet_folder, S_IRWXU);
	char* table_name_file_path = m_cats(4, tablet_folder, FOLDER_SEPARATOR_STRING, tablet->table_name, TABLE_EXT);
	create_or_rewrite_file(table_name_file_path, "");
	char* column_family_file_path = m_cats(4, tablet_folder, FOLDER_SEPARATOR_STRING, tablet->column_family, COLUMN_FAMILY_EXT);
	create_or_rewrite_file(column_family_file_path, "");
	return tablet;
}

public boolean match_tablet(Tablet *tablet, char* table_name, char* column_family){
	if(match(tablet->table_name, table_name) &&
			match(tablet->column_family, column_family)) return true;
	else return false;
}

public boolean match_tablet_by_table_name(Tablet *tablet, char* table_name){
	if(match(tablet->table_name, table_name)) return true;
	else return false;
}

public char* get_column_family_by_tablet(Tablet *tablet){
	return tablet->column_family;
}

public void put_tablet(Tablet *tablet, long incr_item_id, Item *item){
	pthread_mutex_lock(&tablet->flushing_mutex);

	append_memstore(tablet->memstore, item);

	pthread_mutex_unlock(&tablet->flushing_mutex);

	tablet->max_item_id = incr_item_id;
}

public boolean need_to_flush_tablet(Tablet *tablet){
	return memstore_full(tablet->memstore);
}

private ResultSet* query_yfiles_by_row_key(List* yfileList, char *row_key){
	ResultSet* resultSet = m_create_result_set(0, NULL);
	int i=0, size=list_size(yfileList);
	for(i=0; i<size; i++){
		YFile *yfile = list_get(yfileList, i);
		ResultSet* set1 = query_yfile_by_row_key(yfile, row_key);
		ResultSet* set2 = m_combine_result_set(resultSet, set1);
		free_result_set(set1);
		free_result_set(resultSet);
		resultSet = set2;
	}
	return resultSet;
}

private ResultSet* query_yfiles_by_timestamp(List* yfileList, int begin_timestamp, int end_timestamp){
	ResultSet* resultSet = m_create_result_set(0, NULL);
	int i=0, size=list_size(yfileList);
	for(i=0; i<size; i++){
		YFile *yfile = list_get(yfileList, i);
		ResultSet* set1 = query_yfile_by_timestamp(yfile, begin_timestamp, end_timestamp);
		ResultSet* set2 = m_combine_result_set(resultSet, set1);
		free_result_set(set1);
		free_result_set(resultSet);
		resultSet = set2;
	}
	return resultSet;
}

public ResultSet* query_tablet_row_key(Tablet *tablet, char* row_key){
	pthread_mutex_lock(&tablet->flushing_mutex);

	ResultSet* memstoreSet = query_memstore_by_row_key(tablet->memstore, row_key);
	ResultSet* yfileSet = query_yfiles_by_row_key(tablet->yfileList, row_key);

	pthread_mutex_unlock(&tablet->flushing_mutex);

	ResultSet* combinedSet = m_combine_result_set(memstoreSet, yfileSet);

	free(memstoreSet);
	free(yfileSet);
	return combinedSet;
}

public  ResultSet* query_tablet_by_timestamp(Tablet *tablet, int begin_timestamp, int end_timestamp){
	//query memstore,
	ResultSet* memstoreSet = query_memstore_by_timestamp(tablet->memstore, begin_timestamp, end_timestamp);
	//query yfile
	ResultSet* yfileSet = query_yfiles_by_timestamp(tablet->yfileList, begin_timestamp, end_timestamp);

	ResultSet* combinedSet = m_combine_result_set(memstoreSet, yfileSet);

	free(memstoreSet);
	free(yfileSet);
	return combinedSet;
}

private char* get_next_yfile_path(Tablet *tablet){
	int count = list_size(tablet->yfileList);
	char* count_string = m_itos(count);
	char* next_yfile_path = m_cats(5, tablet->folder, FOLDER_SEPARATOR_STRING,
			tablet->column_family, count_string, YFILE_EXT);
	free(count_string);
	return next_yfile_path;
}

public void flush_tablet(Tablet *tablet){
	logg("The memstore for %s is flushing to yfile now\n", tablet->column_family);
	sort_memstore(tablet->memstore);
	ResultSet* resultSet = get_all_items_memstore(tablet->memstore);
	char *next_yfile_path = get_next_yfile_path(tablet);
	YFile *yfile = create_new_yfile(next_yfile_path, resultSet, tablet->table_name, tablet->column_family);

	pthread_mutex_lock(&tablet->flushing_mutex);

	list_append(tablet->yfileList, yfile);
	tablet->memstore = reset_memstore(tablet->memstore, resultSet->size);

	pthread_mutex_unlock(&tablet->flushing_mutex);

	tablet->last_flushed_id = tablet->max_item_id;
}

private int get_disk_usage(Tablet *tablet){
	int disk_usage = 1;
	DIR *tabletFolder = opendir(tablet->folder);
	struct dirent *dp;
	while ((dp = readdir(tabletFolder)) != NULL) {
		//if d_type is 4, means it is a dir
		if(dp->d_type != 4){
			char *file_path = m_cats(3, tabletFolder, FOLDER_SEPARATOR_STRING, dp->d_name);
			disk_usage += get_file_size(file_path)/ MB;
			//printf("the file %s's disk_usage:%d\n",dp->d_name, disk_usage);
			free(file_path);
		}
	}
	closedir(tabletFolder);
	return disk_usage;
}

public int get_used_size_tablet(Tablet *tablet){
	tablet->used_size = get_disk_usage(tablet);
	return tablet->used_size;
}
