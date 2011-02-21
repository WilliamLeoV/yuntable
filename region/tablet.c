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
#include "yfile.h"
#include "memstore.h"
#include "wal.h"
#include "tablet.h"
#include "malloc2.h"
#include "log.h"

/**
 * The sample files that insides the tablet folder
 * tablet0/
 *		table_name.table
 * 		table_name1.yfile
 *  table_name2.yfile
 **/

//one tablet per column family at one region server, but one column family may cross a lot of region servers
struct _Tablet{
		short id; /** will use for wal **/
		char* folder; /** will use the tablet name as the folder name **/
		char* table_name;
		Memstore *memstore;
		List *yfileList;
		int used_size; /**the size will preallocted the max size for wal log, such as 64MB**/
		long long last_flushed_id; /** the it of last item has been flushed to yfile **/
		long long max_item_id; /** current max item id in the tablet and memstore **/
		pthread_mutex_t flushing_mutex;
};

#define TABLE_EXT ".table"
#define YFILE_EXT ".yfile"

private int extract_tablet_id(char *table_folder){
		char* temp = move_pointer(table_folder, sizeof(TABLET_FOLDER_PREFIX)-1);
		return atoi(temp);
}

public short get_tablet_id(Tablet* tablet){
		return tablet->id;
}

public void set_last_flushed_id(Tablet* tablet, long long last_flushed_id){
		tablet->last_flushed_id = last_flushed_id;
}

public long get_last_flushed_id(Tablet* tablet){
		return tablet->last_flushed_id;
}

public void set_max_item_id(Tablet* tablet, long long max_item_id){
		tablet->max_item_id = max_item_id;
}

public char* get_tablet_folder(Tablet* tablet){
		return tablet->folder;
}

private Tablet* init_tablet_struct(int tablet_id, char *tablet_folder){
		Tablet *tablet = malloc2(sizeof(Tablet));
		tablet->id = tablet_id;
		tablet->folder = m_cpy(tablet_folder);
		//init memstore and timestamp
		tablet->memstore = init_memstore(tablet_id);
		tablet->yfileList = list_create();
		tablet->used_size = 0;
		tablet->last_flushed_id = 0;
		tablet->max_item_id = 0;
		pthread_mutex_init(&tablet->flushing_mutex, NULL);
		return tablet;
}

public Tablet* load_tablet(char *tablet_folder){
		logg(INFO, "Loading the tablet at %s.", tablet_folder);
		int table_id = extract_tablet_id(tablet_folder);
		Tablet *tablet = init_tablet_struct(table_id, tablet_folder);
		tablet->table_name = m_get_file_name_by_ext(tablet->folder, TABLE_EXT);
		List* yfiles = get_files_path_by_ext(tablet->folder, YFILE_EXT);
		char* yfile_path = NULL;
		while((yfile_path = list_next(yfiles))!= NULL){
			YFile *yfile = loading_yfile(yfile_path);
			list_append(tablet->yfileList, yfile);
			tablet->used_size += get_file_size(yfile_path) / MB;
		}
		list_destory(yfiles, just_free);
		return tablet;
}

public Tablet* create_tablet(int tablet_id, char *tablet_folder, char *table_name){
		logg(INFO, "Creating a new tablet from table %s at %s.", table_name, tablet_folder);
		Tablet *tablet = init_tablet_struct(tablet_id, tablet_folder);
		tablet->table_name = m_cpy(table_name);
		mkdir(tablet_folder, S_IRWXU);
		char* table_name_file_path = m_cats(4, tablet_folder, FOLDER_SEPARATOR_STRING, tablet->table_name, TABLE_EXT);
		create_or_rewrite_file(table_name_file_path, "");
		return tablet;
}

public boolean match_tablet(Tablet *tablet, char* table_name){
		if(match(tablet->table_name, table_name)) return true;
		else return false;
}

public boolean match_tablet_by_table_name(Tablet *tablet, char* table_name){
		if(match(tablet->table_name, table_name)) return true;
		else return false;
}

public void put_tablet(Tablet *tablet, long incr_item_id, Item *item){
		pthread_mutex_lock(&tablet->flushing_mutex);

		append_memstore(tablet->memstore, item);

		pthread_mutex_unlock(&tablet->flushing_mutex);

		tablet->max_item_id = incr_item_id;
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

private ResultSet* query_yfiles_by_timestamp(List* yfileList, long long begin_timestamp, long long end_timestamp){
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
		//TODO justifies the use of mutex lock
		pthread_mutex_lock(&tablet->flushing_mutex);

		ResultSet* memstoreSet = query_memstore_by_row_key(tablet->memstore, row_key);
		ResultSet* yfileSet = query_yfiles_by_row_key(tablet->yfileList, row_key);

		pthread_mutex_unlock(&tablet->flushing_mutex);

		ResultSet* combinedSet = m_combine_result_set(memstoreSet, yfileSet);

		frees(2, memstoreSet, yfileSet);
		return combinedSet;
}

public  ResultSet* query_tablet_by_timestamp(Tablet *tablet, long long begin_timestamp, long long end_timestamp){
		//query memstore,
		ResultSet* memstoreSet = query_memstore_by_timestamp(tablet->memstore, begin_timestamp, end_timestamp);
		//query yfile
		ResultSet* yfileSet = query_yfiles_by_timestamp(tablet->yfileList, begin_timestamp, end_timestamp);

		ResultSet* combinedSet = m_combine_result_set(memstoreSet, yfileSet);

		frees(2, memstoreSet, yfileSet);
		return combinedSet;
}

public ResultSet* query_tablet_all(Tablet *tablet){
		 //get all items from memstore
		 ResultSet* memstoreSet = get_all_items_memstore(tablet->memstore);
		 //get all items from yfile
		 ResultSet* yfileSet = query_yfiles_by_timestamp(tablet->yfileList, 0, get_current_time_stamp());

		 ResultSet* combinedSet = m_combine_result_set(memstoreSet, yfileSet);

		 frees(2, memstoreSet, yfileSet);
		 return combinedSet;
}

public char* get_metadata_tablet(Tablet *tablet){
		Buf* buf = init_buf();
		char* memstore_meta = get_memstore_metadata(tablet->memstore);
		buf_cat(buf, memstore_meta, strlen(memstore_meta));
		free2(memstore_meta);
		int i=0, size=list_size(tablet->yfileList);
		for(i=0; i<size; i++){
			YFile *yfile = list_get(tablet->yfileList, i);
			char* yfile_meta = get_yfile_metadata(yfile);
			buf_cat(buf, yfile_meta, strlen(yfile_meta));
			buf_cat(buf, LINE_SEPARATOR_STRING, strlen(LINE_SEPARATOR_STRING));
			free(yfile_meta);
		}
		char* metadata = m_get_buf_string(buf);
		destory_buf(buf);
		return metadata;
}

private char* get_next_yfile_path(Tablet *tablet){
		int count = list_size(tablet->yfileList);
		char* count_string = m_itos(count);
		char* next_yfile_path = m_cats(5, tablet->folder, FOLDER_SEPARATOR_STRING,
				tablet->table_name, count_string, YFILE_EXT);
		free2(count_string);
		return next_yfile_path;
}

public void refresh_tablet(Tablet *tablet, int hotnessValue){
		logg(INFO, "Refreshing for tablet %d now.", tablet->id);
		//YFile Data Block Cache Refresh Part
		int i=0, size=list_size(tablet->yfileList);
		for(i=0; i<size; i++){
			YFile *yfile = list_get(tablet->yfileList, i);
			refresh_yfile_data_block_cache(yfile, hotnessValue);
		}

		//Memstore Flushing Part
		if(memstore_full(tablet->memstore)){
			logg(INFO, "The memstore for %s is flushing to yfile now.", tablet->table_name);
			ResultSet* resultSet = get_all_items_memstore(tablet->memstore);
			int flushed_size = resultSet->size;
			char *next_yfile_path = get_next_yfile_path(tablet);
			YFile *yfile = create_new_yfile(next_yfile_path, resultSet, tablet->table_name);

			pthread_mutex_lock(&tablet->flushing_mutex);

			list_append(tablet->yfileList, yfile);
			tablet->memstore = reset_memstore(tablet->memstore, flushed_size);

			pthread_mutex_unlock(&tablet->flushing_mutex);

			tablet->last_flushed_id = tablet->max_item_id;
		}
}

private int get_disk_usage(Tablet *tablet){
		int disk_usage = 1;
		DIR *tabletFolder = opendir(tablet->folder);
		struct dirent *dp;
		while ((dp = readdir(tabletFolder)) != NULL) {
			//if d_type is 4, means it is a dir
			if(dp->d_type != 4){
				char *file_path = m_cats(3, tablet->folder, FOLDER_SEPARATOR_STRING, dp->d_name);
				int file_size = get_file_size(file_path);
				logg(INFO, "The file %s's disk_usage:%d.", dp->d_name, file_size);
				disk_usage += file_size/MB;
				free2(file_path);
			}
		}
		closedir(tabletFolder);
		return disk_usage;
}

/** Not only will calculate the total disk usage, will also update the size at tablet instance **/
public int get_used_size_tablet(Tablet *tablet){
	tablet->used_size = get_disk_usage(tablet);
	return tablet->used_size;
}
