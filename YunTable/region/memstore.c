#include "utils.h"
#include "list.h"
#include "item.h"
#include "memstore.h"
#include "malloc2.h"
#include "log.h"

#define ONE_ALLOCATED_SIZE 10000
#define DEFAULT_SORT_WATERMARK 10000
#define FLUSH_THERSHOLD_PERCENT 0.98

#define BEGIN_ALLOCATED_SIZE 500000
#define END_ALLOCATED_SIZE 640000

struct _Memstore{
		int tablet_id; /** Used for logging information purpose **/
        Item** items; /**used array for better searching and sorting, since the store will be very big**/
        size_t used_size; /** the number of slots has been used **/
        size_t sorted_size;  /** the cursor for defining the sorted mark **/
        size_t allocated_size; /** the size of items has been allocated in memory, the default is 500000 **/
        size_t max_allocated_size; /** the threshold for flushing, and will be assigned by the tablet **/
        long long begin_timestamp; /* the least item timestamp from memstore's all items */
        long long end_timestamp; /* the last item timestamp from memstore's all items */
};

private int generate_random_allocated_size(){
        int percent = generate_random_int();
        int diff = END_ALLOCATED_SIZE - BEGIN_ALLOCATED_SIZE;
        int max_allocated_size = BEGIN_ALLOCATED_SIZE + (percent * diff / 100);
        logg(INFO, "The Memstore's random generated allocated size: %d", max_allocated_size);
        return max_allocated_size;
}

public Memstore* init_memstore(int id){
        Memstore *memstore = malloc2(sizeof(Memstore));
        memstore->tablet_id = id;
        memstore->items = malloc2(sizeof(Item *) * ONE_ALLOCATED_SIZE);
        memstore->allocated_size = ONE_ALLOCATED_SIZE;
        memstore->used_size = 0;
        memstore->sorted_size = 0;
        memstore->max_allocated_size = generate_random_allocated_size();
        return memstore;
}

public boolean memstore_full(Memstore* memstore){
        //if(memstore->used_size > 0) return true; a trigge that init flush every time
        //if the used size about 98% allocated size, that means the memstore is full
        if(memstore->used_size > memstore->max_allocated_size * FLUSH_THERSHOLD_PERCENT){
        	logg(INFO, "The memstore for tablet %d is almost full.", memstore->tablet_id);
        	return true;
        }else{
        	return false;
        }
}

private Memstore* enlarge_memstore(Memstore *memstore){
		logg(INFO, "Enlarge memstore for tablet %d.", memstore->tablet_id);
        Item** items;
        int target_size = memstore->allocated_size + ONE_ALLOCATED_SIZE;
        items = realloc2(memstore->items, target_size * sizeof(Item *));
		memstore->items = items;
		memstore->allocated_size = target_size;
		return memstore;
}

public void append_memstore(Memstore *memstore, Item *item){
        if(memstore->used_size == memstore->allocated_size) {
        	memstore = enlarge_memstore(memstore);
        }
        memstore->items[memstore->used_size] = item;
        memstore->used_size++;
        //update the memstore's timestamp
        long long timestamp = get_timestamp(item);
        if(memstore->begin_timestamp == 0 || timestamp < memstore->begin_timestamp )
			memstore->begin_timestamp = timestamp;
        if(memstore->end_timestamp == 0 || timestamp > memstore->end_timestamp)
			memstore->end_timestamp = timestamp;
}

public ResultSet* get_all_sorted_items_memstore(Memstore *memstore){
        return m_create_result_set(memstore->sorted_size, memstore->items);
}

public ResultSet* get_all_items_memstore(Memstore *memstore){
        return m_create_result_set(memstore->used_size, memstore->items);
}

/** if the machine powers off during reset, may have chance of losing data, TODO fix it in the later release **/
public Memstore* reset_memstore(Memstore *memstore, int flushed_size){
		logg(INFO, "The memstore resetting process for tablet %d has begin.", memstore->tablet_id);
        int tablet_id = memstore->tablet_id;
		//check if have new added items has been flushed
        List* new_added_item_list = list_create();
        int i = 0;
        if(memstore->used_size > flushed_size){
			for(i=flushed_size-1; i<memstore->used_size; i++)
				list_append(new_added_item_list, m_clone_item(memstore->items[i]));
        }
        //recreate memstore
        free_item_array(memstore->used_size, memstore->items);
        frees(2, memstore,memstore->items);
        memstore = init_memstore(tablet_id);
        //append the new added_item_list to the memstore
        Item* item = NULL;
        while((item = list_next(new_added_item_list)) != NULL){
                append_memstore(memstore, item);
        }
        list_rewind(new_added_item_list);
        list_destory(new_added_item_list, only_free_struct);
        return memstore;
}

private ResultSet* query_unsorted_part(Memstore* memstore, char* row_key){
        List *itemList = list_create();
        int i=0;
        for(i=memstore->sorted_size; i<memstore->used_size; i++){
			if(cmp_item_with_row_key(memstore->items[i], row_key)==0)
				list_append(itemList, memstore->items[i]);
        }
        ResultSet* set = m_item_list_to_result_set(itemList);
        list_destory(itemList, only_free_struct);
        return set;
}

public ResultSet* query_memstore_by_row_key(Memstore* memstore, char* row_key){
        ResultSet* combined_set;
        //step 0. searching the sorted part by using bsearch algorithm
        ResultSet* set1 = found_items_by_row_key(memstore->sorted_size, memstore->items, row_key);
        //step 1. query unsorted part of memstore
        ResultSet* set2 = query_unsorted_part(memstore, row_key);
        if (set1 && set2) {
            combined_set = m_combine_result_set(set1, set2);
        }
        frees(2, set1, set2);
        return combined_set;
}

public ResultSet* query_memstore_by_timestamp(Memstore* memstore, int begin_timestamp, int end_timestamp){
        List* itemList = list_create();
        if(match_by_timestamps(begin_timestamp, end_timestamp, memstore->begin_timestamp, memstore->end_timestamp)){
			int i=0;
			for(i=0; i<memstore->used_size; i++){
				Item* item = memstore->items[i];
				if(between_timestamps(get_timestamp(item), begin_timestamp, end_timestamp))
					list_append(itemList, item);
			}
        }
        return m_item_list_to_result_set(itemList);
}

public void sort_memstore(Memstore* memstore){
        int sorted_size = memstore->used_size;
        qsort(memstore->items, sorted_size, sizeof(Item *), cmp_item);
        memstore->sorted_size = sorted_size;
}

public char* get_memstore_metadata(Memstore* memstore){
		char* metadata = mallocs(LINE_BUF_SIZE);
		sprintf(metadata, "The Number of Item inside memstore: %d.\n", memstore->used_size);
		return metadata;
}
