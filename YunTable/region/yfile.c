#include "utils.h"
#include "item.h"
#include "list.h"
#include "yfile.h"
#include "malloc2.h"
#include "log.h"

/***
 *  The Document for YFile
 *
 *  1) YFile Format
 *	Data Block 0 -> DataBlockMagic, Item**
 *	Data Block 1
 *	Data Block 2
 *	.....
 *	Index Block -> IndexBlockMagic, index_count, Index** -> offset, item_size, lastKey
 *  Trailer -> TailerBlockMagic, index_block_offset, column_family, table_name, lastKey
 *
 *  The Default size of a Data Block about 64K
 *  The Default size of a YFile about 64MB, not sure, mainly base on the input list size
 **/

typedef struct _DataBlock{
	byte magic[8];
	Item **items;
	size_t item_size; /** No Persistence**/
	long long last_visited_timestamp; /** Used for Hotness Caching **/
}DataBlock;

typedef struct _Index{
	size_t offset;
	size_t item_size;
	Key* lastKey;
	long long begin_timestamp; /* the least item timestamp inside the datablock */
	long long end_timestamp; /* the last timestamp inside the datablock */
}Index;

typedef struct _IndexBlock{
	byte magic[8];
	size_t index_count;
	Index** indexs;
	size_t current_index_allocated_size; /** No Persistence, count allocated size in memory, usable at creating**/
}IndexBlock;

typedef struct _Trailer{
	byte magic[8];
	size_t index_block_offset;
	size_t total_item_num; /** Calculate the total number of stored item **/
	short table_name_len;
	byte* table_name;
	Key* firstKey;
	Key* lastKey; /** mainly used as bloom filter**/
	long long begin_timestamp; /* the least item timestamp inside the yfile */
	long long end_timestamp; /* the last item timestamp inside the yfile */
	size_t trailer_offset; /** used for locating the trailer **/
}Trailer;

struct _YFile{
	char* file_path; /** No Persistence **/
	IndexBlock *indexBlock; //offset = 16
	Trailer *trailer; //offset = 0
	DataBlock** dataBlockCache; /** Used for Hotness Caching **/
};

#define INITIAL_INDEX_SIZE 10000 /** the final size will be multiply GROWTH_FACTOR, which is 150**/
#define INDEX_GROWTH_FACTOR 1.5

#define DEFAULT_SIZE_OF_DATA_BLOCK 64 * 1024

#define DEFAULT_HOTNESS_VALUE 60 * 60 //One Hour

#define DATA_BLOCK_MAGIC "datablk"
#define TRAILER_MAGIC "yftrilr"
#define INDEX_BLOCK_MAGIC "indxblk"


private Trailer* create_trailer(int index_block_offset, char* table_name, size_t total_item_num, Key* firstKey,
	Key* lastKey, long long begin_timestamp, long long end_timestamp, size_t trailer_offset){
		Trailer *trailer = malloc2(sizeof(Trailer));
		cpy(trailer->magic, TRAILER_MAGIC);
		trailer->index_block_offset = index_block_offset;
		trailer->table_name_len = strlen(table_name);
		trailer->table_name = m_cpy(table_name);
		trailer->total_item_num = total_item_num;
		trailer->firstKey = m_clone_key(firstKey);
		trailer->lastKey = m_clone_key(lastKey);
		trailer->begin_timestamp = begin_timestamp;
		trailer->end_timestamp = end_timestamp;
		trailer->trailer_offset = trailer_offset;
		return trailer;
}

private void flush_trailer(Trailer *trailer, FILE *fp){
		fwrite(trailer->magic, sizeof(trailer->magic), 1, fp);
		fwrite(&trailer->index_block_offset, sizeof(trailer->index_block_offset), 1, fp);
		fwrite(&trailer->table_name_len, sizeof(trailer->table_name_len), 1, fp);
		fwrite(trailer->table_name, trailer->table_name_len, 1, fp);
		fwrite(&trailer->total_item_num, sizeof(trailer->total_item_num), 1, fp);
		flush_key(trailer->firstKey, fp);
		flush_key(trailer->lastKey, fp);
		fwrite(&trailer->begin_timestamp, sizeof(trailer->begin_timestamp), 1, fp);
		fwrite(&trailer->end_timestamp, sizeof(trailer->end_timestamp), 1, fp);
		fwrite(&trailer->trailer_offset, sizeof(trailer->trailer_offset), 1, fp);
}

private Trailer* load_trailer(FILE *fp){
		Trailer *trailer = malloc2(sizeof(Trailer));
		fseek(fp, 0, SEEK_END);
		fseek(fp, -sizeof(trailer->trailer_offset), SEEK_CUR);
		fread(&trailer->trailer_offset, sizeof(trailer->trailer_offset), 1, fp);
		fseek(fp, trailer->trailer_offset, SEEK_SET);
		fread(trailer->magic, sizeof(trailer->magic), 1, fp);
		fread(&trailer->index_block_offset, sizeof(trailer->index_block_offset), 1, fp);
		fread(&trailer->table_name_len, sizeof(trailer->table_name_len), 1, fp);
		trailer->table_name = mallocs(trailer->table_name_len);
		fread(trailer->table_name, trailer->table_name_len, 1, fp);
		fread(&trailer->total_item_num, sizeof(trailer->total_item_num), 1, fp);
		trailer->firstKey = m_load_key(fp);
		trailer->lastKey = m_load_key(fp);
		fread(&trailer->begin_timestamp, sizeof(trailer->begin_timestamp), 1, fp);
		fread(&trailer->end_timestamp, sizeof(trailer->end_timestamp), 1, fp);
		return trailer;
}

//will increase the size of data index to 1.5 * target_size
private IndexBlock* resize_indexs(IndexBlock *indexBlock, int target_size){
		indexBlock->current_index_allocated_size = target_size;
		if(indexBlock->indexs == NULL) indexBlock->indexs = malloc2(sizeof(Index*) * target_size);
		else indexBlock->indexs = realloc2(indexBlock->indexs, sizeof(Index*) * target_size);
		return indexBlock;
}

private IndexBlock* create_index_block(void){
		IndexBlock *indexBlock = malloc2(sizeof(IndexBlock));
		cpy(indexBlock->magic, INDEX_BLOCK_MAGIC);
		indexBlock->index_count = 0;
		indexBlock->indexs = NULL;
		indexBlock = resize_indexs(indexBlock, INITIAL_INDEX_SIZE);
		return indexBlock;
}

/** will copy the lastKey **/
private void append_index(IndexBlock *indexBlock, size_t offset, size_t item_size, Key* lastKey,
				long long begin_timestamp, long long end_timestamp){
		Index *index = malloc2(sizeof(Index));
		index->offset = offset;
		index->item_size = item_size;
		index->lastKey = m_clone_key(lastKey);
		index->begin_timestamp = begin_timestamp;
		index->end_timestamp = end_timestamp;
		if(indexBlock->index_count == indexBlock->current_index_allocated_size)
			resize_indexs(indexBlock, indexBlock->current_index_allocated_size * INDEX_GROWTH_FACTOR);
		indexBlock->indexs[indexBlock->index_count] = index;
		indexBlock->index_count++;
}

private void flush_index(Index *index, FILE *fp){
		fwrite(&index->offset, sizeof(index->offset), 1, fp);
		fwrite(&index->item_size, sizeof(index->item_size), 1, fp);
		flush_key(index->lastKey, fp);
		fwrite(&index->begin_timestamp, sizeof(index->begin_timestamp), 1, fp);
		fwrite(&index->end_timestamp, sizeof(index->end_timestamp), 1, fp);
}

private Index* load_index(FILE *fp){
		Index *index = malloc2(sizeof(index));
		fread(&index->offset, sizeof(index->offset), 1, fp);
		fread(&index->item_size, sizeof(index->item_size), 1, fp);
		index->lastKey = m_load_key(fp);
		fread(&index->begin_timestamp, sizeof(index->begin_timestamp), 1, fp);
		fread(&index->end_timestamp, sizeof(index->end_timestamp), 1, fp);
		return index;
}

private IndexBlock* load_index_block(int index_block_offset, FILE *fp){
		fseek(fp, index_block_offset, SEEK_SET);
		IndexBlock *indexBlock = malloc2(sizeof(IndexBlock));
		fread(indexBlock->magic, sizeof(indexBlock->magic), 1, fp);
		fread(&indexBlock->index_count, sizeof(indexBlock->index_count), 1, fp);
		indexBlock->indexs = NULL;
		indexBlock = resize_indexs(indexBlock, indexBlock->index_count);
		int i=0;
		for(i=0; i<indexBlock->index_count; i++) indexBlock->indexs[i] = load_index(fp);
		return indexBlock;
}

private void flush_index_block(IndexBlock *indexBlock, int begin_offset, FILE* fp){
		fseek(fp, begin_offset, SEEK_SET);
		//flush index block
		fwrite(indexBlock->magic, sizeof(indexBlock->magic), 1, fp);
		fwrite(&indexBlock->index_count, sizeof(indexBlock->index_count), 1, fp);
		//flush indexs
		int i=0;
		for(i=0; i<indexBlock->index_count; i++)flush_index(indexBlock->indexs[i], fp);
}


private void flush_item_list(IndexBlock *indexBlock, size_t begin_offset, ResultSet* resultSet, FILE* fp){
		int i=0, size = 0;
		size_t offset = begin_offset;
		fseek(fp, offset, SEEK_SET);
		boolean newBlock = true;
		Key *lastKey = NULL;
		int beign_timestamp = 0;
		int end_timestamp = 0;
		for(i=0;i<resultSet->size;i++){
			Item* item = resultSet->items[i];
			if(newBlock == true){
				offset = ftell(fp);
				fwrite(DATA_BLOCK_MAGIC, sizeof(DATA_BLOCK_MAGIC), 1, fp);
				newBlock = false;
				size = 0;
			}
			flush_item(item, fp);
			//change the timestamp;
			int timestamp = get_timestamp(item);
			if(beign_timestamp == 0 || timestamp < beign_timestamp) beign_timestamp = timestamp;
			if(end_timestamp == 0 || timestamp > end_timestamp) end_timestamp = timestamp;

			lastKey = get_key(item);
			size++;
			//if the block size bigger than default size, also the next item not share the same row key with item
			if(ftell(fp) > offset + DEFAULT_SIZE_OF_DATA_BLOCK
					&& cmp_item_with_row_key(item,  get_row_key(get_key(resultSet->items[i+1]))) != 0){
				append_index(indexBlock, offset, size, lastKey, beign_timestamp, end_timestamp);
				newBlock = true;
			}
		}
		//if list is not null, will append final index
		if(lastKey != NULL) append_index(indexBlock, offset, size, lastKey, beign_timestamp, end_timestamp);
}

/* if the begin is true, the method will return the least timestamp of the index block, if false, will get the last*/
private int search_timestamp(IndexBlock* indexBlock, boolean begin){
		int i=0, timestamp = 0;
		for(i=0; i<indexBlock->index_count; i++){
			Index* index = indexBlock->indexs[i];
			if(timestamp == 0) timestamp = index->begin_timestamp;
			if(begin && index->begin_timestamp < timestamp ) timestamp = index->begin_timestamp;
			if(!begin && index->end_timestamp > timestamp ) timestamp = index->end_timestamp;
		}
		return timestamp;
}

/**
 * creating a new yfile base on the input and the file is immutable after this insertion
 **/
public YFile* create_new_yfile(char* file_path, ResultSet* resultSet, char* table_name){
		logg(INFO, "Creating a new yfile %s.", file_path);
		YFile* yfile = malloc2(sizeof(YFile));
		yfile->file_path = m_cpy(file_path);
		yfile->indexBlock = create_index_block();
		FILE *fp = fopen(yfile->file_path, "wb");

		//flush itemList to data block
		size_t begin_data_block_offset = ftell(fp);
		flush_item_list(yfile->indexBlock, begin_data_block_offset, resultSet, fp);
		//flush index to index block
		size_t begin_index_block_offset = ftell(fp);
		flush_index_block(yfile->indexBlock, begin_index_block_offset, fp);
		//get key and timestamp create and flush trailer
		Key *firstKey = get_first_key(resultSet);
		Key* lastKey = get_last_key(resultSet);
		long long begin_timestamp = search_timestamp(yfile->indexBlock, true);
		long long end_timestamp = search_timestamp(yfile->indexBlock, false);
		size_t trailer_offset = ftell(fp);
		size_t total_item_num = resultSet->size;
		Trailer *trailer = create_trailer(begin_index_block_offset, table_name, total_item_num, firstKey, lastKey,
				begin_timestamp, end_timestamp, trailer_offset);
		flush_trailer(trailer, fp);
		yfile->trailer = trailer;
		fclose(fp);
		logg(INFO, "The new yfile %s has been created.", file_path);
		return yfile;
}

/**
 * mainly is loading index and trailer
 **/
public YFile* loading_yfile(char* file_path){
		logg(INFO, "Loading the yfile %s....", file_path);
		YFile *yfile = malloc2(sizeof(YFile));
		yfile->file_path = m_cpy(file_path);
		FILE *fp = fopen(file_path, "rb+");
		//reaches to the end of file
		fseek(fp, 0, SEEK_END);
		size_t file_size = ftell(fp);
		//check the file is valid or not
		if(file_size < sizeof(yfile->trailer)) return NULL;
		yfile->trailer = load_trailer(fp);
		//check the loaded trailer is valid or not
		if(cmp(yfile->trailer->magic, TRAILER_MAGIC, strlen(TRAILER_MAGIC))==false) return NULL;
		//load the index
		size_t index_block_offset = yfile->trailer->index_block_offset;
		yfile->indexBlock = load_index_block(index_block_offset, fp);
		size_t index_count = yfile->indexBlock->index_count;
		yfile->dataBlockCache = malloc2(sizeof(DataBlock*) * index_count);

		logg(INFO, "The loading of yfile %s is complete.", file_path);
		return yfile;
}

private void free_data_block(DataBlock* dataBlock){
		if(dataBlock != NULL){
			free_item_array(dataBlock->item_size,dataBlock->items);
			frees(2, dataBlock, dataBlock->items);
		}
}

private DataBlock* load_data_block(YFile *yfile, int index_pos, FILE *fp){
		if(yfile->dataBlockCache[index_pos] == NULL){
			Index* index = yfile->indexBlock->indexs[index_pos];
			DataBlock* dataBlock = malloc2(sizeof(DataBlock));
			dataBlock->item_size = index->item_size;
			dataBlock->items = malloc2(sizeof(Item *) * dataBlock->item_size);
			int offset = index->offset, i=0;
			fseek(fp, offset, SEEK_SET);
			//load the magic string
			fread(dataBlock->magic, sizeof(dataBlock->magic), 1, fp);
			//load the items
			for(i=0; i<dataBlock->item_size; i++){
				dataBlock->items[i] = m_load_item(fp);
			}
			yfile->dataBlockCache[index_pos] = dataBlock;
		}
		yfile->dataBlockCache[index_pos]->last_visited_timestamp = get_current_time_stamp();
		return yfile->dataBlockCache[index_pos];
}

private int bsearch_indexs(int l, int r, Index** indexs, char* row_key, Key* firstKey){
		//using bsearch algorithm
		int m = (l+r)/2;
		if(l > r) return -1;
		//compare the previous index with current index, if row_key is between the two index,
		//if current_index ==0, will use traliler first key as first key
		boolean between_result;
		if(m-1>=0) between_result = between_keys(indexs[m-1]->lastKey, indexs[m]->lastKey, row_key);
		else between_result = between_keys(firstKey, indexs[m]->lastKey, row_key);

		int cmp_result = cmp_key_with_row_key(indexs[m]->lastKey, row_key);
		if( cmp_result == 0 || between_result == true) return m;
		else if(l == r ) return -1;
		else if(cmp_result > 0) return bsearch_indexs(l, m-1, indexs, row_key, firstKey);
		else return bsearch_indexs(m+1, r, indexs, row_key, firstKey);
}

private boolean bloom_filter_key(YFile* yfile, char* row_key){
		return between_keys(yfile->trailer->firstKey, yfile->trailer->lastKey, row_key);
}

private boolean bloom_filter_timestamp(YFile* yfile, int begin_timestamp, int end_timestamp){
		return match_by_timestamps(begin_timestamp, end_timestamp, yfile->trailer->begin_timestamp, yfile->trailer->end_timestamp);
}

/**
 * if nothing has been found, the method will return zero size result set
 **/
public ResultSet* query_yfile_by_row_key(YFile* yfile, char* row_key){
		ResultSet *resultSet = NULL;
		//step 0. using the bloom filter
		if(bloom_filter_key(yfile, row_key)){
			//step 1. found the data block has the target row_keys
			int founded_index = bsearch_indexs(0, yfile->indexBlock->index_count-1, yfile->indexBlock->indexs, row_key, yfile->trailer->firstKey);
			FILE *fp = fopen(yfile->file_path, "r+");
			DataBlock* dataBlock = load_data_block(yfile,founded_index, fp);
			fclose(fp);
			//step 2. found the items from the data block
			resultSet = found_items_by_row_key(dataBlock->item_size, dataBlock->items, row_key);
		}else resultSet = m_create_result_set(0, NULL);
		return	resultSet;
}

public ResultSet* query_yfile_by_timestamp(YFile* yfile, int begin_timestamp, int end_timestamp){
		ResultSet *resultSet = m_create_result_set(0, NULL);
		//step 0. using the bloom filter
		if(bloom_filter_timestamp(yfile, begin_timestamp, end_timestamp)){
			FILE *fp = fopen(yfile->file_path, "r+");
			int i=0;
			for(i=0; i<yfile->indexBlock->index_count;i++){
				Index* index = yfile->indexBlock->indexs[i];
				if(match_by_timestamps(index->begin_timestamp, index->end_timestamp, begin_timestamp, end_timestamp)){
					DataBlock* dataBlock = load_data_block(yfile, i, fp);
					ResultSet* set = found_items_by_timestamp(dataBlock->item_size,
							dataBlock->items, begin_timestamp, end_timestamp);
					ResultSet* combinedSet = m_combine_result_set(resultSet, set);
					free_result_set(resultSet);
					free_result_set(set);
					resultSet = combinedSet;
				}
			}
		}
		return resultSet;
}

public void refresh_yfile_data_block_cache(YFile* yfile){
	int i=0, size = yfile->indexBlock->index_count;
	long long current_timestamp = get_current_time_stamp();
	for(i=0; i<size; i++){
		DataBlock* dataBlock = yfile->dataBlockCache[i];
		if(dataBlock != NULL){
			if(dataBlock->last_visited_timestamp + DEFAULT_HOTNESS_VALUE < current_timestamp){
				free_data_block(dataBlock);
			}
		}
	}
}

public char* get_yfile_metadata(YFile* yfile){
		char* line1 = mallocs(1000);
		sprintf(line1, "Total Number of Item: %d.\n",yfile->trailer->total_item_num);
		char* line2 = mallocs(1000);
		sprintf(line2, "The First Row Key: %s.\n", get_row_key(yfile->trailer->firstKey));
		char* line3 = mallocs(1000);
		sprintf(line3, "The Last Row Key: %s.\n", get_row_key(yfile->trailer->lastKey));
		char* line4 = mallocs(1000);
		sprintf(line4, "The Begin Timestamp: %lld.\n", yfile->trailer->begin_timestamp);
		char* line5 = mallocs(1000);
		sprintf(line5, "The Begin Timestamp: %lld.\n", yfile->trailer->begin_timestamp);
		char* metadata = m_cats(5, line1, line2, line3, line4, line5);
		frees(5, line1, line2, line3, line4, line5);
		return metadata;
}


#ifdef YFILE_TEST
void testcase_for_create_new_yfile(void){
	char *file_path = "test.yfile";
	char *column_family = "default_cf";
	char *table_name = "test_table";
	char *row_key = "me";
	char *column_name = "name";
	char *value = "ike";
	Item *item = m_create_item(row_key, column_family, column_name, value);
	Item** items = malloc2(sizeof(Item *) * 1);
	items[0] = item;
	ResultSet* resultSet = m_create_result_set(1,items);
	create_new_yfile(file_path, resultSet, column_family, table_name);
}

void testcase_for_loading_yfile(void){
	char *file_path = "test.yfile";
	YFile *yfile = loading_yfile(file_path);
	printf("%s %s \n",yfile->trailer->column_family, yfile->trailer->table_name);
	printf("%d \n",yfile->indexBlock->index_count);
}

int main(void){
	testcase_for_create_new_yfile();
	testcase_for_loading_yfile();
	return 1;
}
#endif
