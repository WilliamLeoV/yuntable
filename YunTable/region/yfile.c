#include "global.h"
#include "utils.h"
#include "item.h"
#include "list.h"
#include "yfile.h"

typedef struct _DataBlock{
	byte magic[8];
	Item **items;
	int item_size; /** No Persistence**/
}DataBlock;

typedef struct _Index{
	int offset;
	int item_size;
	Key* lastKey;
	int begin_timestamp; /* the least item timestamp inside the datablock */
	int end_timestamp; /* the last timestamp inside the datablock */
}Index;

typedef struct _IndexBlock{
	byte magic[8];
	int index_count;
	Index** indexs;
	int current_index_allocated_size; /** No Persistence, count allocated size in memory, usable at creating**/
}IndexBlock;

typedef struct _Trailer{
	byte magic[8];
	int index_block_offset;
	short column_family_len;
	byte* column_family;
	short table_name_len;
	byte* table_name;
	Key* firstKey;
	Key* lastKey; /** mainly used as bloom filter**/
	int begin_timestamp; /* the least item timestamp inside the yfile */
	int end_timestamp; /* the last item timestamp inside the yfile */
	int trailer_offset; /** used for locating the trailer **/
}Trailer;

struct _YFile{
	char* file_path; /** No Persistence **/
	IndexBlock *indexBlock; //offset = 16
	Trailer *trailer; //offset = 0
};

#define INIT_INDEX_SIZE 10000 /** the final size will multiply GROWTH_FACTOR, which is 150**/
#define GROWTH_FACTOR 1.5

#define DEFAULT_SIZE_OF_DATA_BLOCK 64 * 1024

#define DATA_BLOCK_MAGIC "datablk"
#define TRAILER_MAGIC "yftrilr"
#define INDEX_BLOCK_MAGIC "indxblk"


private Trailer* create_trailer(int index_block_offset, char* column_family, char* table_name,
		Key* firstKey, Key* lastKey, int begin_timestamp, int end_timestamp, int trailer_offset){
	Trailer *trailer = malloc(sizeof(Trailer));
	cpy(trailer->magic, TRAILER_MAGIC);
	trailer->index_block_offset = index_block_offset;
	trailer->column_family_len = strlen(column_family);
	trailer->column_family = m_cpy(column_family);
	trailer->table_name_len = strlen(table_name);
	trailer->table_name = m_cpy(table_name);
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
	fwrite(&trailer->column_family_len, sizeof(trailer->column_family_len), 1, fp);
	fwrite(trailer->column_family, trailer->column_family_len, 1, fp);
	fwrite(&trailer->table_name_len, sizeof(trailer->table_name_len), 1, fp);
	fwrite(trailer->table_name, trailer->table_name_len, 1, fp);
	flush_key(trailer->firstKey, fp);
	flush_key(trailer->lastKey, fp);
	fwrite(&trailer->begin_timestamp, sizeof(trailer->begin_timestamp), 1, fp);
	fwrite(&trailer->end_timestamp, sizeof(trailer->end_timestamp), 1, fp);
	fwrite(&trailer->trailer_offset, sizeof(trailer->trailer_offset), 1, fp);
}

private Trailer* load_trailer(FILE *fp){
	Trailer *trailer = malloc(sizeof(Trailer));
	fseek(fp, 0, SEEK_END);
	fseek(fp, -sizeof(trailer->trailer_offset), SEEK_CUR);
	fread(&trailer->trailer_offset, sizeof(trailer->trailer_offset), 1, fp);
	fseek(fp, trailer->trailer_offset, SEEK_SET);
	fread(trailer->magic, sizeof(trailer->magic), 1, fp);
	fread(&trailer->index_block_offset, sizeof(trailer->index_block_offset), 1, fp);
	fread(&trailer->column_family_len, sizeof(trailer->column_family_len), 1, fp);
	trailer->column_family = mallocs(trailer->column_family_len);
	fread(trailer->column_family, trailer->column_family_len, 1, fp);
	fread(&trailer->table_name_len, sizeof(trailer->table_name_len), 1, fp);
	trailer->table_name = mallocs(trailer->table_name_len);
	fread(trailer->table_name, trailer->table_name_len, 1, fp);
	trailer->firstKey = m_load_key(fp);
	trailer->lastKey = m_load_key(fp);
	fread(&trailer->begin_timestamp, sizeof(trailer->begin_timestamp), 1, fp);
	fread(&trailer->end_timestamp, sizeof(trailer->end_timestamp), 1, fp);
	return trailer;
}

//will increase the size of data index to 1.5 * target_size
private IndexBlock* resize_indexs(IndexBlock *indexBlock, int target_size){
	indexBlock->current_index_allocated_size = target_size;
	if(indexBlock->indexs == NULL) indexBlock->indexs = malloc(sizeof(Index*) * target_size);
	else indexBlock->indexs = realloc(indexBlock->indexs, sizeof(Index*) * target_size);
	return indexBlock;
}

private IndexBlock* create_index_block(void){
	IndexBlock *indexBlock = malloc(sizeof(IndexBlock));
	cpy(indexBlock->magic, INDEX_BLOCK_MAGIC);
	indexBlock->index_count = 0;
	indexBlock->indexs = NULL;
	indexBlock = resize_indexs(indexBlock, INIT_INDEX_SIZE);
	return indexBlock;
}

/** will copy the lastKey **/
private void append_index(IndexBlock *indexBlock, int offset, int item_size, Key* lastKey,
				int begin_timestamp, int end_timestamp){
	Index *index = malloc(sizeof(Index));
	index->offset = offset;
	index->item_size = item_size;
	index->lastKey = m_clone_key(lastKey);
	index->begin_timestamp = begin_timestamp;
	index->end_timestamp = end_timestamp;
	if(indexBlock->index_count == indexBlock->current_index_allocated_size)
		resize_indexs(indexBlock, indexBlock->current_index_allocated_size * GROWTH_FACTOR);
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
	Index *index = malloc(sizeof(index));
	fread(&index->offset, sizeof(index->offset), 1, fp);
	fread(&index->item_size, sizeof(index->item_size), 1, fp);
	index->lastKey = m_load_key(fp);
	fread(&index->begin_timestamp, sizeof(index->begin_timestamp), 1, fp);
	fread(&index->end_timestamp, sizeof(index->end_timestamp), 1, fp);
	return index;
}

private IndexBlock* load_index_block(int index_block_offset, FILE *fp){
	fseek(fp, index_block_offset, SEEK_SET);
	IndexBlock *indexBlock = malloc(sizeof(IndexBlock));
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

private void flush_item_list(IndexBlock *indexBlock, int begin_offset, ResultSet* resultSet, FILE* fp){
	int i=0, size = 0;
	int offset = begin_offset;
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
				&& cmp_item_with_row_key(item,  get_row_key(resultSet->items[i+1])) != 0){
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

public YFile* create_new_yfile(char* file_path, ResultSet* resultSet, char* column_family, char* table_name){
	logg("Creating a new yfile %s.\n", file_path);
	YFile* yfile = malloc(sizeof(YFile));
	yfile->file_path = m_cpy(file_path);
	yfile->indexBlock = create_index_block();
	FILE *fp = fopen(yfile->file_path, "wb");

	//flush itemList to data block
	int begin_data_block_offset = ftell(fp);
	flush_item_list(yfile->indexBlock, begin_data_block_offset, resultSet, fp);
	//flush index to index block
	int begin_index_block_offset = ftell(fp);
	flush_index_block(yfile->indexBlock, begin_index_block_offset, fp);
	//get key and timestamp create and flush trailer
	Key *firstKey = get_first_key(resultSet);
	Key* lastKey = get_last_key(resultSet);
	int begin_timestamp = search_timestamp(yfile->indexBlock, true);
	int end_timestamp = search_timestamp(yfile->indexBlock, false);
	int trailer_offset = ftell(fp);
	Trailer *trailer = create_trailer(begin_index_block_offset, column_family, table_name,
			firstKey, lastKey, begin_timestamp, end_timestamp, trailer_offset);
	flush_trailer(trailer, fp);
	yfile->trailer = trailer;
	fclose(fp);
	logg("The new yfile %s has been created.\n", file_path);
	return yfile;
}

public YFile* loading_yfile(char* file_path){
	logg("Loading the yfile %s...\n", file_path);
	YFile *yfile = malloc(sizeof(YFile));
	yfile->file_path = m_cpy(file_path);
	FILE *fp = fopen(file_path, "rb+");
	//reaches to the end of file
	fseek(fp, 0, SEEK_END);
	int file_size = ftell(fp);
	//check the file is valid or not
	if(file_size < sizeof(yfile->trailer)) return NULL;
	yfile->trailer = load_trailer(fp);
	//check the loaded trailer is valid or not
	if(cmp(yfile->trailer->magic, TRAILER_MAGIC, strlen(TRAILER_MAGIC))==false) return NULL;
	//load the index
	int index_block_offset = yfile->trailer->index_block_offset;
	yfile->indexBlock = load_index_block(index_block_offset, fp);
	logg("The loading of yfile %s is complete.\n", file_path);
	return yfile;
}

private DataBlock* load_data_block(Index *index, FILE *fp){
	DataBlock* dataBlock = malloc(sizeof(DataBlock));
	dataBlock->item_size = index->item_size;
	dataBlock->items = malloc(sizeof(Item *) * dataBlock->item_size);
	int offset = index->offset, i=0;
	fseek(fp, offset, SEEK_SET);
	//load the magic string
	fread(dataBlock->magic, sizeof(dataBlock->magic), 1, fp);
	//load the items
	for(i=0; i<dataBlock->item_size; i++){
		dataBlock->items[i] = m_load_item(fp);
	}
	return dataBlock;
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

public ResultSet* query_yfile_by_row_key(YFile* yfile, char* row_key){
	ResultSet *resultSet = NULL;
	//step 0. using the bloom filter
	if(bloom_filter_key(yfile, row_key)){
		//step 1. found the data block has the target row_keys
		int founded_index = bsearch_indexs(0, yfile->indexBlock->index_count-1, yfile->indexBlock->indexs, row_key, yfile->trailer->firstKey);
		FILE *fp = fopen(yfile->file_path, "r+");
		DataBlock* dataBlock = load_data_block(yfile->indexBlock->indexs[founded_index], fp);
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
				DataBlock* dataBlock = load_data_block(index, fp);
				ResultSet* set = found_items_by_timestamp(dataBlock->item_size,
						dataBlock->items, begin_timestamp, end_timestamp);
				ResultSet* combinedSet = m_combine_result_set(resultSet, set);
				free_result_set(resultSet);
				free_result_set(set);
				resultSet = combinedSet;
			}
		}
	}
	//TODO handle the free the datablock items
	return resultSet;
}

#ifdef YFILE_TEST
void testcase_for_create_new_yfile(void){
	char *file_path = "test.yfile";
	char *column_family = "default_cf";
	char *table_name = "test_table";
	char *row_key = "me";
	char *column_qualifer = "name";
	char *value = "ike";
	Item *item = m_create_item(row_key, column_family, column_qualifer, value);
	Item** items = malloc(sizeof(Item *) * 1);
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
