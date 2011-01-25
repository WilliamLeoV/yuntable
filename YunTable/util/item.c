#include "global.h"
#include "utils.h"
#include "item.h"
#include "buf.h"
#include "malloc2.h"
#include "log.h"

struct _Key{
		short row_key_len;
		char* row_key;
		short column_name_len; /**If column_name_len == 0, means all columns before timestamp has been deleted **/
		char* column_name; /**If the item is the deletion mark, the column name means the column before timestamp has been deleted **/
		long long timestamp;
};

struct _Item{
        Key *key;
        int val_len; /** If the val_len is 0, means the item is the deletion mark **/
        byte *value;
};

/* The binary format of ResultSet:
 * Magic
 * item_size
 * Item** -> key, val_len, value
 ***/
#define RESULT_SET_MAGIC "rsutSet"

public Key* get_key(Item *item){
        return item->key;
}

public Key* m_load_key(FILE *fp){
		Key *key = malloc2(sizeof(Key));
		fread(&key->row_key_len, sizeof(key->row_key_len), 1, fp);
		key->row_key = mallocs(key->row_key_len);
		fread(key->row_key, key->row_key_len, 1, fp);
		fread(&key->column_name_len, sizeof(key->column_name_len), 1, fp);
		key->column_name = mallocs(key->column_name_len);
		fread(key->column_name, key->column_name_len, 1, fp);
		fread(&key->timestamp, sizeof(key->timestamp), 1, fp);
		return key;
}

private Key* create_key(char *row_key, char *column_name){
		if(row_key == NULL){
			logg(ISSUE, "Insert Null Row key!!!!");
		}
		Key *key = malloc2(sizeof(Key));
		key->row_key_len = strlen(row_key);
		key->row_key = m_cpy(row_key);
		if(column_name != NULL){
			key->column_name_len = strlen(column_name);
			key->column_name = m_cpy(column_name);
		}else{
			key->column_name_len = 0;
			key->column_name = NULL;
		}
		key->timestamp = get_current_time_stamp();
		return key;
}

public Key* m_clone_key(Key* key){
		return create_key(key->row_key, key->column_name);
}

public Item* m_create_item(char *row_key, char *column_name, char *value){
		Key *key = create_key(row_key, column_name);
		//init a new item base on the input
		Item *item = malloc2(sizeof(Item));
		item->key = key;
		if(value != NULL){
			item->val_len = strlen(value);
			item->value = m_cpy(value);
		}else{
			item->val_len = 0;
			item->value = NULL;
		}
		return item;
}

public Item* m_load_item(FILE *fp){
        Item *item = malloc2(sizeof(Item));
        item->key = m_load_key(fp);
        fread(&item->val_len, sizeof(item->val_len), 1, fp);
        item->value = mallocs(item->val_len);
        fread(item->value, item->val_len, 1, fp);
        return item;
}

public Item* m_clone_item(Item* item){
		return m_create_item(item->key->row_key, item->key->column_name, item->value);
}

public char* get_column_name(Item *item){
		return item->key->column_name;
}

public char* get_value(Item *item){
        return item->value;
}

public char* get_row_key(Key *key){
		return key->row_key;
}

public long long get_timestamp(Item *item){
        return item->key->timestamp;
}

/* find the two sets timestamp has some shared space */
public boolean match_by_timestamps(long long src_begin_timestamp, long long src_end_timestamp,
                long long dest_begin_timestamp, long long dest_end_timestamp){
        if(src_begin_timestamp > dest_end_timestamp || src_end_timestamp < dest_begin_timestamp) return false;
        else return true;
}

public boolean between_timestamps(long long timestamp, long long begin_timestamp, long long end_timestamp){
        if(timestamp >= begin_timestamp || timestamp <= end_timestamp) return true;
        else return false;
}

public void flush_key(Key* key, FILE *fp){
		fwrite(&key->row_key_len, sizeof(key->row_key_len), 1,  fp);
		fwrite(key->row_key, key->row_key_len, 1,  fp);
		fwrite(&key->column_name_len, sizeof(key->column_name_len), 1,  fp);
		fwrite(key->column_name, key->column_name_len, 1,  fp);
		fwrite(&key->timestamp, sizeof(key->timestamp), 1,  fp);
}

public void flush_item(Item *item, FILE *fp){
        flush_key(item->key, fp);
        fwrite(&item->val_len, sizeof(item->val_len), 1, fp);
        fwrite(item->value, item->val_len, 1, fp);
}

private Buf* key_to_byte(Key *key){
		Buf *buf = init_buf();
		buf_cat(buf, &key->row_key_len, sizeof(key->row_key_len));
		buf_cat(buf, key->row_key, key->row_key_len);
		buf_cat(buf, &key->column_name_len, sizeof(key->column_name_len));
		buf_cat(buf, key->column_name, key->column_name_len);
		buf_cat(buf, &key->timestamp, sizeof(key->timestamp));
		return buf;
}

private Buf* item_to_byte(Item* item){
        Buf *buf = init_buf();
        Buf *key_buf = key_to_byte(item->key);
        buf_combine(buf, key_buf);
        buf_cat(buf, &item->val_len, sizeof(item->val_len));
        buf_cat(buf, item->value, item->val_len);

        destory_buf(key_buf);
        return buf;
}

public Buf* result_set_to_byte(ResultSet* resultSet){
        Buf *buf = init_buf();
        buf_cat(buf, resultSet->magic, sizeof(resultSet->magic));
        buf_cat(buf, &resultSet->size, sizeof(resultSet->size));
        int i=0;
        for(i=0; i<resultSet->size; i++){
			Buf* item_buf = item_to_byte(resultSet->items[i]);
			buf_combine(buf, item_buf);

			destory_buf(item_buf);
        }
        return buf;
}

private Key* byte_to_key(Buf* buf){
		Key *key = malloc2(sizeof(Key));
		key->row_key_len = buf_load_short(buf);
		key->row_key = buf_load(buf, key->row_key_len);
		key->column_name_len = buf_load_short(buf);
		key->column_name = buf_load(buf, key->column_name_len);
		key->timestamp = buf_load_long_long(buf);
		return key;
}

private Item* byte_to_item(Buf* buf){
        Item *item = malloc2(sizeof(Item));
        item->key = byte_to_key(buf);
        item->val_len = buf_load_int(buf);
        item->value = buf_load(buf, item->val_len);
        return item;
}

public ResultSet* byte_to_result_set(byte* bytes){
        Buf* buf = create_buf(0, bytes);
        ResultSet *resultSet = malloc2(sizeof(ResultSet));
        char* magic_string = buf_load(buf, sizeof(resultSet->magic));
        cpy(resultSet->magic, magic_string);
        free2(magic_string);
        resultSet->size = buf_load_int(buf);
        resultSet->items = malloc2(sizeof(Item *) * resultSet->size);
        int i=0;
        for(i=0; i<resultSet->size; i++){
        	resultSet->items[i] = byte_to_item(buf);
        }
        return resultSet;
}

private void free_key(Key *key){
		frees(3, key->row_key, key->column_name, key);
}

public void free_item(Item* item){
        if(item == NULL) return;
        free_key(item->key);
        frees(2, item->value, item);
}

/* only free inside items */
public void free_item_array(int item_size, Item** items){
        int i=0;
        for(i=0; i<item_size; i++) free_item(items[i]);
}

/**won't free inside items**/
public void free_result_set(ResultSet *resultSet){
        if(resultSet == NULL) return;
        frees(2, resultSet->items, resultSet);
}

/** free all items inside result set **/
public void destory_result_set(ResultSet *resultSet){
        if(resultSet == NULL) return;
        free_item_array(resultSet->size, resultSet->items);
        frees(2, resultSet->items, resultSet);
}

/** This method is used for qsort **/
public int cmp_item_void(const void* item1_void, const void* item2_void){
		  //first load 4byte of address, then converted to item pointer, and only tested at 32bit
		  Item* const item1 = (Item*)(*(long*)item1_void);
		  Item* const item2 = (Item*)(*(long*)item2_void);
		  return cmp_item(item1, item2);
}

/** Will item base on row key, timestamp and column name, will return result if either one of them are not match
 *  If the method return zero, means the two items are the same
 * **/
public int cmp_item(Item* item1, Item* item2){
		int key_cmp = cmp_item_with_row_key(item1, item2->key->row_key);
        if(key_cmp != 0){
        	return key_cmp;
        }
        if(item1->key->timestamp == item2->key->timestamp){
			char* column_name1 = item1->key->column_name;
			char* column_name2 = item2->key->column_name;
        	//The Column Name NULL is bigger than other Column Name
			if(column_name1 == NULL && column_name2 == NULL){
        		return 0;
        	}else if(column_name1 == NULL){
        		return 1;
        	}else if(column_name2 == NULL){
        		return -1;
        	}else{
        		return match_int(column_name1, column_name2);
        	}
        }else if(item1->key->timestamp > item2->key->timestamp){
        	return 1;
        }else{
        	return -1;
        }
}

/** If the two items share same row key, the method will return 0 **/
public int cmp_key_with_row_key(const Key *key, const char* row_key){
        int max_len = max(key->row_key_len, strlen(row_key));
        //Since the case is matter, so it not used the match method from Util
        return strncmp(key->row_key, row_key, max_len);
}

public int cmp_item_with_row_key(const Item *item, const char* row_key){
        return cmp_key_with_row_key(item->key, row_key);
}

public boolean between_keys(Key *firstKey, Key *lastKey, char *row_key){
        if(cmp_key_with_row_key(firstKey, row_key) <= 0 &&
                        cmp_key_with_row_key(lastKey, row_key) >=0)
                 return true;
        else return false;
}

private int bsearch_item_index_by_row_key(int l, int r, Item** items, char *row_key){
        int m = (l+r)/2;
        if(l > r) return -1;
        int cmp_result = cmp_item_with_row_key(items[m], row_key);
        if( cmp_result == 0) return m;
        else if(l == r ) return -1;
        else if(cmp_result > 0) return bsearch_item_index_by_row_key(l, m-1, items, row_key);
        else return bsearch_item_index_by_row_key(m+1, r, items, row_key);
}

public ResultSet* found_items_by_row_key(int size, Item** items, char* row_key){
        List *itemList = list_create();
        int index = bsearch_item_index_by_row_key(0, size-1, items, row_key);
        if(index != -1){
                list_append(itemList, items[index]);
                //left rotate the index
                int left_index = index - 1;
                while(left_index >= 0 && cmp_item_with_row_key(items[left_index], row_key)==0){
					list_append(itemList, items[left_index]);
					left_index--;
                }
                //right rotate the index
                int right_index = index + 1;
                while(right_index < size && cmp_item_with_row_key(items[right_index], row_key)==0){
					list_append(itemList, items[right_index]);
					right_index++;
                }
        }
        ResultSet* resultSet = m_item_list_to_result_set(itemList);
        list_destory(itemList, only_free_struct);
        return resultSet;
}

public ResultSet* found_items_by_timestamp(int size, Item** items, int begin_timestamp, int end_timestamp){
        List* itemList = list_create();
        int i=0;
        for(i=0; i<size; i++){
			if(between_timestamps(items[i]->key->timestamp, begin_timestamp, end_timestamp))
				list_append(itemList, items[i]);
        }
        ResultSet* resultSet = m_item_list_to_result_set(itemList);
        list_destory(itemList, only_free_struct);
        return resultSet;
}

/** the caller need to make sure the result set is not empty**/
public Key* get_last_key(ResultSet* resultSet){
        return get_key(resultSet->items[resultSet->size-1]);
}

public Key* get_first_key(ResultSet* resultSet){
        return get_key(resultSet->items[0]);
}

/** input items will be inserted into a new result set **/
public ResultSet *m_create_result_set(int item_size, Item **items){
        ResultSet *resultSet = malloc2(sizeof(ResultSet));
        cpy(resultSet->magic, RESULT_SET_MAGIC);
        resultSet->size = item_size;
        resultSet->items = items;
        return resultSet;
}

public ResultSet *m_item_list_to_result_set(List* itemList){
        int item_size = list_size(itemList);
        Item **items = (Item**)list_to_array(itemList);
        return m_create_result_set(item_size, items);
}

public void print_result_set_in_nice_format(ResultSet* resultSet){
		int i=0;
		for(i=0; i<resultSet->size; i++){
			Item *item = resultSet->items[i];
			if(item != NULL){
				printf("%s %s:%s timstamp:%lld\n", item->key->row_key, item->key->column_name,
						item->value, item->key->timestamp);
			}
		}
}

/** The method will merge the and remove duplication, besides two inputed set needs be sorted,
 *  but the return will keep item copy.
 ***/
public ResultSet* m_combine_result_set(ResultSet* leftSet, ResultSet* rightSet){
        //init new ResultSetArray
        int total_size = leftSet->size + rightSet->size;
        Item** items = malloc2(sizeof(Item *) * total_size);
        int left = 0; //The Pointer for left ResultSet
        int right = 0; //The Pointer for right ResultSet
        int index = 0; //The Pointer of the new ResultSet
        //The method will iterate after all two sets has been processed
        while(left!=leftSet->size || right!=rightSet->size){
        	//If the right set has been all processed
        	if(right == rightSet->size){
        		//the left set can immediately insert into the new set
        		items[index] = leftSet->items[left];
        		left++;
        	}
        	//If the left set has been all processed
        	else if(left == leftSet->size){
        		items[index] = rightSet->items[right];
        		right++;
        	}else{
        		Item* leftItem = leftSet->items[left];
        		Item* rightItem = rightSet->items[right];
        		int cmp_result = cmp_item(leftItem, rightItem);
        		//If the two items are the same, insert the left and discard right
        		if(cmp_result == 0){
        			items[index] = leftItem;
        			left++;
        			//If the val lens are same, means they are same
        			if(leftItem->val_len == rightItem->val_len)
        			right++;
        		}
        		//If left smaller than right
        		else if(cmp_result < 0){
        			items[index] = leftItem;
        			left++;
        		}
        		//If right smalled than left
        		else{
        			items[index] = rightItem;
        			right++;
        		}
        	}
        	index++;
        }
        ResultSet* newSet = m_create_result_set(index, items);
        return newSet;
}

/**
 * 	Removing deleted item from result set, and return result set will have some empty slots.
 *  TODO This method contains some duplication code.
 * */
public ResultSet* cleansing(ResultSet* set){
		int i;
		for(i=0; i<set->size; i++){
			Item* item = set->items[i];
			//check if the item is the row key deletion mark,
			//which will delete the row key's all items before this item's timestamp
			if(item->val_len == 0 && item->key->column_name_len ==0){
				int j=0;//The reverse index, the first item gonna be the item before current deletion mark
				for(j=i-1; j>=0; j--){
					Item *prevItem = set->items[j];
					if(prevItem == NULL) continue;
					//The procedure will break if the prev item was not belong to this key
					if(strcmp(item->key->row_key, prevItem->key->row_key) != 0){
						break;
					}else{
						free_item(prevItem);
						set->items[j] = NULL;
					}
				}
				free_item(item);
				set->items[i] = NULL;
			}
			//check if the item is the column name deletion mark,
			//which will delete all item belong to this row key and column name before this item's timestamp
			else if(item->val_len == 0){
				int j=0;
				for(j=i-1; j>=0; j--){
					Item *prevItem = set->items[j];
					if(prevItem == NULL) continue;
					//The procedure will break if the prev item was not belong to this key
					if(strcmp(item->key->row_key, prevItem->key->row_key) != 0){
						break;
					}else if(strcmp(item->key->column_name, prevItem->key->column_name) == 0){
						free_item(prevItem);
						set->items[j] = NULL;
					}
				}
				free_item(item);
				set->items[i] = NULL;
			}
		}
		return set;
}

#ifdef ITEM_TEST
void testcase_for_result_set_to_byte(void){
        int index = 2;
        Item** items = malloc2(sizeof(Item *) * index);
        items[0] = m_create_item("row1", "column_qualifier", "value");
        items[1] = m_create_item("row2",  "column_qualifier", "value");
        ResultSet* resultSet = m_create_result_set(index, items);
        Buf* buf = result_set_to_byte(resultSet);
        ResultSet* newSet = byte_to_result_set(get_buf_data(buf));
        printf("%s\n", newSet->magic);
        printf("%d\n", newSet->size);
        printf("%s\n", newSet->items[0]->value);
}

void testcase_for_qsort_item(void){
        int index = 4;
        Item** items = malloc2(sizeof(Item *) * index);
        items[0] = m_create_item("b","","");
        items[1] = m_create_item("c","","");
        items[2] = m_create_item("d","","");
        items[3] = m_create_item("a","","");
        qsort(items, index, sizeof(Item *), cmp_item_void);
        printf("%s\n",items[0]->key->row_key);
}

void testcase_for_between_keys(void){
        char *keys[] = {"a","b", "c"};
        int index = 3;
        Item** items = malloc2(sizeof(Item *) * 3);
        int i = 0;
        for(i = 0; i < index; i++) items[i] = m_create_item(keys[i], "", "");
        assert(!between_keys(items[0]->key, items[1]->key, keys[2]));
        assert(between_keys(items[0]->key, items[2]->key, keys[1]));
}

void testcase_for_bsearch_item_index_by_row_key(void){
        char *tests[] = {"a","b", "c","d","e"};
        int index = 5;
        Item** items = malloc2(sizeof(Item *) * index);
        int i = 0;
        for(i = 0; i < index; i++) items[i] = m_create_item(tests[i], "", "");
        printf("the result:%d\n", bsearch_item_index_by_row_key(0, index-1, items, tests[3]));
}

void testcase_for_cmp_key_with_row_key(void){
        char *test1 = "abc";
        char *test2 = "abd";
        char *test3 = "abe";

        Key *key = malloc2(sizeof(Key));
        key->row_key = m_cpy(test1);
        assert(cmp_key_with_row_key(key, test1)==0);
        assert(cmp_key_with_row_key(key, test2)!=0);
        assert(cmp_key_with_row_key(key, test3)!=0);
}

void print_item(Item *item){
		printf("column_qualifier:%s\n", get_column_name(item));
		printf("time_stamp:%lld\n", get_timestamp(item));
		printf("value:%s\n", get_value(item));
		printf("\n");
}

void testcase_for_result_set_serializing(void){
        ResultSet* resultSet1 = malloc2(sizeof(ResultSet));
        char *row_key1 = "m1";
        char *column_qualifier1 = "name";
        char *value1 = "ike";
        char *row_key2 = "m2";
        char *column_qualifier2 = "name";
        char *value2 = "wu";
        Item *item1 = m_create_item(row_key1, column_qualifier1, value1);
        Item *item2 = m_create_item(row_key2, column_qualifier2, value2);
        resultSet1->size = 2;
        resultSet1->items = malloc2(sizeof(Item *) * 2);
        resultSet1->items[0] = item1;
        resultSet1->items[1] = item2;
        Buf *buf = result_set_to_byte(resultSet1);
        ResultSet* resultSet2 = byte_to_result_set(get_buf_data(buf));
        int i=0;
        for(i=0; i<resultSet2->size; i++) print_item(resultSet2->items[i]);
}

void testcase_for_combine_and_cleansing_result_set(){
		Item* item1 = m_create_item("1", "column_qualifier", "value");
		Item* item2 = m_create_item("2", "column_qualifier", "value");
		Item* item3 = m_create_item("2", "column_qualifier", NULL);
		Item* item4 = m_create_item("3", "column_qualifier", "value");
		Item* item5 = m_create_item("3", NULL, NULL);
		Item* item6 = m_create_item("6", "column_qualifier", "value");
		Item** items1 = malloc2(sizeof(Item*) * 4);
		items1[0] = item1;
		items1[1] = item2;
		items1[2] = item3;
		items1[3] = item4;
		ResultSet* leftSet = m_create_result_set(4, items1);
		Item** items2 = malloc2(sizeof(Item*) * 4);
		items2[0] = item1;
		items2[1] = item2;
		items2[2] = item5;
		items2[3] = item6;
		ResultSet* rightSet = m_create_result_set(4, items2);
		ResultSet* resultSet = m_combine_result_set(leftSet, rightSet);
		printf("The ResultSet after combining.\n");
		print_result_set_in_nice_format(resultSet);
		resultSet = cleansing(resultSet);
		printf("The ResultSet after cleansing.\n");
		print_result_set_in_nice_format(resultSet);
}

void test_suite(void){
        printf("The Test Suit of item is starting\n");
        printf("0) testcase_for_result_set_to_byte\n");
        testcase_for_result_set_to_byte();
        printf("1) testcase_for_between_keys\n");
        testcase_for_between_keys();
        printf("2) testcase_for_bsearch_item_index_by_row_key\n");
        testcase_for_bsearch_item_index_by_row_key();
        printf("3) testcase_for_cmp_key_with_row_key\n");
        testcase_for_cmp_key_with_row_key();
        printf("4) testcase_for_result_set_serializing\n");
        testcase_for_result_set_serializing();
        printf("5) testcase_for_qsort_item\n");
        testcase_for_qsort_item();
        printf("6) testcase_for_combine_and_cleansing_result_set\n");
        testcase_for_combine_and_cleansing_result_set();
        printf("Completed Successfully\n");
}

int main(int argc, char *argv[]){
        test_suite();
		return 1;
}
#endif
