#include "global.h"
#include "utils.h"
#include "item.h"

struct _Key{
        short row_key_len;
        char* row_key;
        short column_family_len;
        char* column_family;
        short column_qualifer_len;
        char* column_qualifer;
        long long timestamp;
};

struct _Item{
        Key *key;
        int val_len;
        byte *value;
};

#define RESULT_SET_MAGIC "rsutSet"

public Key* get_key(Item *item){
        return item->key;
}

public Key* m_load_key(FILE *fp){
        Key *key = malloc(sizeof(Key));
        fread(&key->row_key_len, sizeof(key->row_key_len), 1, fp);
        key->row_key = mallocs(key->row_key_len);
        fread(key->row_key, key->row_key_len, 1, fp);
        fread(&key->column_family_len, sizeof(key->column_family_len), 1, fp);
        key->column_family = mallocs(key->column_family_len);
        fread(key->column_family, key->column_family_len, 1, fp);
        fread(&key->column_qualifer_len, sizeof(key->column_qualifer_len), 1, fp);
        key->column_qualifer = mallocs(key->column_qualifer_len);
        fread(key->column_qualifer, key->column_qualifer_len, 1, fp);
        fread(&key->timestamp, sizeof(key->timestamp), 1, fp);
        return key;
}

private Key* create_key(char *row_key, char *column_family, char *column_qualifier){
        Key *key = malloc(sizeof(Key));
        key->row_key_len = strlen(row_key);
        key->row_key = m_cpy(row_key);
        key->column_family_len = strlen(column_family);
        key->column_family = m_cpy(column_family);
        key->column_qualifer_len = strlen(column_qualifier);
        key->column_qualifer = m_cpy(column_qualifier);
        key->timestamp = get_current_time_mills();
        return key;
}

public Key* m_clone_key(Key* key){
        return create_key(key->row_key, key->column_family, key->column_qualifer);
}

public Item* m_create_item(char *row_key, char *column_family, char *column_qualifier, char *value){
        Key *key = create_key(row_key, column_family, column_qualifier);
        //init a new item base on the input
        Item *item = malloc(sizeof(Item));
        item->key = key;
        item->val_len = strlen(value);
        item->value = m_cpy(value);
        return item;
}

public Item* m_load_item(FILE *fp){
        Item *item = malloc(sizeof(Item));
        item->key = m_load_key(fp);
        fread(&item->val_len, sizeof(item->val_len), 1, fp);
        item->value = mallocs(item->val_len);
        fread(item->value, item->val_len, 1, fp);
        return item;
}

public Item* m_clone_item(Item* item){
        return m_create_item(item->key->row_key, item->key->column_family, item->key->column_qualifer, item->value);
}

public char* get_column_family(Item *item){
        return item->key->column_family;
}

public char* get_column_qualifier(Item *item){
        return item->key->column_qualifer;
}

public char* get_value(Item *item){
        return item->value;
}

public char* get_row_key(Item *item){
        return item->key->row_key;
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
        fwrite(&key->column_family_len, sizeof(key->column_family_len), 1,  fp);
        fwrite(key->column_family, key->column_family_len, 1,  fp);
        fwrite(&key->column_qualifer_len, sizeof(key->column_qualifer_len), 1,  fp);
        fwrite(key->column_qualifer, key->column_qualifer_len, 1,  fp);
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
        buf_cat(buf, &key->column_family_len, sizeof(key->column_family_len));
        buf_cat(buf, key->column_family, key->column_family_len);
        buf_cat(buf, &key->column_qualifer_len, sizeof(key->column_qualifer_len));
        buf_cat(buf, key->column_qualifer, key->column_qualifer_len);
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
        Key *key = malloc(sizeof(Key));
        key->row_key_len = buf_load_short(buf);
        key->row_key = buf_load(buf, key->row_key_len);
        key->column_family_len = buf_load_short(buf);
        key->column_family = buf_load(buf, key->column_family_len);
        key->column_qualifer_len = buf_load_short(buf);
        key->column_qualifer = buf_load(buf, key->column_qualifer_len);
        key->timestamp = buf_load_long_long(buf);
        return key;
}

private Item* byte_to_item(Buf* buf){
        Item *item = malloc(sizeof(Item));
        item->key = byte_to_key(buf);
        item->val_len = buf_load_int(buf);
        item->value = buf_load(buf, item->val_len);
        return item;
}

public ResultSet* byte_to_result_set(byte* bytes){
        Buf* buf = create_buf(0, bytes);
        ResultSet *resultSet = malloc(sizeof(ResultSet));
        char* magic_string = buf_load(buf, sizeof(resultSet->magic));
        cpy(resultSet->magic, magic_string);
        free(magic_string);
        resultSet->size = buf_load_int(buf);
        resultSet->items = malloc(sizeof(Item *) * resultSet->size);
        int i=0;
        for(i=0; i<resultSet->size; i++){
                resultSet->items[i] = byte_to_item(buf);
        }
        return resultSet;
}

private void free_key(Key *key){
        free(key->row_key);
        free(key->column_family);
        free(key->column_qualifer);
        free(key);
}

public void free_item(Item* item){
        if(item == NULL) return;
        free_key(item->key);
        free(item->value);
        free(item);
}

/* only free inside items */
public void free_item_array(int item_size, Item** items){
        int i=0;
        for(i=0; i<item_size; i++) free_item(items[i]);
}

public void free_result_set(ResultSet *resultSet){
        if(resultSet == NULL) return;
        free(resultSet->items);
        free(resultSet);
}

/** free all items inside result set **/
public void destory_result_set(ResultSet *resultSet){
        if(resultSet == NULL) return;
        free_item_array(resultSet->size, resultSet->items);
        free(resultSet->items);
        free(resultSet);
}

public int cmp_item(const void* item1_void, const void* item2_void){
        //first load 4bit of address, then converted to item pointer, and only tested at 32bit
        const Item* item1 = (Item*)(*(long*)item1_void);
        const Item* item2 = (Item*)(*(long*)item2_void);
        return cmp_item_with_row_key(item1, item2->key->row_key);
}

public int cmp_key_with_row_key(const Key *key, const char* row_key){
        int max_len = max(key->row_key_len, strlen(row_key));
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

public Key* get_last_key(ResultSet* resultSet){
        return get_key(resultSet->items[resultSet->size-1]);
}

public Key* get_first_key(ResultSet* resultSet){
        return get_key(resultSet->items[0]);
}

public ResultSet *m_create_result_set(int item_size, Item **items){
        ResultSet *resultSet = malloc(sizeof(ResultSet));
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
                char *column_family =  get_column_family(item);
                if(match(column_family, DEFAULT_COLUMN_FAMILY))
                        printf("%s:%s ", get_column_qualifier(item), get_value(item));
                else printf("%s.%s:%s ", column_family, get_column_qualifier(item), get_value(item));
        }
        printf("\n");
}

public ResultSet* m_combine_result_set(ResultSet* set0, ResultSet* set1){
        //init new ResultSetArray
        int total_size = set0->size + set1->size;
        Item** items = malloc(sizeof(Item *) * total_size);
        int i=0, j=0;
        //append array0
        for(i=0; i<set0->size; i++){
                items[i] = set0->items[i];
        }
        //append array1
        for(j=0; j<set1->size; j++, i++){
                items[i] = set1->items[j];
        }
        ResultSet* newSet = m_create_result_set(total_size, items);
        return newSet;
}

public List* get_unique_column_familys(ResultSet* resultSet){
        List* unique_column_familys = list_create();
        int i=0;
        for(i=0; i<resultSet->size; i++){
                char* column_family = resultSet->items[i]->key->column_family;
                char* found = list_find(unique_column_familys, column_family, match_for_list_find);
                if(found == NULL) list_append(unique_column_familys, m_cpy(column_family));
        }
        return unique_column_familys;
}

public ResultSet* filter_result_sets_by_colume_family(ResultSet* resultSet, char* column_family){
        List* itemList = list_create();
        int i=0;
        for(i=0; i<resultSet->size; i++){
                Item *item = resultSet->items[i];
                if(match(item->key->column_family, column_family))
                        list_append(itemList, item);
        }
        return m_item_list_to_result_set(itemList);
}

public void remove_legacy_items(ResultSet* resultSet){
        List* clean_list = list_create();
        int i=0, j=0, k=resultSet->size;
        for(i=0; i<resultSet->size; i++){
                Item *item = resultSet->items[i];
                if(item == NULL) continue;
                for(j=i+1;j<resultSet->size;j++){
                        Item *nextItem = resultSet->items[j];
                        if(nextItem == NULL) continue;
                        if(match(get_column_family(item), get_column_family(nextItem))&&
                                        match(get_column_qualifier(item), get_column_qualifier(nextItem))){
                                if(get_timestamp(nextItem) > get_timestamp(item)){
                                        //use the nextItem to replace item
                                        resultSet->items[i] = nextItem;
                                        item = resultSet->items[i];
                                }
                                //remove the next item
                                resultSet->items[j] = NULL;
                                free_item(resultSet->items[j]);
                                k--;
                        }
                }
                list_append(clean_list, item);
        }
        free_result_set(resultSet);
        resultSet = m_item_list_to_result_set(clean_list);
}

#ifdef ITEM_TEST
void testcase_for_result_set_to_byte(void){
        int index = 2;
        Item** items = malloc(sizeof(Item *) * index);
        items[0] = m_create_item("row1", "column_family", "column_qualifier", "value");
        items[1] = m_create_item("row2", "column_family", "column_qualifier", "value");
        ResultSet* resultSet = m_create_result_set(index, items);
        Buf* buf = result_set_to_byte(resultSet);
        ResultSet* newSet = byte_to_result_set(get_buf_data(buf));
        printf("%s\n", newSet->magic);
        printf("%d\n", newSet->size);
        printf("%s\n", newSet->items[0]->value);
        printf("%s\n", newSet->items[1]->key->column_family);
}

void testcase_for_qsort_item(void){
        int index = 4;
        Item** items = malloc(sizeof(Item *) * index);
        items[0] = m_create_item("b","","","");
        items[1] = m_create_item("c","","","");
        items[2] = m_create_item("d","","","");
        items[3] = m_create_item("a","","","");
        qsort(items, index, sizeof(Item *), cmp_item);
        printf("%s\n",items[0]->key->row_key);
}

void testcase_for_between_keys(void){
        char *keys[] = {"a","b", "c"};
        int index = 3;
        Item** items = malloc(sizeof(Item *) * 3);
        int i = 0;
        for(i = 0; i < index; i++) items[i] = m_create_item(keys[i], "", "", "");
        assert(!between_keys(items[0]->key, items[1]->key, keys[2]));
        assert(between_keys(items[0]->key, items[2]->key, keys[1]));
}

void testcase_for_bsearch_item_index_by_row_key(void){
        char *tests[] = {"a","b", "c","d","e"};
        int index = 5;
        Item** items = malloc(sizeof(Item *) * index);
        int i = 0;
        for(i = 0; i < index; i++) items[i] = m_create_item(tests[i], "", "", "");
        printf("the result:%d\n", bsearch_item_index_by_row_key(0, index-1, items, tests[3]));
}

void testcase_for_cmp_key_with_row_key(void){
        char *test1 = "abc";
        char *test2 = "abd";
        char *test3 = "abe";

        Key *key = malloc(sizeof(Key));
        key->row_key = m_cpy(test1);
        assert(cmp_key_with_row_key(key, test1)==0);
        assert(cmp_key_with_row_key(key, test2)!=0);
        assert(cmp_key_with_row_key(key, test3)!=0);

}

void print_item(Item *item){
        printf("rowkey:%s\n", get_row_key(item));
        printf("column_family:%s\n", get_column_family(item));
        printf("column_qualifier:%s\n", get_column_qualifier(item));
        printf("time_stamp:%d\n", get_timestamp(item));
        printf("value:%s\n", get_value(item));
        printf("\n");
}

void testcase_for_result_set_serializing(void){
        ResultSet* resultSet1 = malloc(sizeof(ResultSet));
        char *row_key1 = "m1";
        char *column_family1 = "cf1";
        char *column_qualifier1 = "name";
        char *value1 = "ike";
        char *row_key2 = "m2";
        char *column_family2 = "cf2";
        char *column_qualifier2 = "name";
        char *value2 = "wu";
        Item *item1 = m_create_item(row_key1, column_family1, column_qualifier1, value1);
        Item *item2 = m_create_item(row_key2, column_family2, column_qualifier2, value2);
        resultSet1->size = 2;
        resultSet1->items = malloc(sizeof(Item *) * 2);
        resultSet1->items[0] = item1;
        resultSet1->items[1] = item2;
        Buf *buf = result_set_to_byte(resultSet1);
        ResultSet* resultSet2 = byte_to_result_set(get_buf_data(buf));
        int i=0;
        for(i=0; i<resultSet2->size; i++) print_item(resultSet2->items[i]);
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
        printf("Completed Successfully\n");
}

int main(int argc, char *argv[]){
        test_suite();
        return 1;
}
#endif
