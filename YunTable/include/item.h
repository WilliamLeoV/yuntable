#ifndef ITEM_H_
#define ITEM_H_

#include "global.h"
#include "list.h"
#include "utils.h"
#include "buf.h"

typedef struct _Key Key;

typedef struct _Item Item;

typedef struct _ResultSet{
        byte magic[8];
        int size;
        Item **items;
}ResultSet;

public Key* get_key(Item *item);

public Key* m_clone_key(Key* key);

public Key* m_load_key(FILE *fp);

public Item* m_create_item(char *row_key, char*column_name, char* value);

public Item* m_load_item(FILE *fp);

public Item* m_clone_item(Item* item);

public char* get_column_name(Item *item);

public long long get_timestamp(Item *item);

public char* get_value(Item *item);

public char* get_row_key(Key *key);

public boolean match_by_timestamps(long long src_begin_timestamp, long long src_end_timestamp,
                long long dest_begin_timestamp, long long dest_end_timestamp);

public boolean between_timestamps(long long timestamp, long long begin_timestamp, long long end_timestamp);

public void flush_key(Key* key, FILE *fp);

public void flush_item(Item *item, FILE *fp);

public Buf* result_set_to_byte(ResultSet* resultSet);

public ResultSet* byte_to_result_set(byte* buf);

public void free_key(Key *key);

public void free_item(Item *item);

public void free_item_array(int item_size, Item** items);

public void free_result_set(ResultSet *resultSet);

public void destory_result_set(ResultSet *resultSet);

public Key* get_last_key(ResultSet* resultSet);

public Key* get_first_key(ResultSet* resultSet);

public int cmp_item_void(const void* item1_void, const void* item2_void);

public int cmp_item(Item* item1, Item* item2);

public int cmp_key_with_row_key(const Key *key, const char* row_key);

public int cmp_item_with_row_key(const Item *item, const char* row_key);

public boolean between_keys(Key *firstKey, Key *lastKey, char *row_key);

public ResultSet* found_items_by_row_key(int size, Item** items, char* row_key);

public ResultSet* found_items_by_timestamp(int size, Item** items, int begin_timestamp, int end_timestamp);

public ResultSet *m_item_list_to_result_set(List* itemList);

public ResultSet* m_create_result_set(int item_size, Item **items);

public void print_result_set_in_nice_format(ResultSet* resultSet);

public ResultSet* m_combine_result_set(ResultSet* set0, ResultSet* set1);

public ResultSet* cleansing(ResultSet* set);

#endif /* ITEM_H_ */
