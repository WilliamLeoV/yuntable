#ifndef MEMSTORE_H_
#define MEMSTORE_H_

#include "global.h"
#include "list.h"
#include "item.h"

typedef struct _Memstore Memstore;

public Memstore* init_memstore(int id);

public boolean memstore_full(Memstore* memstore);

public void append_memstore(Memstore *memstore, Item *item);

public ResultSet* get_all_sorted_items_memstore(Memstore *memstore);

public ResultSet* get_all_items_memstore(Memstore *memstore);

public void sort_memstore(Memstore* memstore);

public ResultSet* query_memstore_by_row_key(Memstore* memstore, char* row_key);

public ResultSet* query_memstore_by_timestamp(Memstore* memstore, long long begin_timestamp, long long end_timestamp);

public Memstore* reset_memstore(Memstore *memstore, int flushed_size);

public char* get_memstore_metadata(Memstore* memstore);

#endif /* MEMSTORE_H_ */
