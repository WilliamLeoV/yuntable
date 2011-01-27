/**
 * Copyright 2011 Wu Zhu Hua, Xi Ming Gao, Xue Ying Fei, Li Jun Long
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
