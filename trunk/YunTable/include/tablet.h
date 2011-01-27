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

#ifndef TABLET_H_
#define TABLET_H_

#include "global.h"
#include "item.h"

#define DEFAULT_DATASTORE_FILE_FOLDER "tablet"

typedef struct _Tablet Tablet;

public short get_tablet_id(Tablet* tablet);

public void set_last_flushed_id(Tablet* tablet, long long last_flushed_id);

public long get_last_flushed_id(Tablet* tablet);

public void set_max_item_id(Tablet* tablet, long long max_item_id);

public char* get_tablet_folder(Tablet* tablet);

public Tablet* load_tablet(char *tablet_folder);

/** this method will create the tablet folder, table name file and wal file**/
public Tablet* create_tablet(int tablet_id, char *tablet_folder, char *table_name);

public char* get_tablet_folder(Tablet *tablet);

public boolean match_tablet(Tablet *tablet, char* table_name);

public boolean match_tablet_by_table_name(Tablet *tablet, char* table_name);

public void put_tablet(Tablet *tablet, long incr_item_id, Item *item);

public ResultSet* query_tablet_row_key(Tablet *tablet, char* row_key);

public  ResultSet* query_tablet_by_timestamp(Tablet *tablet, long long begin_timestamp, long long end_timestamp);

public ResultSet* query_tablet_all(Tablet *tablet);

public char* get_metadata_tablet(Tablet *tablet);

public void refresh_tablet(Tablet *tablet, int hotnessValue);

public int get_used_size_tablet(Tablet *tablet);

public int get_free_size_tablet(Tablet *tablet);

#endif /* TABLET_H_ */
