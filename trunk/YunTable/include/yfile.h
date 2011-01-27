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

#ifndef YFILE_H_
#define YFILE_H_

#include "global.h"
#include "item.h"

typedef struct _YFile YFile;

public YFile* loading_yfile(char* file_path);

public YFile* create_new_yfile(char* file_path, ResultSet* resultSet, char* table_name);

public ResultSet* query_yfile_by_row_key(YFile* yfile, char* row_key);

public ResultSet* query_yfile_by_timestamp(YFile* yfile, long long begin_timestamp, long long end_timestamp);

public void refresh_yfile_data_block_cache(YFile* yfile, int hotness_value);

public char* get_yfile_metadata(YFile* yfile);

#endif /* YFILE_H_ */
