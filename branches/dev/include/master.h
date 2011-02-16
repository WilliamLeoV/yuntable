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

#ifndef MASTER_H_
#define MASTER_H_

#include "global.h"
#include "conf.h"

/** The below methods just used for reference, won't directly called **/

#define GET_TABLE_INFO_MASTER_CMD "get_table_info_master"

public TableInfo* get_table_info_master(char* table_name);

#define CHECK_PROBLEM_REGION_MASTER_CMD "check_problem_region"

public boolean check_problem_region_master(char* problem_region_conn);

#define ADD_NEW_REGION_MASTER_CMD "add_new_region_master"

public boolean add_new_region_master(char* region_conn);

#define CREATE_NEW_TABLE_MASTER_CMD "create_new_table_master"

public boolean create_new_table_master(char* table_name);

#define GET_METADATA_MASTER_CMD "get_metadata_master"

public char* get_metadata_master();

#endif /* MASTER_H_ */
