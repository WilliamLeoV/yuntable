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
