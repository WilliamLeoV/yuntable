#ifndef MASTER_H_
#define MASTER_H_

#include "global.h"
#include "conf.h"

#define DEFAULT_MASTER_PORT 8301
#define DEFAULT_MASTER_CONF_PATH "conf/master.conf"

#define GET_TABLE_INFO_MASTER_CMD "get_table_info_master"

public TableInfo* get_table_info_master(char* table_name);

#define REFRESH_TABLE_INFO_NOW_MASTER_CMD "refresh_table_info_now_master"

/* This cmd will ask the master to check the problem node immediately */
public boolean refresh_region_status_now_master(char* problem_region_conn);

#define ADD_NEW_REGION_MASTER_CMD "add_new_region_master"

public boolean add_new_region_master(char* region_conn);

#define CREATE_NEW_TABLE_MASTER_CMD "create_new_table_master"

public boolean create_new_table_master(char* table_name);

#define ADD_NEW_COLUMN_FAMILY_MASTER_CMD "add_new_column_family_master"

public boolean add_new_column_family_master(char* table_name, char* column_family);


#endif /* MASTER_H_ */
