#ifndef REGION_H_
#define REGION_H_

#include "global.h"

#define DEFAULT_REGION_CONF_PATH "conf/region.conf"

/** the methods just used for reference in the RPC Request**/

#define GET_COLUMN_FAMILYS_CMD "get_column_familys"

public char* get_column_familys(char *table_name);

#define ADD_NEW_TABLET_REGION_CMD "add_new_tablet"

public boolean add_new_tablet_region(char *table_name, char* column_famliy);

#define PUT_DATA_REGION_CMD "put_data_region"

public boolean put_data_region(char *table_name, char* column_family, ResultSet* resultSet);

#define QUERY_ROW_REGION_CMD "query_row_region"

public ResultSet* query_row_region(char* table_name, char* column_family, char* row_key);

#define QUERY_ALL_REGION_CMD "query_all_region"

public ResultSet* query_all_region(char* table_name, char* column_family);

#define AVAILABLE_SPACE_REGION_CMD "available_space_region"

public int available_space_region(void);

#define TABLET_USED_SIZE_REGION_CMD "tablet_used_size_region"

/* if this cmd returns 0, means the table has not been created */
public int tablet_used_size_region(char* table_name, char* column_family);

#define START_SYNC_REGION_CMD "start_sync_region"

public boolean start_sync_region(char* target_conn, char* table_name, char* column_family, int begin_timestamp, int end_timestamp);


#endif /* REGION_H_ */
