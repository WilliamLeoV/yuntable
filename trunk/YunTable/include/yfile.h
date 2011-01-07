#ifndef YFILE_H_
#define YFILE_H_

#include "global.h"
#include "item.h"

typedef struct _YFile YFile;

public YFile* loading_yfile(char* file_path);

public YFile* create_new_yfile(char* file_path, ResultSet* resultSet, char* table_name);

public ResultSet* query_yfile_by_row_key(YFile* yfile, char* row_key);

public ResultSet* query_yfile_by_timestamp(YFile* yfile, int begin_timestamp, int end_timestamp);

public void refresh_yfile_data_block_cache(YFile* yfile);

public char* get_yfile_metadata(YFile* yfile);

#endif /* YFILE_H_ */
