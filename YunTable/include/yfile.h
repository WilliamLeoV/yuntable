#ifndef YFILE_H_
#define YFILE_H_

#include "global.h"
#include "item.h"

typedef struct _YFile YFile;

/**
 * mainly is loading index and trailer
 **/
public YFile* loading_yfile(char* file_path);

/**
 * creating a new yfile base on the input and the file is immutable after this insertion
 **/

public YFile* create_new_yfile(char* file_path, ResultSet* resultSet, char* column_family, char* table_name);
/**
 * if nothing has been found, the method will return zero size result set
 **/
public ResultSet* query_yfile_by_row_key(YFile* yfile, char* row_key);

public ResultSet* query_yfile_by_timestamp(YFile* yfile, int begin_timestamp, int end_timestamp);


#endif /* YFILE_H_ */


/***
 *  The Document for YFile
 *
 *  1) YFile Format
 *	Data Block 0 -> DataBlockMagic, Item**
 *	Data Block 1
 *	Data Block 2
 *	.....
 *	Index Block -> IndexBlockMagic, index_count, Index** -> offset, item_size, lastKey
 *  Trailer -> TailerBlockMagic, index_block_offset, column_family, table_name, lastKey
 *
 *  The Default size of a Data Block about 64K
 *  The Default size of a YFile about 64MB, not sure, mainly base on the input list size
 **/
