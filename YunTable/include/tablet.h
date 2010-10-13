#ifndef TABLET_H_
#define TABLET_H_

#include "global.h"
#include "item.h"

#define DEFAULT_DATASTORE_FILE_FOLDER "tablet"

typedef struct _Tablet Tablet;

public short get_tablet_id(Tablet* tablet);

public void set_last_flushed_id(Tablet* tablet, long last_flushed_id);

public long get_last_flushed_id(Tablet* tablet);

public char* get_tablet_folder(Tablet* tablet);

public Tablet* load_tablet(char *tablet_folder);

/** this method will create the tablet folder, table name file and wal file**/
public Tablet* create_tablet(int tablet_id, char *tablet_folder, char *table_name, char* column_family);

public char* get_tablet_folder(Tablet *tablet);

public boolean match_tablet(Tablet *tablet, char* table_name, char* column_family);

public boolean match_tablet_by_table_name(Tablet *tablet, char* table_name);

public char* get_column_family_by_tablet(Tablet *tablet);

public void put_tablet(Tablet *tablet, long incr_item_id, Item *item);

public ResultSet* query_tablet_row_key(Tablet *tablet, char* row_key);

public  ResultSet* query_tablet_by_timestamp(Tablet *tablet, int begin_timestamp, int end_timestamp);

public boolean need_to_flush_tablet(Tablet *tablet);

public void flush_tablet(Tablet *tablet);

public int get_used_size_tablet(Tablet *tablet);

public int get_free_size_tablet(Tablet *tablet);

#endif /* TABLET_H_ */
