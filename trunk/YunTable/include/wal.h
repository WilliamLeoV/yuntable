#ifndef WAL_H_
#define WAL_H_

#include "global.h"
#include "list.h"
#include "item.h"

/**
 * The Mechanism of wal:
 *    1. In the runtime, will collect the updated items from all the tablets.
 *    2. Once the memstore flushed to the yfile, the legacy log will be marked as flushed.
 *    3. During the system startup, the wal will load the unflushed data to all the tablet.
 *    4. Sometime, the log will refresh the
 *
 **/

typedef struct _Wal Wal;

typedef struct _WalItem WalItem;

public short get_tablet_id_wal_item(WalItem* walItem);

public long get_item_id_wal_item(WalItem* walItem);

public Item* get_item_wal_item(WalItem* walItem);

public void free_wal_item_void(void* walItem);

public Wal* load_wal(char* file_path);

public void append_wal_item(Wal *wal, WalItem* walItem);

public WalItem* create_wal_item(short tablet_id, long item_id,Item *item);

public void reset_log_wal(Wal *wal);

public boolean need_to_reload_wal(Wal *wal);

public List* load_log_wal(Wal *wal);

public void refresh_wal(Wal *wal, List* walItems);

#endif /* WAL_H_ */
