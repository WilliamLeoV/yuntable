#include "utils.h"
#include "list.h"
#include "item.h"
#include "wal.h"
#include "malloc2.h"
#include "log.h"

/**
 * The Mechanism of wal:
 *    1. In the runtime, will collect the updated items from all the tablets.
 *    2. Once the memstore flushed to the yfile, the legacy log will be marked as flushed.
 *    3. During the system startup, the wal will load the unflushed data to all the tablet.
 *    4. Sometime, the log will refresh the
 **/

struct _Wal{
	byte magic[8];
	char *wal_file_path; /**used to locate the wal file **/
};

struct _WalItem{
	short tablet_id;
	long item_id;
	Item* item;
};

#define WAL_MAGIC_HEADER "WALMAGIC"

#define TMP_WAL_FILE_PATH "temp_wal.log"

public short get_tablet_id_wal_item(WalItem* walItem){
		return walItem->tablet_id;
}

public long get_item_id_wal_item(WalItem* walItem){
		return walItem->item_id;
}

public Item* get_item_wal_item(WalItem* walItem){
		return walItem->item;
}

public void free_wal_item_void(void* walItem){
		free2(walItem);
}

private void flush_wal_item(WalItem *walItem, FILE *fp){
		fwrite(&walItem->tablet_id, sizeof(walItem->tablet_id), 1, fp);
		fwrite(&walItem->item_id, sizeof(walItem->item_id), 1, fp);
		flush_item(walItem->item, fp);
}

public void append_wal_item(Wal *wal, WalItem* walItem){
		FILE *fp = fopen(wal->wal_file_path, "a");
		flush_wal_item(walItem, fp);
		fclose(fp);
}

public WalItem* create_wal_item(short tablet_id, long item_id, Item* item){
		WalItem *walItem = malloc2(sizeof(WalItem));
		walItem->tablet_id = tablet_id;
		walItem->item_id = item_id;
		walItem->item = item;
		return walItem;
}

private WalItem* load_wal_item(FILE* fp){
		short tablet_id = 0;
		fread(&tablet_id, sizeof(tablet_id), 1, fp);
		long item_id = 0;
		fread(&item_id, sizeof(item_id), 1, fp);
		Item* item = m_load_item(fp);
		return create_wal_item(tablet_id, item_id, item);
}

public Wal* load_wal(char* file_path){
		Wal *wal = malloc2(sizeof(Wal));
		cpy(wal->magic, WAL_MAGIC_HEADER);
		wal->wal_file_path = m_cpy(file_path);
		//if the file not exist, create file
		if(file_exist(wal->wal_file_path) == false){
			refresh_wal(wal, NULL);
		}
		return wal;
}

public boolean need_to_reload_wal(Wal *wal){
		int file_size = get_file_size(wal->wal_file_path);
		if(file_size <= sizeof(wal->magic)) return false;
		else return true;
}

public void refresh_wal(Wal *wal, List* walItems){
		char* tmp_file_path = TMP_WAL_FILE_PATH;
		logg(INFO, "creating a tmp wal file %s.", tmp_file_path);
		create_or_rewrite_file(tmp_file_path, WAL_MAGIC_HEADER);
		FILE *fp = fopen(tmp_file_path, "a");
		if(walItems != NULL){
			WalItem* walItem = NULL;
			while((walItem = list_next(walItems)) != NULL){
				flush_wal_item(walItem, fp);
			}
		}
		fclose(fp);
		logg(INFO, "replace the wal file with tmp wal file.", tmp_file_path);
		//TODO handle the failure situation
		unlink(wal->wal_file_path);
		rename(tmp_file_path, wal->wal_file_path);
}

public List* load_log_wal(Wal *wal){
		int file_size = get_file_size(wal->wal_file_path);
		List *logList = list_create();
		FILE *fp = fopen(wal->wal_file_path, "r");
		fseek(fp, sizeof(wal->magic), SEEK_SET);
		while(ftell(fp) != file_size){
			WalItem *walItem = load_wal_item(fp);
			//TODO may have some performance issues
			list_append(logList, walItem);
		}
		list_rewind(logList);
		return logList;
}
