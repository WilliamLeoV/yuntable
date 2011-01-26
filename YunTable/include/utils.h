#ifndef UTILS_H
#define UTILS_H

#ifdef __cplusplus
extern "c" {
#endif

#include "global.h"
#include "list.h"

/**   string normal operation part  **/

public char* move_pointer(char* pointer, int div);

public char* cat(char* dest, char* src);

public char* m_cats(int size, ...);

public char* cpy(char* dest, char* src);

public char* m_cpy(char *src);

public char* trim(char *str, char deli);

/** conversion methods **/

public char* m_itos(int num);

public long long btoll(byte *b);

public int btoi(byte *b);

public short btos(byte *b);

public char* m_lltos(long long num);

public char* m_ctos(char chr);

public boolean stob(char* bool_str);

public short btos(byte *b);

/** other methods **/

public int max(int a, int b);

public boolean cmp(char* dest, char* src, int len);

public boolean match(char* dest, char *src);

public int match_int(char* dest, char* src);

public boolean match_for_list_find(void* dest, void *src);

public boolean match_tail(char* dest, char *src);

public int count(char *string, char target);

/**   string tokens operation part  **/

typedef struct _Tokens{
	int size;
	char** tokens;
}Tokens;

public Tokens* init_tokens(char *str, char deli);

public void free_tokens(Tokens* tokens);

public List* generate_list_by_token(char* buf, char token);

public char* list_to_string_by_token(List* list, char token);

/**   I/O operation part  **/

public char* m_load_txt_file_to_memory(char *file_path);

public void create_or_rewrite_file(char *file_path, char* content);

public boolean file_exist(char* file_path);

public int get_file_size(char *file_path);

public List* get_files_path_by_ext(char *folder_path, char *ext);

public char* m_get_file_name_by_ext(char* folder, char* ext);

public char* get_full_file_name(char* file_path);

public List* get_lines_from_text_file_base_on_prefix(char* file_path, char* prefix);

public List* get_lines_from_text_file(char* file_path);

/** connection string part**/

public char* m_get_ip_address(char* connection_string);

public int get_port(char* connection_string);

public List* string_to_list(char* string);

public char* list_to_string(List* list);

public char* array_to_string(char** array, int begin_index, int end_index);

public int generate_random_int();

public long long get_current_time_stamp();

public int get_local_partition_free_space();

#ifdef __cplusplus
}
#endif

#endif /* UTILS_H_ */

