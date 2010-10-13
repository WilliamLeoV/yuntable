#ifndef UTILS_H
#define UTILS_H

#ifdef __cplusplus
extern "c" {
#endif

#include "global.h"
#include "list.h"

public void* mallocs(int size);

/**  serializing and deserializing methods **/
typedef struct _Buf Buf;

public byte* get_buf_data(Buf *buf);

public int get_buf_index(Buf *buf);

public Buf* init_buf();

public Buf* create_buf(int size, byte* data);

public void buf_cat(Buf *buf, void* src, int size);

public void buf_combine(Buf* dest_buf, Buf* src_buf);

public void* buf_load(Buf* buf, int size);

public int buf_load_int(Buf* buf, int size);

public short buf_load_short(Buf* buf, int size);

public void destory_buf(Buf* buf);

public void free_buf(Buf* buf);

/**   log operation part  **/

public void logg(char *format, void *p);

/**   string normal operation part  **/

/** the safe implmenetaion of strcat **/
public char* move_pointer(char* pointer, int div);

public char* cat(char* dest, char* src);

/**allocate a memory chunk and concat multiple strings**/
public char* m_cats(int size, ...);

/** the safe implmenetaion of strcpy, need to make sure
 * the size of dest is bigger than the size of src + 1 **/
public char* cpy(char* dest, char* src);

/** will create a memory chunk, before cpy, and needs to be freed**/
public char* m_cpy(char *src);

public char* trim(char *str, char deli);

public char* m_concat_strings(int begin_index, int end_index, char *strings[]);

/** conversion methods **/

/** integer to string **/
public char* m_itos(int num);

/** long to string **/
public char* m_ltos(long num);

/** char to string **/
public char* m_ctos(char chr);

/** string to boolean **/
public boolean stob(char* bool_str);

/** byte to integer **/
public int btoi(byte *b);

/** byte to short **/
public short btos(byte *b);

/** the simple implmenetaion for strncmp, and ignore case **/
public boolean cmp(char* dest, char* src, int len);

/** the safe version of cmp,  will used max len between the two strings **/
public boolean match(char* dest, char *src);

/** the match for list find **/
public boolean match_for_list_find(void* dest, void *src);

/** if the tail of dest is as same as src**/
public boolean match_tail(char* dest, char *src);

/** count the occurrence of target inside the string **/
public int count(char *string, char target);

/**   string tokens operation part  **/

typedef struct _Tokens{
	int size;
	char** tokens;
}Tokens;

/*
 * sample str: name, sex,  home_address, work_address
 * sample deli: , may have some issue on deli
 */
public Tokens* init_tokens(char *str, char deli);

/*
 * since the tokens has a nested array, it needs a method to free
 */
public void free_tokens(Tokens* tokens);

public List* generate_list_by_token(char* buf, char token);

public char* list_to_string_by_token(List* list, char token);

/** other memthods **/

public int max(int a, int b);

/**   I/O operation part  **/

/** if returns 0, that means the file not exist**/
public char* m_load_file_to_memory(char *file_path);

public boolean create_or_rewrite_file(char *file_path, char* content);

/** if the method return -1, means the file not exist**/
public int get_file_size(char *file_path);

public List* get_files_path_by_ext(char *folder_path, char *ext);

/** will get file name before  first '.' with first file with this ext **/
public char* m_get_file_name_by_ext(char* folder, char* ext);

/** will the file name with ext **/
public char* get_full_file_name(char* file_path);

public List* get_lines_from_file_base_on_prefix(char* file_path, char* prefix);

public List* get_lines_from_file(char* file_path);

/** connection string part**/

public char* m_get_ip_address(char* connection_string);

public int get_port(char* connection_string);

/** only allows character list **/
public List* string_to_list(char* string);

/** only allows character list **/
public char* list_to_string(List* list);

/** return should be between 0 to 100**/
public int generate_random_int();

/* should get timestamp base on micro seconds */
public long long get_current_time_mills();

#ifdef __cplusplus
}
#endif

#endif /* UTILS_H_ */

