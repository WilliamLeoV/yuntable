#include "global.h"
#include "list.h"
#include "utils.h"

public void* mallocs(int size){
	return calloc(1, size+1);
}

#define BEGIN_BUF_SIZE 1000

#define GROWTH_FACTOR 2

/**  serializing and deserializing methods **/
struct _Buf{
	byte* data;
	int index; /** the pointer of data, not the size of data, its usage will be varied base on the method **/
	int allocated_size; /* the size has been allocated */
};

public byte* get_buf_data(Buf *buf){
	return buf->data;
}

public int get_buf_index(Buf *buf){
	return buf->index;
}

public Buf* init_buf(){
	Buf *buf = malloc(sizeof(Buf));
	buf->index = 0;
	buf->allocated_size = BEGIN_BUF_SIZE;
	buf->data = calloc(1, buf->allocated_size);
	return buf;
}

public void enlarge_buf(Buf *buf, int size){
	int required_len = buf->index + size;
	if(buf->allocated_size < required_len){
		int target_len = buf->allocated_size * GROWTH_FACTOR;
		if(target_len < required_len) target_len = required_len * GROWTH_FACTOR;
		buf->allocated_size = target_len;
		buf->data = realloc(buf->data, buf->allocated_size);
	}
}

/* the container for importing the current data sets  */
public Buf* create_buf(int index, byte* data){
	Buf *buf = malloc(sizeof(Buf));
	buf->index = index;
	buf->allocated_size = 0;
	buf->data = data;
	return buf;
}

public void buf_cat(Buf *buf, void* src, int size){
	enlarge_buf(buf, size);
	void* tmp_dest = buf->data;
	int dest_begin_index = buf->index;
	//locate the dest begin index
	while(dest_begin_index--!=0) tmp_dest++;
	memcpy(tmp_dest, src, size);
	buf->index += size;
}

public void buf_combine(Buf* dest_buf, Buf* src_buf){
	enlarge_buf(dest_buf, src_buf->index);
	void* tmp_dest = dest_buf->data;
	int dest_begin_index = dest_buf->index;
	//locate the dest begin index
	while(dest_begin_index--!=0) tmp_dest++;
	memcpy(tmp_dest, src_buf->data, src_buf->index);
	dest_buf->index += src_buf->index;
}

public void* buf_load(Buf* buf, int size){
	//add 1 for string
	void *dest = mallocs(size);
	void *tmp_src = buf->data;
	tmp_src = move_pointer(tmp_src, buf->index);
	memcpy(dest, tmp_src, size);
	buf->index += size;
	return dest;
}

public int buf_load_int(Buf* buf){
	void* string = buf_load(buf, sizeof(int));
	int integer = btoi(string);
	free(string);
	return integer;
}

public long long buf_load_long_long(Buf* buf){
	void* string = buf_load(buf, sizeof(long long));
	long long longlong = btoll(string);
	free(string);
	return longlong;
}

public short buf_load_short(Buf* buf){
	void* string = buf_load(buf, sizeof(short));
	short short_int = btos(string);
	free(string);
	return short_int;
}

public void destory_buf(Buf* buf){
	if(buf != NULL){
		free(buf->data);
		free(buf);
	}
}

public void free_buf(Buf* buf){
	if(buf != NULL) free(buf);
}

#define LOG_PREFIX "log:"

public void logg(char *format, void *p){
#ifndef DISABLE_LOGGING
	printf(LOG_PREFIX);
	printf(format, p);
#endif
}

/**   string normal operation part  **/

public char* move_pointer(char* pointer, int div){
	int i=0;
	for(i=0; i<div; i++) pointer++;
	return pointer;
}

public char* cat(char* dest, char* src){
	return strncat(dest, src, strlen(src));
}

public char* m_cats(int size, ...){
	//clean dest
	char *dest = calloc(1, 10);
	va_list ap;
	int i=0;

	va_start(ap, size);
	for(i=0; i<size; i++){
		//this syntax error is a eclipse bug
		char *str = va_arg(ap, char *);
		int len = strlen(dest) + strlen(str);
		dest = realloc(dest, len + 1);
		cat(dest, str);
		dest[len] = '\0';
	}
	return dest;
}

/** need to make sure the size of dest is bigger than the size of src + 1 **/
public char* cpy(char* dest, char* src){
	memset(dest, 0, strlen(src)+1);
	return strncpy(dest, src, strlen(src));
}

public char* m_cpy(char *src){
	char *s = mallocs(strlen(src));
	cpy(s, src);
	return s;
}

public boolean cmp(char* dest, char* src, int len){
	boolean result = false;
	if(strncasecmp(dest, src, len)==0) result = true;
	return result;
}

public boolean match(char* dest, char *src){
	int max_len = max(strlen(dest), strlen(src));
	return cmp(dest, src, max_len);
}

public boolean match_for_list_find(void* dest, void *src){
	char* tmp_dest = (char*)dest;
	char* tmp_src = (char*)src;
	int max_len = max(strlen(tmp_dest), strlen(tmp_src));
	return cmp(tmp_dest, tmp_src, max_len);
}

public boolean match_tail(char* dest, char *src){
	//TODO if the dest has two set of chars like src, which will cause some problems
	char* tmp_dest = dest;
	tmp_dest = strstr(tmp_dest, src);
	if(tmp_dest == NULL) return false;
	//check if the tmp_dest is at the tail
	tmp_dest = move_pointer(tmp_dest, strlen(src));
	if( *tmp_dest == '\0') return true;
	else return false;
}

/** count the occurrence of target inside the string **/
public int count(char *string, char target){
	int occurrence = 0;
	while(*string != '\0'){
		if(*string == target) occurrence++;
		string++;
	}
	return occurrence;
}

/** conversion methods **/

/** integer to string **/
public char* m_itos(int num){
	char *str = calloc(1, 20);
	sprintf(str, "%d", num);
	return str;
}

/** long long to string **/
public char* m_lltos(long long num){
	char *str = calloc(1, 50);
	sprintf(str, "%lld", num);
	return str;
}

/** char to string **/
public char* m_ctos(char chr){
	char *str = calloc(1, 1);
	sprintf(str, "%c", chr);
	return str;
}

/** string to boolean **/
public boolean stob(char* bool_str){
	if(match(bool_str, true_str)){
		return true;
	}else return false;
}

/** byte to integer **/
public int btoi(byte *b){
	return *((int *)b);
}

/** byte to long long **/
public long long btoll(byte *b){
	return *((long long *)b);
}

/** byte to short **/
public short btos(byte *b){
	return *((short *)b);
}

public char* trim(char *str, char deli){
	//remove left side
	while(*str == deli){
		str++;
	}
	//remove right side
	char* tmp = str;
	while(*tmp != '\0')tmp++;
	tmp--;

	while(*tmp == deli){
		*tmp = '\0';
		tmp--;
	}
	return str;
}

/**   string tokens operation part  **/

/** this method will remove  the token which is empty or just have a lot of space **/
private Tokens* clean_tokens(Tokens* tokens){
	int i=0;
	for(i=0; i<tokens->size; i++){
		trim(tokens->tokens[i], ' ');
		if(strlen(tokens->tokens[i]) == 0){
			tokens->size--;
			free(tokens->tokens[i]);
			if(i<tokens->size - 1)
				tokens->tokens[i] = tokens->tokens[i+1];
		}
	}
	return tokens;
}

public Tokens* init_tokens(char *str, char deli){
	Tokens *tokens = malloc(sizeof(Tokens));
	tokens->size = 0;
	if(str == NULL || strlen(str) == 0) return tokens;
	char *tmp = str;
	int i = 0, len = strlen(str);
	int max_times = count(tmp, deli) + 1;
	tokens->tokens = malloc(max_times * sizeof(char*));
	tokens->tokens[0] = mallocs(len);
	boolean prev_matched = false;
	while(*tmp != '\0'){
		if(*tmp == deli){
			if(!prev_matched){
				i++;
				tokens->tokens[i] = mallocs(len);
			}
			prev_matched = true;
		}else{
			prev_matched = false;
			strncat(tokens->tokens[i],tmp,1);
		}
		tmp++;
	}
	tokens->size = i + 1 ;
	return clean_tokens(tokens);
}

public void free_tokens(Tokens* tokens){
	int i=0;
	for(i=0; i<tokens->size; i++){
		free(tokens->tokens[i]);
	}
	free(tokens);
}

public List* generate_list_by_token(char* bytes, char token){
	List* list = list_create();
	Tokens* tokens = init_tokens(bytes, token);
	int i=0;
	for(i=0; i<tokens->size; i++){
		if(strlen(tokens->tokens[i]) == 0) continue;
		char* item = m_cpy(tokens->tokens[i]);
		list_append(list, item);
	}
	free_tokens(tokens);
	return list;
}

public char* list_to_string_by_token(List* list, char token){
	Buf* buf = init_buf();
	char *token_string = m_ctos(token);
	int size = list_size(list);
	int i=0;
	for(i=0; i<size; i++){
		char *value = list_next(list);
		buf_cat(buf, value, strlen(value));
		buf_cat(buf, token_string, strlen(token_string));
	}
	char* result = get_buf_data(buf);
	trim(result, token);
	free(token_string);
	list_rewind(list);
	free_buf(buf);
	return result;
}

/** other methods **/
public int max(int a, int b){
	int max = a > b? a : b;
	return max;
}

/**   I/O operation part  **/
public char* m_load_file_to_memory(char *file_path){
	int file_size = get_file_size(file_path);
	if(file_size == 0) return NULL;
	char *buf = mallocs(file_size);
	FILE *fp = fopen(file_path, "r");
	fread(buf, file_size,1, fp);
	fclose(fp);
	return buf;
}

public boolean create_or_rewrite_file(char *filename, char* content){
	FILE *fp = fopen(filename, "w");
	if (fp == NULL) {
		printf("error:can not create file");
		return false;
	}
	fwrite(content, strlen(content), 1, fp);
	fclose(fp);
	return true;
}

/** if the method return -1, means the file not exist**/
public int get_file_size(char *file_path){
	FILE *fp = fopen(file_path, "r");
	if(fp == NULL) return -1;
	fseek(fp,0,SEEK_END);
	int size = ftell(fp);
	fclose(fp);
	return size;
}

public List* get_files_path_by_ext(char *folder_path, char *ext){
	List *fileList = list_create();
	DIR *dir = opendir(folder_path);
	struct dirent *dp;
	while ((dp = readdir(dir)) != NULL) {
		if(match_tail(dp->d_name, ext)){
			char *file_path = m_cats(3, folder_path, FOLDER_SEPARATOR_STRING, dp->d_name);
			list_append(fileList, file_path);
		}
	}
	closedir(dir);
	return fileList;
}

public char* m_get_file_name_by_ext(char* folder, char* ext){
	List *files = get_files_path_by_ext(folder, ext);
	Tokens* tokens = init_tokens(get_full_file_name(list_get(files, 0)), MID_SEPARATOR);
	char* file_name = m_cpy(tokens->tokens[0]);
	free_tokens(tokens);
	return file_name;
}

/** will the file name with ext **/
public char* get_full_file_name(char* file_path){
	char* file_name = file_path;
	char* tmp = file_name;
	while( *tmp++ != '\0'){
		if(*tmp ==  FOLDER_SEPARATOR)
			file_name = tmp + 1;
	}
	return file_name;
}

public List* get_lines_from_file(char* file_path){
	char* file_content = m_load_file_to_memory(file_path);
	List* lines = generate_list_by_token(file_content, LINE_SEPARATOR);
	free(file_content);
	return lines;
}

public List* get_lines_from_file_base_on_prefix(char* file_path, char* prefix){
	List* lines = get_lines_from_file(file_path);
	List* newLines = list_create();
	char* line = NULL;
	while((line = list_next(lines)) != NULL){
		if(cmp(line, prefix, strlen(prefix)))
			list_append(newLines, m_cpy(line));
	}
	list_destory(lines, just_free);
	return newLines;
}

/** conn string part **/
public char* m_get_ip_address(char* connection_string){
	char* ip_address = NULL;
	Tokens* tokens = init_tokens(connection_string, ':');
	ip_address = m_cpy(tokens->tokens[0]);
	free_tokens(tokens);
	return ip_address;
}

public int get_port(char* connection_string){
	Tokens* tokens = init_tokens(connection_string, ':');
	int port = atoi(tokens->tokens[1]);
	free_tokens(tokens);
	return port;
}

public List* string_to_list(char* list_string){
	return generate_list_by_token(list_string, STRING_SEPARATOR);
}

public char* list_to_string(List* list){
	return list_to_string_by_token(list, STRING_SEPARATOR);
}

/** return should be between 0 to 100**/
public int generate_random_int(){
	struct timeval tv;
	struct timezone tz;
	gettimeofday (&tv , &tz);
	srand(tv.tv_usec);
	int percent = rand() % 100;
	return percent;
}

/* should get timestamp base on micro seconds */
public long long get_current_time_mills(){
	struct timeval t;
	gettimeofday (&t, NULL);
	long long time_mills = (long long)t.tv_sec * 1000;
	time_mills +=(long long) t.tv_usec/1000;
	return time_mills;
}

#ifdef UTILS_TEST
#include <pthread.h>
void test_long_to_string(void){
	char* string = "123456";
	long value = strtol(string, NULL, 10);
	char* result = m_lltos(value);
	assert(match(string, result));
}

pthread_t fresh_thread_id;

void* test_thread(void* test){
	pid_t      pid;
	pthread_t  tid;

	pid = getpid();
	tid = pthread_self();
	while(1){
		sleep(5);
		printf("%d\n", (int)time(0));
		printf("pid %u tid %u (0x%x)\n", (unsigned int)pid,
			   (unsigned int)tid, (unsigned int)tid);
	}
}

void testcase_pthread(void){
	pthread_create(&fresh_thread_id, NULL, test_thread, (void*)NULL);
	while(1){
	}
}

void testcase_for_generate_list_by_token(void){
	char *test = "test1,test2,test3";
	List* list = generate_list_by_token(test, ',');
	printf("%d\n", list_size(list));
	char *str;
	while((str = list_next(list)) != NULL){
		printf("%s\n", str);
	}
	list_rewind(list);
	char* line = list_to_string_by_token(list, ',');
	printf("the string after recover %s\n", line);
	free(line);
	list_destory(list, just_free);
}

void testcase_for_get_lines_from_file(void){
	char* file_path = "conf/master.conf";
	List* lines = get_lines_from_file(file_path);
	int i=0, index = list_size(lines);
	for(i=0; i<index; i++){
		printf("%s\n", (char*)list_next(lines));
	}
	list_destory(lines, just_free);
}

void testcase_for_ip_port(void){
	char* test = "127.0.0.1:8301";
	char* ip_address = m_get_ip_address(test);
	int port = get_port(test);
	printf("ip address:%s\n", ip_address);
	printf("port:%d\n", port);
}

void testcase_for_m_itos(void){
	int test = 1237129083;
	char* ret = m_itos(test);
	printf("%s\n", ret);
	free(ret);
}

void testcase_for_token_operations(void){
	char* test = "insert row:me name:ike sex:male";
	Tokens* thiz = init_tokens(test,' ');
	printf("--%d---\n", thiz->size);

	int i=0;
	for(i=0; i<thiz->size; i++){
		printf("---%d--%s---\n",i, thiz->tokens[i]);
	}
	free_tokens(thiz);
}

void testcase_for_get_files_by_ext(void){
	char *folder_path = "util";
	char *cext = ".c";
	char *hext = ".h";
	List* clist = get_files_path_by_ext(folder_path, cext);
	printf("%d\n",list_size(clist));
	List* hlist = get_files_path_by_ext(folder_path, hext);
	printf("%d\n",list_size(hlist));
}

void testcase_for_match_tail(void){
	char *dest = "people.table";
	char *src1 = ".table";
	char *src2 = ".yfile";
	printf("%s\n", bool_to_str(match_tail(dest, src1)));
	printf("%s\n", bool_to_str(match_tail(dest, src2)));
}

void testcase_for_match(void){
	char *test1 = "address";
	char *test2 = "address2";
	printf("the result is:%s\n", bool_to_str(match(test1, test2)));
}

void testcase_for_m_cats(void){
	char *test = m_cats(3, "datastore", "/", "default_cf");
	printf("the result is:%s-\n", test);
}

void testcase_for_m_cpy(void){
	char *src = "hello world";
	char *dest = m_cpy(src);
	printf("%s\n",dest);
	free(dest);
}

void testcase_for_logg(void){
	//char *str = "world";
	logg("hello\n", NULL);
}

void testcase_for_times(void){
	char *string = "insert  row:me   name:ike  sex:male";
	printf("The number of occurrence:%d\n", count(string, ':'));
}

void testcase_for_cmp(void){
	char *dest = "HELlo WorLD";
	char *src = "heLlo WorLD";
	boolean result = cmp(dest, src, strlen(src));
	printf("--%s--\n",bool_to_str(result));
}

void testcase_for_trim(void){
	char *str = "\"male\"";
	char *dest = mallocs(strlen(str));
	cpy(dest, str);
	dest = trim(dest, '"');
	printf("the string after trim:%s--\n",dest);
}

void testcase_for_get_file_size(void){
	char *file1 = "test";
	char *file2 = "yuntable.yfile";
	char *file3 = "nothing";
	printf("The size of %s is %d\n", file1, get_file_size(file1));
	printf("The size of %s is %d\n", file2, get_file_size(file2));
	printf("The size of %s is %d\n", file3, get_file_size(file3));
}

void testcase_for_fwrite_int(void){
	FILE *fp = fopen("test","w");
	int i = 1;
	fwrite(&i, sizeof(i), 1, fp);
	fclose(fp);
}

void testcase_for_remove_pointer(void){
	char* tablet_name = "tablet1";
	printf("%s\n", move_pointer(tablet_name, strlen("tablet")));
	printf("%s\n", tablet_name);
}
#include "msg.h"

char* return_msg(void){
	return ISSUE_MSG_NOTHING_FOUND;
}

char* return_msg2(void){
	return return_msg();
}

void testcase_for_msg(void){
	char* msg = return_msg2();
	printf("%s\n", msg);
}

void testcase_for_list_to_string(void){
	List* list = list_create();
	list_append(list, m_cpy("test"));
	char* value = list_to_string(list);
	printf("value:%s\n", value);
	free(value);
	printf("the list after free:%s\n", (char*)list_next(list));
}

#define BEGIN_ALLOCATED_SIZE 500000
#define END_ALLOCATED_SIZE 640000

void testcase_for_generate_random_int(void){
	printf("%d\n", generate_random_int());
}

void testcase_for_get_current_time_mills(void){
	printf("%lld\n", get_current_time_mills());
}

void test_suite(void){
	printf("The Test Suit of utils is starting\n");
	printf("0) test_long_to_string\n");
	test_long_to_string();
	printf("1) testcase_for_generate_list_by_token\n");
	testcase_for_generate_list_by_token();
	printf("2) testcase_for_get_lines_from_file\n");
	testcase_for_get_lines_from_file();
	printf("3) testcase_for_ip_port\n");
	testcase_for_ip_port();
	printf("4) testcase_for_m_itos\n");
	testcase_for_m_itos();
	printf("5) testcase_for_token_operations\n");
	testcase_for_token_operations();
	printf("6) testcase_for_get_files_by_ext\n");
	testcase_for_get_files_by_ext();
	printf("7) testcase_for_match_tail\n");
	testcase_for_match_tail();
	printf("8) testcase_for_match\n");
	testcase_for_match();
	printf("9) testcase_for_m_cats\n");
	testcase_for_m_cats();
	printf("10) testcase_for_m_cpy\n");
	testcase_for_m_cpy();
	printf("11) testcase_for_logg\n");
	testcase_for_logg();
	printf("12) testcase_for_times\n");
	testcase_for_times();
	printf("13) testcase_for_cmp\n");
	testcase_for_cmp();
	printf("14) testcase_for_trim\n");
	testcase_for_trim();
	printf("15) testcase_for_get_file_size\n");
	testcase_for_get_file_size();
	printf("16) testcase_for_fwrite_int\n");
	testcase_for_fwrite_int();
	printf("17) testcase_for_remove_pointer\n");
	testcase_for_remove_pointer();
	printf("18) testcase_for_msg\n");
	testcase_for_msg();
	printf("19) testcase_for_list_to_string\n");
	testcase_for_list_to_string();
	printf("20) testcase_for_generate_random_int\n");
	testcase_for_generate_random_int();
	printf("21) testcase_get_current_time_mills\n");
	testcase_for_get_current_time_mills();
	printf("Completed Successfully\n");
}

int main(int argc, char *argv[]){
	test_suite();
	return 1;
}
#endif
