/**
 * Copyright 2011 Wu Zhu Hua, Xi Ming Gao, Xue Ying Fei, Li Jun Long
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

#include "list.h"
#include "utils.h"
#include "malloc2.h"
#include "log.h"
#include "buf.h"
#include "msg.h"

/**   string normal operation part  **/


static char *err_str[] = {
    /* SUCCESS              */    "success",
    /* ERR_NO_CMD_INPUT     */    "Please attach cmd string if you want to use the silent mode.",
    /* ERR_CMD_NOT_COMPLETE */    "Please input the complete cmd.",
    /* ERR_NULL_STRING      */    "The input string is NULL or invalid.",
    /* ERR_WRONG_ACTION     */    "The action you just input is not supported. Please type \"help\" for more information.",
    /* ERR_NO_COLUMN        */    "No column has been inputed.",
    /* ERR_NO_TABLE_NAME    */    "No table name has been inputed.",
    /* ERR_NO_ROW_KEY       */    "Should have a row key.",
    /* ERR_PUT              */    "The Put Data action has failed.",
    /* ERR_TABLE_NOT_CREATE */    "Please create the table first.",
    /* ERR_NO_MASTER        */    "Please set up a master at first",
    /* ERR_CLUSTER_FULL     */    "Can not get a new region for this table because the whole cluster is full.",
    /* ERR_WRONG_MASTER     */    "Either the inputted master is wrong or has some connection problem.",
    /* ERR_WRONG_REGION     */    "Either the inputted region is wrong, has some connection problem or already existed.",
    /* ERR_WRONG_ADD_CMD    */    "The Add Command is not valid, for example, the table name contains space",
    /* ERR_NOTHING_FOUND    */    "Nothing has been found.",
    /* ERR_EXIST            */    "The table has already existed.",
    /* ERR_NO_EXIST         */    "The entry does not exist",
    /* ERR_INVAL            */    "invalid value",
};

char *err_to_str(int err)
{
    static char err_buf[128];
    if (err < 0 || err >= ERR_MAX) {
        snprintf(err_buf, sizeof(err_buf), "unknown error, errno = %d", err);
        return err_buf;
    }
    return err_str[err];
}

/** the safe implmenetaion of strcat **/
public char* move_pointer(char* pointer, int div){
		char* temp = pointer;
        int i=0;
        for(i=0; i<div; i++) temp++;
        return temp;
}


/**allocate a memory chunk and concat multiple strings**/
public char* m_cats(int size, ...){
        //clean dest
        char *dest = mallocs(10);
        va_list ap;
        int i=0;

        va_start(ap, size);
        for(i=0; i<size; i++){
			//this syntax error is a eclipse bug
			char *str = va_arg(ap, char *);
			int len = strlen(dest) + strlen(str);
			dest = realloc2(dest, len + 1);
			strcat(dest, str);
			dest[len] = '\0';
        }
        return dest;
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

/** other methods **/

public int max(int a, int b){
        int max = a > b ? a : b;
        return max;
}

/** the simple implmenetaion for strncmp, and ignore case **/
public boolean str_n_match(const char* dest, const char* src, int len)
{
    return !strncasecmp(dest, src, len);
}

/** the safe version of cmp,  will used max len between the two strings and ignore case **/
public boolean match(const char* dest, const char *src)
{
    return !strcasecmp(dest, src);
}

/**The match method for returning int compare result**/
public int match_int(char* dest, char* src){
		int max_len = max(strlen(dest), strlen(src));
		return strncasecmp(dest, src, max_len);
}

/** the match method for list find **/
public boolean match_for_list_find(void* dest, void *src){
        char* tmp_dest = (char*)dest;
        char* tmp_src = (char*)src;
        return match(tmp_dest, tmp_src);
}

/** if the tail of dest is as same as src **/
public boolean match_tail(char* dest, char *src){
        char* tmp_dest = dest;
        char* ptr = NULL;
        //find the rightmost match string
        while(1){
        	tmp_dest = strstr(tmp_dest, src);
        	if(tmp_dest != NULL){
            	ptr = tmp_dest;
            	tmp_dest++;
        	}else{
        		break;
        	}
        }
        if(ptr == NULL) return false;
        //check if the tmp_dest is at the tail
        ptr = move_pointer(ptr, strlen(src));
        if( *ptr == '\0') return true;
        else return false;
}

/** count the occurrence of target char inside the string **/
public int count(char *string, char target){
        int occurrence = 0;
        while(*string != '\0'){
                if(*string == target) occurrence++;
                string++;
        }
        return occurrence;
}

/** conversion methods **/
/** byte and char are very different, need to make sure to use the right to handle it **/

/** integer to string **/
public char* m_itos(int num){
        char *str = mallocs(20);
        sprintf(str, "%d", num);
        return str;
}

/** long long to string **/
public char* m_lltos(long long num){
        char *str = mallocs(50);
        sprintf(str, "%lld", num);
        return str;
}

/** char to string **/
public char* m_ctos(char chr){
        char *str = mallocs(1);
        sprintf(str, "%c", chr);
        return str;
}

/** string to boolean **/
public boolean stob(char* bool_str){
        if(match(bool_str, true_str)){
			return true;
        }else return false;
}

/** byte to integer, is necessary when loading integer from bytes **/
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

/**   string tokens operation part  **/

/*
 * sample str: name, sex,  home_address, work_address
 * sample deli: , may have some issue on deli
 */
public Tokens* init_tokens(char *str, char deli)
{
#define DEF_TOKEN_NUM 5
    int alloc_num = DEF_TOKEN_NUM;
    Tokens *tokens = malloc2(sizeof(Tokens));
    tokens->size = 0;
    tokens->tokens = malloc2(alloc_num * sizeof(char*));
    if (str == NULL || strlen(str) == 0) {
        return tokens;
    }

    while (*str == ' ' || *str == '\t' || *str == deli) {
        str++;
    }
    int tnum = 0;
    while (*str != '\0') {
        char *start = str;
        while (*str != '\0' && *str != deli) {
            str++;
        }
        int len = str - start;
        char *token = malloc2(len + 1);
        strncpy(token, start, len);
        token[len] = '\0';
        tokens->tokens[tnum++] = token;
        if (tnum >= alloc_num) {
            alloc_num *= 2;
            tokens->tokens = realloc2(tokens->tokens, sizeof(char*) * alloc_num);
        }
        while (*str == ' ' || *str == '\t' || *str == deli) {
            str++;
        }
    }
    tokens->size = tnum ;
    return tokens;
}



/*
 * since the tokens has a nested array, it needs a method to free
 */
public void free_tokens(Tokens* tokens){
        int i;
        for(i=0; i<tokens->size; i++){
            if (tokens->tokens[i] != NULL) {
                free2(tokens->tokens[i]);
            }
        }
        free2(tokens);
}

public List* generate_list_by_token(char* bytes, char token){
        List* list = list_create();
        Tokens* tokens = init_tokens(bytes, token);
        int i=0;
        for(i=0; i<tokens->size; i++){
			if(strlen(tokens->tokens[i]) == 0) continue;
			char* item = strdup(tokens->tokens[i]);
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
        char* result = m_get_buf_string(buf);
        trim(result, token);
        free2(token_string);
        list_rewind(list);
        destory_buf(buf);
        return result;
}

/**   I/O operation part  **/

/** if returns 0, that means the file not exist**/
public char* m_load_txt_file_to_memory(char *file_path){
        int file_size = get_file_size(file_path);
        if(file_size == 0) return NULL;
        char *buffer = mallocs(file_size);
        FILE *fp = fopen(file_path, "r");
        fread(buffer, file_size,1, fp);
        fclose(fp);
        return buffer;
}

/** If the file can not be created, the system will be aborted!!!
 *  Besides the content can be NULL, if just want to created the file.
 * **/
public void create_or_rewrite_file(char *file_name, char* content){
        FILE *fp = fopen(file_name, "w");
        if (fp == NULL) {
        	logg(EMERG, "can not create this file %s.", file_name);
        	exit(-1);
        }
        if(content != NULL){
            fwrite(content, strlen(content), 1, fp);
        }
        fclose(fp);
}

public boolean file_exist(char* file_path){
    	boolean result = false;
		FILE *fp = fopen(file_path, "r");
    	if(fp != NULL){
    		result = true;
    		fclose(fp);
    	}
    	return result;
}

/** If the file can not be handle, the system will be aborted!!! **/
public int get_file_size(char *file_path){
        FILE *fp = fopen(file_path, "r");
        if(fp == NULL){
        	logg(EMERG, "Can not open the file %s.", file_path);
        	exit(-1);
        }
        fseek(fp,0,SEEK_END);
        int size = ftell(fp);
        fclose(fp);
        return size;
}

/** will get file name before  first '.' with first file with this ext **/
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
        char* file_name = strdup(tokens->tokens[0]);
        free_tokens(tokens);
        return file_name;
}

/** will the file name with ext **/
public char* get_full_file_name(char* file_path){
        char* file_name = file_path;
        char* tmp = file_name;
        while( *tmp++ != '\0'){
			if(*tmp ==  FOLDER_SEPARATOR){
				file_name = tmp + 1;
			}
        }
        return file_name;
}

public List* get_lines_from_text_file(char* file_path){
        char* file_content = m_load_txt_file_to_memory(file_path);
        List* lines = generate_list_by_token(file_content, LINE_SEPARATOR);
        free2(file_content);
        return lines;
}

public List* get_lines_from_text_file_base_on_prefix(char* file_path, char* prefix){
        List* lines = get_lines_from_text_file(file_path);
        List* newLines = list_create();
        char* line = NULL;
        while((line = list_next(lines)) != NULL){
			if(str_n_match(line, prefix, strlen(prefix)))
				list_append(newLines, strdup(line));
        }
        list_destory(lines, just_free);
        return newLines;
}

/** conn string part **/
public char* m_get_ip_address(char* connection_string){
        char* ip_address = NULL;
        Tokens* tokens = init_tokens(connection_string, ':');
        ip_address = strdup(tokens->tokens[0]);
        free_tokens(tokens);
        return ip_address;
}

public int get_port(char* connection_string){
        Tokens* tokens = init_tokens(connection_string, ':');
        int port = atoi(tokens->tokens[1]);
        free_tokens(tokens);
        return port;
}


/** only allows character list **/
public List* string_to_list(char* list_string){
        return generate_list_by_token(list_string, STRING_SEPARATOR);
}

/** only allows character list **/
public char* list_to_string(List* list){
        return list_to_string_by_token(list, STRING_SEPARATOR);
}

/** The begin index is starts at zero **/
public char* array_to_string(char** array, int begin_index, int end_index){
		char* string = NULL;
		Buf* buf = init_buf();
		buf_cat(buf, array[begin_index], strlen(array[begin_index]));
		int i = 0;
		for(i = begin_index+1; i<=end_index; i++){
			buf_cat(buf, " ", strlen(" "));
			char* part = array[i];
			buf_cat(buf, part, strlen(part));
		}
		string = m_get_buf_string(buf);
		free_buf(buf);
		return string;
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
public long long get_current_time_stamp(){
        struct timeval t;
        gettimeofday (&t, NULL);
        long long time_mills = (long long)t.tv_sec * 1000;
        time_mills +=(long long) t.tv_usec/1000;
        return time_mills;
}

/** The Unit is MB **/
public int get_local_partition_free_space(){
		char* local_dir = ".";
		struct statfs diskInfo;
		statfs(local_dir, &diskInfo);
		unsigned long long totalBlocks = diskInfo.f_bsize;
		unsigned long long freeDisk = diskInfo.f_bavail*totalBlocks;
		return freeDisk / MB;
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
        free2(line);
        list_destory(list, just_free);
}

void testcase_for_get_lines_from_file(void){
        char* file_path = "conf/master.conf";
        List* lines = get_lines_from_text_file(file_path);
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
        free2(ret);
}

void testcase_for_token_operations(void)
{
    Tokens* thiz;
    char* test1 = "insert row:me  name:ike \tsex:male ";
    thiz = init_tokens(test1, ' ');
    assert(thiz->size == 4);
    assert(strcmp(thiz->tokens[0], "insert") == 0);
    assert(strcmp(thiz->tokens[1], "row:me") == 0);
    assert(strcmp(thiz->tokens[2], "name:ike") == 0);
    assert(strcmp(thiz->tokens[3], "sex:male") == 0);
    free_tokens(thiz);

    char *test2 = "insert#row:me###name:ike###sex:male###";
    thiz = init_tokens(test2, '#');
    assert(thiz->size == 4);
    assert(strcmp(thiz->tokens[0], "insert") == 0);
    assert(strcmp(thiz->tokens[1], "row:me") == 0);
    assert(strcmp(thiz->tokens[2], "name:ike") == 0);
    assert(strcmp(thiz->tokens[3], "sex:male") == 0);
    free_tokens(thiz);

    char *test3 = "";
    thiz = init_tokens(test3, '#');
    assert(thiz->size == 0);
    free_tokens(thiz);

    char *test4 = "     ";
    thiz = init_tokens(test4, ' ');
    assert(thiz->size == 0);
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
        char *dest = "peopley.yfile.table";
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
        char *dest = strdup(src);
        printf("%s\n",dest);
        free2(dest);
}

void testcase_for_logg(void){
        //char *str = "world";
	    setup_logging(INFO, "test.log");
	    logg(EMERG, "EMERG Line");
        logg(ISSUE, "ISSUE Line");
        logg(INFO, "INFO Line");
        logg(DEBUG, "DEBUG Line");
}

void testcase_for_times(void){
        char *string = "insert  row:me   name:ike  sex:male";
        printf("The number of occurrence:%d\n", count(string, ':'));
}

void testcase_for_cmp(void){
        char *dest = "HELlo WorLD";
        char *src = "heLlo WorLD";
        boolean result = str_n_match(dest, src, strlen(src));
        printf("--%s--\n",bool_to_str(result));
}

void testcase_for_trim(void){
        char *str = "\"male\"";
        char *dest = mallocs(strlen(str));
        strcpy(dest, str);
        dest = trim(dest, '"');
        printf("the string after trim:%s--\n",dest);
}

void testcase_for_get_file_size(void){
        char *file1 = "build.sh";
        char *file2 = "Readme";
        char *file3 = "Makefile";
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

void testcase_for_list_to_string(void){
        List* list = list_create();
        list_append(list, strdup("test"));
        char* value = list_to_string(list);
        printf("value:%s\n", value);
        free2(value);
        printf("the list after free:%s\n", (char*)list_next(list));
}

void testcase_for_generate_random_int(void){
        printf("%d\n", generate_random_int());
}

void testcase_for_get_current_time_mills(void){
        printf("%lld\n", get_current_time_stamp());
}

void testcase_for_get_local_partition_free_space(void){
		printf("%d\n", get_local_partition_free_space());
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
        printf("18) testcase_for_list_to_string\n");
        testcase_for_list_to_string();
        printf("19) testcase_for_generate_random_int\n");
        testcase_for_generate_random_int();
        printf("20) testcase_get_current_time_mills\n");
        testcase_for_get_current_time_mills();
        printf("21) testcase_for_get_local_partition_free_space\n");
        testcase_for_get_local_partition_free_space();
        printf("Completed Successfully\n");
}

int main(int argc, char *argv[])
{
    test_suite();
    return 0;
}
#endif
