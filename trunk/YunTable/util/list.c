#include "malloc2.h"
#include "list.h"


typedef struct _ListNode{
	struct _ListNode *next;
	struct _ListNode *prev;
	void* data;
}ListNode;

struct _List{
	ListNode* cursor;
	ListNode* first;
};

/** methods body **/
private ListNode* list_next_node(List* thiz){
	//if the list is empty or reach the end
	if(thiz->cursor==NULL) return NULL;

	ListNode *node = thiz->cursor;
	thiz->cursor = thiz->cursor->next;
	return node;
}

private void free_node(ListNode* node){
	if(node->prev != NULL) node->prev = NULL;
	if(node->next != NULL) node->next = NULL;
	free2(node);
}

public List* list_create(void){
	List *list = malloc2(sizeof(List));
	//safe action
	list->first = NULL;
	list->cursor = NULL;
	return list;
}

private ListNode* create_list_node(void* data){
	ListNode *next = malloc2(sizeof(ListNode));
	next->data = data;
	next->next = NULL;
	next->prev = NULL;
	return next;
}

/** Not Performance Wise Choice, but no need to rewind the list after finishing using it **/
public void list_append(List* thiz, void* data){
	list_rewind(thiz);
	list_add(thiz, data);
	list_rewind(thiz);
}

/** Performance Wise Choice, pls rewind the list after finishing using it **/
public void list_add(List* thiz, void* data){
	ListNode *next = create_list_node(data);
	//if the list is empty
	if(thiz->cursor == NULL){
		next->prev = NULL;
		thiz->first = next;
		list_rewind(thiz);
	}else{
		while(thiz->cursor->next!=NULL){
			thiz->cursor = thiz->cursor->next;
		}
		next->prev = thiz->cursor;
		thiz->cursor->next = next;
	}
}

/** the method for better coding, and thread safe, but not performance wise for large list iterating **/
public void* list_get(List* thiz, int index){
	void* result = NULL;
	list_rewind(thiz);
	int i=0;
	void* temp = NULL;
	while((temp = list_next(thiz)) != NULL){
		if(i == index){
			result = temp;
			break;
		}else{
			i++;
		}
	}
	list_rewind(thiz);
	return result;
}

/** this method won't rewind the list, pls rewind the list after iterating it, and it is performance wise**/
public void* list_next(List* thiz){
	//if the list is empty or reach the end
	ListNode *node = list_next_node(thiz);
	if(node==NULL){
		return NULL;
	}else{
		return node->data;
	}
}

public void* list_find(List* thiz, void* target, boolean(*match)(void *object, void *target)){
	list_rewind(thiz);
	void *object = NULL;
	while((object = list_next(thiz)) != NULL){
		if(match(object, target)) break;
	}
	list_rewind(thiz);
	return object;
}

public void list_replace(List* thiz, void* old_data, void* new_data){
	list_rewind(thiz);
	ListNode *node = list_next_node(thiz);
	if(node!=NULL && node->data == old_data)
		node->data = new_data;
	list_rewind(thiz);
}

public List* list_find_all(List* thiz, void* target, boolean(*match)(void *object, void *target)){
	List* newList = list_create();
	list_rewind(thiz);
	void *object = NULL;
	while((object = list_next(thiz)) != NULL){
		if(match(object, target)) list_append(newList, object);
	}
	list_rewind(thiz);
	return newList;
}

/** will use default strcmp method to sort**/
public List* list_sort(List* thiz){
	List *newList = list_create();
	//convert list to array
	int size = list_size(thiz);
	char** arrays = (char**)list_to_array(thiz);
	int i=0, j=0, k=0;
	for(i=0; i<size; i++){
		for(j=i+1; j<size; j++){
			if(strcmp(arrays[i], arrays[j])>0){
				swap_pointer(arrays[i], arrays[j]);
			}
		}
	}
	for(k=0; k<size; k++){
		list_append(newList, arrays[k]);
	}
	return newList;
}

/**
 * @param free_object since the object maybe nested struct, so need method for further freeing object
 *  but if the object has not been malloced, you only need a do nothing method;
 */
public boolean list_remove(List* thiz, void* data, void (*free_object)(void *object)){
	free_object(data);
	list_rewind(thiz);
	ListNode *node = NULL;
	while((node = list_next_node(thiz))!=NULL){
		if(node->data == data){
			//if the first node catchs
			if(node == thiz->first){
				thiz->first = node -> next;
			}else{
				node->prev->next = node->next;
				node->next->prev = node->prev;
			}
			free_node(node);
			return true;
		}
	}
	return false;
}

public void list_destory(List* thiz, void (*free_object)(void *object)){
	if(thiz != NULL){
		list_rewind(thiz);
		void *data = NULL;
		while((data = list_next(thiz))!=NULL) {
			list_remove(thiz, data, free_object);
		}
		free2(thiz);
	}
}

public void list_rewind(List* thiz){
	thiz->cursor = thiz->first;
}

public int list_size(List* thiz){
	list_rewind(thiz);
	int i=0;
	void *data = NULL;
	while((data = list_next(thiz))!=NULL) i++;
	list_rewind(thiz);
	return i;
}

/** the method is a fake method, for freeing object has not been malloced **/
public void only_free_struct(void *p){};

/** this free method for character list **/
public void just_free(void *p){
	if(p != NULL) free2(p);
}

public List* array_to_list(void** array, int size){
	List* list = list_create();
	int i=0;
	for(i=0; i<size; i++){
		list_append(list, array[i]);
	}
	return list;
}

public void** list_to_array(List* thiz){
	int size = list_size(thiz);
	void **array = malloc2(sizeof(void *) * size);
	int i=0;
	list_rewind(thiz);
	for(i=0; i<size; i++){
		array[i] = list_next(thiz);
	}
	list_rewind(thiz);
	return array;
}
#ifdef LIST_TEST
void testcase_for_list_int(void){
	List* list = list_create();
	list_append(list, (void*)1);
	int i = (int)list_get(list, 0);
	printf("%d\n", i);
}

#include <string.h>
void testcase_for_list_get(void){
	List* list = list_create();
	char *text[] = {"1", "2", "3", "4"};
	int i=0;
	for(i=0; i<4; i++) list_append(list, text[i]);
	for(i=0; i<4; i++) printf("%s\n",(char*)list_get(list, i));
}

void testcase_for_char_list(void){
	printf("the testcase for char list\n");
	//char_list_print(empty_list);
	char *text[] = {"hello", "world", "hell1", "worl1"};
	List *list = list_create();
	int i=0;
	for(i=0; i<4; i++){
		list_add(list, text[i]);
	}

	list_rewind(list);
	char* data;
	while((data = list_next(list))!= NULL){
		if((strcmp(data, "hell1")==0)||(strcmp(data, "hello")==0)){
			list_remove(list, data, only_free_struct);
			printf("one word has been deleted\n");
		}
	}

	printf("the list after deleted\n");
	//char_list_print(list);
	list_destory(list, only_free_struct);
	printf("the list after destory\n");
	//char_list_print(list);
}

typedef struct _Mess{
	char* value;
}Mess;


void struct_list_print(List *thiz){
	list_rewind(thiz);
	Mess *mess;
	while((mess = list_next(thiz)) != NULL){
		printf("%s\n",(char *)(mess->value));
	}
	list_rewind(thiz);
}

void free_mess(void *mess){
	free2(((Mess *)mess)->value);
	free2(mess);
}

void testcase_for_struct_list(void){
	printf("the testcase for struct list\n");
	char *text[] = {"hello", "world", "hell1", "worl1"};
	List *list = list_create();
	int i = 0;
	for(i=0; i<4; i++){
		Mess *mess = malloc2(sizeof(Mess));
		mess->value = malloc2(strlen(text[i]));
		strncpy(mess->value, text[i], strlen(text[i]));
		list_append(list, mess);
	}
	list_rewind(list);
	printf("list size:%d\n", list_size(list));
	printf("the list after creation\n");
	struct_list_print(list);

	Mess *mess;
	while((mess = list_next(list))!=NULL){
		if((strcmp(mess->value, "hell1")==0)||(strcmp(mess->value, "hello")==0)){
				list_remove(list, mess, free_mess);
				printf("one mess has been deleted\n");
		}
	}

	printf("the list after deleted\n");
	struct_list_print(list);
	list_destory(list, free_mess);
	printf("the list after destory\n");
	struct_list_print(list);
}


boolean test_matchs(void *mess, void *target){
	boolean result = false;
	Mess *tmp_mess = (Mess *)mess;
	char *tmp_target = (char *)target;
	if(strncmp(tmp_mess->value, tmp_target, strlen(tmp_mess->value))==0){
		result = true;
	}
	printf("%s\n", bool_to_str(result));
	return result;
}

void testcase_for_list_find(void){
	printf("the testcase for struct list\n");
	char *text[] = {"hello", "world", "hell1", "worl1"};
	List *list = list_create();
	int i = 0;
	for(i=0; i<4; i++){
		Mess *mess = malloc2(sizeof(Mess));
		mess->value = calloc(1, sizeof(char)*strlen(text[i]));
		strncpy(mess->value, text[i], strlen(text[i]));
		list_append(list, mess);
	}
	printf("the list after creation\n");
	struct_list_print(list);

	char *target = "hell1";
	Mess *target_mess = list_find(list, target, test_matchs);
	printf("---matchs:%p\n", target_mess);
}

void testcase_for_append_next(){
	char *test = "hello";
	List* list = list_create();
	list_append(list, test);
	printf("%s", (char*) list_next(list));
}

int main(void){
	testcase_for_struct_list();
	return 1;
}
#endif /* LIST_TEST */
