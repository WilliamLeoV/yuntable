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

#include "malloc2.h"
#include "list.h"


/** methods body **/
private ListNode* list_next_node(List* thiz){
	//if the list is empty or reach the end
	if(thiz->cursor==NULL) {
        return NULL;
    }

	ListNode *node = thiz->cursor;
	thiz->cursor = thiz->cursor->next;
	return node;
}

private void free_node(ListNode* node)
{
	free2(node);
}

public List* list_create(void){
	List *list = malloc2(sizeof(List));
	//safe action
	list->first = NULL;
	list->last = NULL;
	list->cursor = NULL;
    list->node_num = 0;
	return list;
}

private ListNode* list_create_node(void* data)
{
	ListNode *next = malloc2(sizeof(ListNode));
	next->data = data;
	next->next = NULL;
	next->prev = NULL;
	return next;
}

/** Not Performance Wise Choice, but no need to rewind the list after finishing using it **/
public void list_append(List* thiz, void* data)
{
	list_add(thiz, data);
	list_rewind(thiz);
}

/** Performance Wise Choice, pls rewind the list after finishing using it **/
public void list_add(List* thiz, void* data)
{
	ListNode *node = list_create_node(data);
	//if the list is empty
	if (thiz->first == NULL) {
		thiz->first = node;
        thiz->last = node;
	} else {
        node->prev = thiz->last;
        thiz->last->next = node;
        thiz->last = node;
	}
    thiz->node_num++;
}

/** the method for better coding, and thread safe, but not performance wise for large list iterating **/
public void* list_get(List* thiz, int index)
{
    int i=0;
    ListNode *node;

    for_each_list_node(node, thiz) {
        if (i++ == index) {
            return node->data;
        }
    }
	return NULL;
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

public void* list_find(List* thiz, void* target, boolean(*match)(void *object, void *target))
{
    ListNode *node;

    for_each_list_node(node, thiz) {
        if(match(node->data, target)) {
            return node->data;
        }
    }
    return NULL;
}

public void list_replace(List* thiz, void* old_data, void* new_data)
{
    ListNode *node;

    for_each_list_node(node, thiz) {
        if (node->data == old_data) {
            node->data = new_data;
            return; /* there should only one old_data in the list */
        }
    }
}

public List* list_find_all(List* thiz, void* target, boolean(*match)(void *object, void *target)){

    ListNode *node;
    List* newList = list_create();

    for_each_list_node(node, thiz) {
        if (match(node->data, target)) {
            list_append(newList, node->data);
        }
    }

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
public int list_remove(List* thiz, void* data, void (*free_object)(void *object))
{
    int ret = ERR_NO_EXIST;
    ListNode *node, *next_node;

    if (thiz == NULL || free_object == NULL) {
        return ERR_INVAL;
    }
    for_each_list_node_safe(node, next_node, thiz) {
        if (node->data == data) {
            thiz->node_num--;
            if (node->prev) {
                node->prev->next = node->next;
            }
            if (node->next) {
                node->next->prev = node->prev;
            }
            if (thiz->first == node) {
                thiz->first = node->next;
            }
            if (thiz->last == node) {
                thiz->last = node->prev;
            }
            free_object(data);
            free_node(node);
            ret = SUCCESS;
            break;
        }
    }
    list_rewind(thiz);
    return ret;
}


public void list_destory(List* thiz, void (*free_object)(void *object))
{
    if (thiz == NULL) {
        return;
    }
    if (free_object == NULL) {
        free_object = free2;
    }
    ListNode *node, *next_node;
    for_each_list_node_safe(node, next_node, thiz) {
        if (node->data) {
            free_object(node->data);
        }
        free2(node);
    }
    free2(thiz);
}

public void list_rewind(List* thiz){
        thiz->cursor = thiz->first;
}

public int list_size(List* thiz)
{
    return thiz->node_num;
}

/** the method is a fake method, for freeing object has not been malloced **/
public void only_free_struct(void *p){};

/** this free method for character list **/
public void just_free(void *p){
        if(p != NULL) {
                free2(p);
        }
}

public List* array_to_list(void** array, int size){
        List* list = list_create();
        int i=0;
        for(i=0; i<size; i++){
            list_append(list, array[i]);
        }
        return list;
}

public void** list_to_array(List* thiz)
{
    int i = 0;
    ListNode *node;
    int size = list_size(thiz);
    void **array = malloc2(sizeof(void *) * size);
    for_each_list_node(node, thiz) {
        array[i++] = node->data;
    }
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


void struct_list_print(List *thiz)
{

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
                mess->value = strdup(text[i]);
                list_append(list, mess);
        }
        list_rewind(list);
        printf("the list after creation\n");
        assert(list_size(list) == 4);
        struct_list_print(list);

        Mess *mess;
        while((mess = list_next(list))!=NULL){
                if((strcmp(mess->value, "hell1")==0)||(strcmp(mess->value, "hello")==0)){
                                list_remove(list, mess, free_mess);
                                printf("one mess has been deleted\n");
                }
        }

        printf("the list after deleted\n");
        assert(list_size(list) == 2);
        struct_list_print(list);
        list_destory(list, free_mess);
}


boolean test_matchs(void *mess, void *target){
        boolean result = false;
        Mess *tmp_mess = (Mess *)mess;
        char *tmp_target = (char *)target;
        if (strcmp(tmp_mess->value, tmp_target)==0){
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
                mess->value = strdup(text[i]);
                list_append(list, mess);
        }
        printf("the list after creation\n");
        assert(list_size(list) == 4);
        struct_list_print(list);

        char *target = "hell1";
        Mess *target_mess = list_find(list, target, test_matchs);
        assert(target_mess != NULL);
        printf("---matchs:%p\n", target_mess);
        list_destory(list, free_mess);
}

void testcase_for_append_next(){
        char *test = strdup("hello");
        List* list = list_create();
        list_append(list, test);
        printf("%s", (char*) list_next(list));
        list_destory(list, NULL);
}

int main(void){
        testcase_for_struct_list();
        testcase_for_list_find();
        return 1;
}
#endif /* LIST_TEST */
