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

#ifndef LIST_H_
#define LIST_H_

#include "global.h"

/** structs declaration **/
typedef struct _ListNode{
        struct _ListNode *next;
        struct _ListNode *prev;
        void* data;
}ListNode;

typedef struct _List{
    ListNode* cursor;
    ListNode* first;
    ListNode* last;
    unsigned long node_num;
} List;

/** public method declaration **/
public List* list_create(void);

public void list_append(List* thiz, void* data);

public void list_add(List* thiz, void* data);

public void* list_get(List* thiz, int index);

public void* list_next(List* thiz);

public void* list_find(List* thiz, void* target, boolean(*matchs)(void *object, void *target));

public List* list_find_all(List* thiz, void* target, boolean(*match)(void *object, void *target));

public void list_replace(List* thiz, void* old_data, void* new_data);

public int list_remove(List* thiz, void* data, void (*free_object)(void *object));

public void list_rewind(List* thiz);

public List* list_sort(List* thiz);

public int list_size(List* thiz);

public void list_destory(List* thiz, void (*free_object)(void *object)) ;

public void only_free_struct(void *p);

public void just_free(void *p);

public List* array_to_list(void** array, int size);

public void** list_to_array(List* thiz);

#define for_each_list_node(node, thiz)                 \
    for (node = thiz->first; node; node = node->next)


#define for_each_list_node_safe(node, next_node, thiz)              \
    for (node = thiz->first, next_node = node ? node->next: NULL;   \
        node;                                                       \
        node = next_node, next_node = node ? node->next: NULL)
#endif /* LIST_H_ */
