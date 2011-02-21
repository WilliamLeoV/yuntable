/**
 * Copyright 2011 Wu Zhu Hua
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
struct _List;
typedef struct _List List;

/** public method declaration **/
public List* list_create(void);

public void list_append(List* thiz, void* data);

public void list_add(List* thiz, void* data);

public void* list_get(List* thiz, int index);

public void* list_next(List* thiz);

public void* list_find(List* thiz, void* target, boolean(*matchs)(void *object, void *target));

public List* list_find_all(List* thiz, void* target, boolean(*match)(void *object, void *target));

public void list_replace(List* thiz, void* old_data, void* new_data);

public boolean list_remove(List* thiz, void* data, void (*free_object)(void *object));

public void list_rewind(List* thiz);

public List* list_sort(List* thiz);

public int list_size(List* thiz);

public void list_destory(List* thiz, void (*free_object)(void *object))	;

public void only_free_struct(void *p);

public void just_free(void *p);

public List* array_to_list(void** array, int size);

public void** list_to_array(List* thiz);


#endif /* LIST_H_ */
