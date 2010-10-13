#ifndef LIST_H_
#define LIST_H_

#include "global.h"

/** structs declaration **/
struct _List;
typedef struct _List List;

/** public method declaration **/
public List* list_create(void);

public boolean list_append(List* thiz, void* data);

/** the method for better coding, and thread safe, but not performance wise for large list iterating **/
public void* list_get(List* thiz, int index);

/** this method won't rewind the list, pls rewind the list after iterating it**/
public void* list_next(List* thiz);

public void* list_find(List* thiz, void* target, boolean(*matchs)(void *object, void *target));

public List* list_find_all(List* thiz, void* target, boolean(*match)(void *object, void *target));

public void list_replace(List* thiz, void* old_data, void* new_data);

/**
 * @param free_object since the object maybe nested struct, so need method for further freeing object
 *  but if the object has not been malloced, you only need a do nothing method;
 */
public boolean list_remove(List* thiz, void* data, void (*free_object)(void *object));

public void list_rewind(List* thiz);

/** will use default strcmp method to sort**/
public List* list_sort(List* thiz);

public int list_size(List* thiz);

public void list_destory(List* thiz, void (*free_object)(void *object))	;

/** the method is a fake method, for freeing object has not been malloced **/
public void only_free_struct(void *p);

/** this free method for character list **/
public void just_free(void *p);

public List* array_to_list(void** array, int size);

public void** list_to_array(List* thiz);


#endif /* LIST_H_ */
