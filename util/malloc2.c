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

#include "malloc2.h"
#include "log.h"

/**
 * Base on Redis implementation, create a new Layer of memory allocation,
 * Which has three features: 
 * 		1. Will abort the system if oom happens;
 * 		2. Has frees method, which can free a bunch of pointers
 * **/

/** The handler for out of memory situation **/
private void oom(size_t size){
		logg(EMERG, "The Memory Allocation has failed by allocating %d byte!!!!", size);
	    sleep(1);
	    abort();
}

/** mallocs is memory allocation method for string, will have one more byte at the end for "\0", plus
 * the allocated memory area will be zeroed **/
public void* mallocs(size_t size){
		size = size + 1;
	    void* ptr = malloc2(size);
	    memset(ptr, 0, size);
        return ptr;
}

/** malloc2 is safe implementaion of malloc, will sum the total usage of memory for information **/
public void* malloc2(size_t size){
        void* ptr = malloc(size);
        if(ptr == NULL) {
		    oom(size);
        }
	    return ptr;
}

public void* realloc2(void* ptr, size_t size){
		void *newptr = realloc(ptr, size);
		if (newptr == NULL){
			oom(size);
		}
		return newptr;
}

public void free2(void* ptr){
       if(ptr != NULL){
    	   free(ptr);
       }
}

/** Free a bunch of pointers **/
public void frees(int size, ...){
		va_list ap;
		int i=0;
		va_start(ap, size);
		for(i=0; i<size; i++){
			void* ptr = va_arg(ap, void*);
			free2(ptr);
		}
}

