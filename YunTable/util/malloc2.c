#include "malloc2.h"
#include "log.h"

/**
 * Base on Redis implementation, create a new Layer of memory allocation,
 * Which has three features: 
 * 		1. Will abort the system if oom happens;
 * 		2. Has frees method, which can free a bunch of pointers
 * **/

/** The handler for out of memory situation **/
private void oom(){
		logg(EMERG, "The Memory Allocation has failed!!!!");
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
		    oom();
        }
	    return ptr;
}

public void* realloc2(void* ptr, size_t size){
		void *newptr = realloc(ptr, size);
		if (newptr == NULL){
			oom();
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

