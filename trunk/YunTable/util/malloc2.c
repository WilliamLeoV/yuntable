#include "malloc2.h"
#include "log.h"

/**
 * Base on Redis implementation, create a new Layer of memory allocation,
 * Which has three features: 
 * 		1. Will abort the system if oom happens;
 * 		2. Will calculate the total memroy usage;
 * 		3. Has frees method, which can free a bunch of pointers
 * **/

/** Pointer Prefix which used for storing the pointer size **/
#define PREFIX_SIZE sizeof(size_t)

/** The memory usage is a rough number, so there is no need to mutex it **/
long long total_memory_usage = 0;

/** The handler for out of memory situation **/
private void oom(){
		logg(EMERG, "The Memory Allocation has failed after allocated %lldB memory !!!!", total_memory_usage);
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
	   size_t total_size = size + PREFIX_SIZE;
       void* ptr = malloc(total_size);
       if(ptr == NULL) {
		   oom();
       }
	   *((size_t*)ptr) = total_size;
	   total_memory_usage += total_size;
	   return (char*)ptr+PREFIX_SIZE;
}

/** malloc2 is safe implementaion of malloc, will a  **/
public void* realloc2(void* ptr, size_t size){
		int new_size = size + PREFIX_SIZE;
		void* realptr = (char*)ptr-PREFIX_SIZE;
		size_t old_size = *((size_t*)realptr);
		void *newptr = realloc(realptr, new_size);
		if (newptr == NULL){
			 oom();
		}
		*((size_t*)newptr) = new_size;
		total_memory_usage -= old_size;
		total_memory_usage += new_size;
		return (char*)newptr+PREFIX_SIZE;
}

public void free2(void* ptr){
       if(ptr != NULL){
    	   void* realptr = (char*)ptr-PREFIX_SIZE;
		   int real_size = *((size_t*)realptr);
    	   total_memory_usage -= real_size;
    	   free(realptr);
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

