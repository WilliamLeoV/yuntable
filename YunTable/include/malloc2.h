#ifndef MALLOC_H_
#define MALLOC_H_

#include "global.h"

public void* mallocs(size_t size);

public void* malloc2(size_t size);

public void* realloc2(void* ptr, size_t size);

public void free2(void* ptr);

public void frees(int size, ...);

#endif /* MALLOC_H_ */
