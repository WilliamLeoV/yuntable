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

#ifndef MALLOC_H_
#define MALLOC_H_

#include "global.h"

public void* mallocs(size_t size);

public void* malloc2(size_t size);

public void* realloc2(void* ptr, size_t size);

public void free2(void* ptr);

public void frees(int size, ...);

#endif /* MALLOC_H_ */
