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

#ifndef BUF_H_
#define BUF_H_

#include "global.h"

/**  serializing and deserializing methods **/
typedef struct _Buf Buf;

public byte* get_buf_data(Buf *buf);

public int get_buf_index(Buf *buf);

public char* m_get_buf_string(Buf *buf);

public Buf* init_buf();

public Buf* create_buf(int size, byte* data);

public void buf_cat(Buf *buf, void* src, int size);

public void buf_combine(Buf* dest_buf, Buf* src_buf);

public void* buf_load(Buf* buf, int size);

public int buf_load_int(Buf* buf);

public long long buf_load_long_long(Buf* buf);

public short buf_load_short(Buf* buf);

public void destory_buf(Buf* buf);

public void free_buf(Buf* buf);


#endif /* BUF_H_ */
