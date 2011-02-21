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

#ifndef LOG_H_
#define LOG_H_

#include "global.h"

/** Log Level Part **/
#define DISABLE 4 //Disable - system is unusable, and will aborted. For example: Memory Allocation failed.
#define EMERG 3 //Emergencies - system is unusable, and will aborted. For example: Memory Allocation failed.
#define ISSUE 2 //Some bad thing has occurs, but the situation is recoverable. For example: the default region port has been taken.
#define INFO 1 //Some important event happens, need to record it. For example: the memstore is flushing.
#define DEBUG 0 //Store all performance and command info. For example: the time of command execution

public void setup_logging(int log_level, char* log_file_path);

public void logg(int level, const char *fmt, ...);

#endif /* LOG_H_ */
