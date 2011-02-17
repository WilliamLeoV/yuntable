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

#ifndef GLOBAL_H_
#define GLOBAL_H_

/**  common header files   **/
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>
#include <assert.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/stat.h>
#include <ctype.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <signal.h>
#include <sys/vfs.h>

#include "msg.h"
/**
 * The Global Header file contains all meta data that will be used throughout the project.
 ***/

/** version part **/
#define VERSION "1.0Dev"

/**  core part   **/

#define	public		/* PUBLIC is the opposite of PRIVATE */
#define	private	static	/* PRIVATE x limits the scope of x */

typedef char byte; /** stands for a series of data or bytes, rather than chars **/

typedef unsigned short boolean;

#define true 1
#define false 0

#define true_str "true"
#define false_str "false"

#define bool_to_str(bool) ((bool) == 1?true_str:false_str)

#define swap_pointer(x, y){void *temp = y; y = x; x = temp;}

#define INDEFINITE -1 //Means the number is not sure

/** Global Constants Part **/
#define DEFAULT_CLI_CONF_PATH "conf/cli.conf"

#define DEFAULT_MASTER_PORT 8301
#define DEFAULT_MASTER_CONF_PATH "conf/master.conf"

/** The port of region begins with 8302, and will be incremented if a new region instance added in this machine **/
#define DEFAULT_LOCAL_REGION_PORT 8302
#define DEFAULT_REGION_CONF_PATH "conf/region.conf"

#define MASTER_LOG_FILE "master.log"
#define REGION_LOG_FILE "region.log"

#define DEFAULT_FLUSH_MEMSTORE_INTERVAL 1 * 60 /** one minute(for testing) to flush memstore content to store file **/

#define LINE_BUF_SIZE 1000 //Means the max size of one line

#define NULL_STRING "NULL"

#define FOLDER_SEPARATOR '/'
#define FOLDER_SEPARATOR_STRING "/"

/** Currently only works at Linux **/
#define LINE_SEPARATOR '\n'
#define LINE_SEPARATOR_STRING "\n"

#define STRING_SEPARATOR ','
#define STRING_SEPARATOR_STRING ","

#define MID_SEPARATOR '.'
#define MID_SEPARATOR_STRING "."

#define CONF_SEPARATOR '='
#define CONF_SEPARATOR_STRING "="

#define ITEM_SEPARATOR ';'
#define ITEM_SEPARATOR_STRING ";"

#define TABLET_FOLDER_PREFIX "tablet"

#define MB		(1024 * 1024)

#define Millis 1000 //1000 Millis = one second

/** shared RPC Call **/

#define GET_ROLE_CMD "get_role"

#define REGION_KEY "region"

#define MASTER_KEY "master"


char *err_to_str(int err);

#endif /* GLOBAL_H_ */
