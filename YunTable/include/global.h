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
#include <arpa/inet.h>
#include <signal.h>

/**  core part   **/

#define	public		/* PUBLIC is the opposite of PRIVATE */
#define	private	static	/* PRIVATE x limits the scope of x */

typedef char byte; /** stands for a series of data or bytes **/

typedef unsigned short boolean;

#define true 1
#define false 0

#define true_str "true"
#define false_str "false"

#define bool_to_str(bool) ((bool) == 1?true_str:false_str)

#define swap_pointer(x, y){void *temp = y; y = x; x = temp;}

/** Global Constants Part **/
#define DEFAULT_FLUSH_MEMSTORE_INTERVAL 1*60 /** one minute(for testing) to flush memstore content to store file **/

#define NULL_STRING "NULL"

#define FOLDER_SEPARATOR '/'
#define FOLDER_SEPARATOR_STRING "/"

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

#define DEFAULT_COLUMN_FAMILY "default_cf"

#define TABLET_FOLDER_PREFIX "tablet"

#define EMPTY_STRING ""

/** shared RPC Call **/

#define GET_ROLE_CMD "get_role"

#define REGION_KEY "region"

#define MASTER_KEY "master"

#endif /* GLOBAL_H_ */
