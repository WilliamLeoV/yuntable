#ifndef CONN_H_
#define CONN_H_

#include "global.h"
#include "item.h"

/** connection constants **/
#define SUCCESS_CONN 1
#define FAIL_CONN 0


typedef struct _ConnParam ConnParam;

/** conn is class used by cli connect to master, master connect to client **/

public ConnParam* create_conn_param(char* conn, char* cmd, List* params);

/** Will be used at list destory, which will destory the params **/
public void destory_conn_param(void* connParam_void);

public byte* get_param(List* params, int index);

public int get_param_int(List* params, int index);

public List* add_int_param(List* params, int param_value);

public List* add_param(List* params, int param_size, byte* param_value);

public void* connect_thread(void* connParam_void);

/** need to free the result  **/
public byte* connect_conn(char* conn, char* cmd, List* params);

/* the connect_conn method will return booleam, and no to need to free the result */
public boolean connect_conn_boolean(char* conn, char* cmd, List* params);

public int connect_conn_int(char* conn, char* cmd, List* params);

public List* generate_charactor_params(int size, ...);

public void startup(int servPort, Buf* (*handler_request)(char *cmd, List* params));

public boolean check_node_validity(char* conn, char* type);

#endif /* CONN_H_ */
