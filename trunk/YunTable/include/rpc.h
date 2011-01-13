#ifndef CONN_H_
#define CONN_H_

#include "global.h"
#include "item.h"

/** Status Code Part, which show the status of RPC Call **/
#define SUCCESS 0 //The cmd has been processed successfully
#define CONN_FAIL 1 //Has failed to connect to remote target node
#define ERROR_NO_CMD 2 //The RPC Request has not included any cmd
#define ERROR_NO_PARAM 3 //The RPC Request has not included needed parameter
#define ERROR_TABLET_NOT_EXIST 4 //The Region Node don't have the target tablet
//#define IN_PROGRESS //Which means the cmd is still processing, may has use in the future

#define CONN_FAIL_MSG "connection Failed"
#define ERROR_NO_CMD_MSG "no command included in the request"
#define ERROR_NO_PARAM_MSG "not enough parameters included in the request"
#define ERROR_TABLET_NOT_EXIST_MSG "the Region Node don't have the the tablet belong to this table"

#define UNDEFINED_STATUS_CODE_MSG "the Status Code has not been defined"

typedef struct _RPCRequest RPCRequest;
typedef struct _RPCResponse RPCResponse;
typedef struct _ConnParam ConnParam;

public char* get_error_message(int status_code);

public int get_status_code(RPCResponse* rpcResponse);

public int get_result_length(RPCResponse* rpcResponse);

public byte* get_result(RPCResponse* rpcResponse);

public RPCRequest* create_rpc_request(char* cmd, List *params);

public RPCResponse* create_rpc_response(int status, int result_length, byte* result);

public void destory_rpc_request(RPCRequest* rpcRequest);

public void destory_rpc_response(RPCResponse* rpcResponse);

public byte* get_param(List* params, int index);

public List* add_int_param(List* params, int param_value);

public List* add_param(List* params, int param_size, byte* param_value);

public RPCResponse* connect_conn(char* conn, RPCRequest* rpcRequest);

public List* generate_charactor_params(int size, ...);

public void startup(int servPort, RPCResponse* (*handler_request)(char *cmd, List* params));

public boolean check_node_validity(char* conn, char* type);

#endif /* CONN_H_ */
