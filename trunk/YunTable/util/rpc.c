#include "global.h"
#include "utils.h"
#include "item.h"
#include "rpc.h"

typedef struct _RPCRequest{
	char magic[8];
	int cmd_length; /** for deserializing**/
	char *cmd_name;
	List *params;
}RPCRequest;

typedef struct _RPCResponse{
	char magic[8];
	int status;
	int result_length; /** used in serializing and deserializing**/
	byte* result;
}RPCResponse;

struct _ConnParam{
	char* conn;
	char* cmd;
	List* params;
};

#define RPC_REQUEST_MAGIC "rpcrqst"
#define RPC_RESPONSE_MAGIC "rpcresp"

#define CONN_BUF_SIZE 1000 * 1000


public ConnParam* create_conn_param(char* conn, char* cmd, List* params){
	ConnParam* connParam = malloc(sizeof(connParam));
	connParam->conn = m_cpy(conn);
	connParam->cmd = m_cpy(cmd);
	connParam->params = params;
	return connParam;
}

/** Will be used at list destory, which will destory the params **/
public void destory_conn_param(void* connParam_void){
	ConnParam* connParam = (ConnParam*)connParam_void;
	free(connParam->conn);
	free(connParam->cmd);
	list_destory(connParam->params, just_free);
	free(connParam);
}

public byte* get_param(List* params, int index){
	return list_get(params, index);
}

public int get_param_int(List* params, int index){
	byte* result_bytes = get_param(params, index);
	int result = atoi(result_bytes);
	free(result_bytes);
	return result;
}

public List* generate_charactor_params(int size, ...){
	List* params = list_create();
	va_list ap;
	int i=0;
	va_start(ap, size);
	for(i=0; i<size; i++){
		//this syntax error is a eclipse bug
		char *str = va_arg(ap, char *);
		list_append(params, m_itos(strlen(str)));
		list_append(params, m_cpy(str));
	}
	return params;
}

public List* add_int_param(List* params, int param_value){
	list_append(params, m_itos(sizeof(param_value)));
	list_append(params, m_itos(param_value));
	return params;
}


public List* add_param(List* params, int param_size, byte* param_value){
	list_append(params, m_itos(param_size));
	list_append(params, param_value);
	return params;
}

/**
 * params serialize format:
 * list_size item_0_size item_0_value item_1_size item_1_value
 */
public Buf* params_to_byte(List* params){
	Buf *buf = init_buf();
	int size = list_size(params)/2, i=0;
	buf_cat(buf, &size, sizeof(size));
	for(i=0; i<size; i++){
		//flush the size of item
		int item_size = atoi(list_next(params));
		buf_cat(buf, &item_size, sizeof(item_size));
		//flush the content of item
		buf_cat(buf, list_next(params), item_size);
	}
	list_rewind(params);
	return buf;
}

/** this transformation will remove size for each item, so the total length will 1/2 **/
public List* byte_to_params(Buf* buf){
	List* list = list_create();
	int i=0, list_size = buf_load_int(buf, sizeof(int));
	for(i=0; i<list_size; i++){
		int item_size = buf_load_int(buf, sizeof(int));
		void *data = buf_load(buf, item_size);
		list_append(list, data);
	}
	return list;
}

private RPCRequest* create_rpc_request(char* cmd, List *params){
	RPCRequest* rpcRequest = malloc(sizeof(RPCRequest));
	cpy(rpcRequest->magic, RPC_REQUEST_MAGIC);
	rpcRequest->cmd_name = m_cpy(cmd);
	rpcRequest->params = params;
	return rpcRequest;
}

private RPCResponse* create_rpc_response(int status, int result_length, byte* result){
	RPCResponse* rpcResponse = malloc(sizeof(RPCResponse));
	cpy(rpcResponse->magic, RPC_RESPONSE_MAGIC);
	rpcResponse->status = status;
	rpcResponse->result_length = result_length;
	rpcResponse->result = result;
	return rpcResponse;
}

private Buf* rpc_request_to_byte(RPCRequest* rpcRequest){
	Buf *buf = init_buf();
	buf_cat(buf, rpcRequest->magic, sizeof(rpcRequest->magic));
	rpcRequest->cmd_length = strlen(rpcRequest->cmd_name) + 1;
	buf_cat(buf, &rpcRequest->cmd_length, sizeof(rpcRequest->cmd_length));
	buf_cat(buf, rpcRequest->cmd_name, rpcRequest->cmd_length);
	Buf *list_buf = params_to_byte(rpcRequest->params);
	buf_cat(buf, get_buf_data(list_buf), get_buf_index(list_buf));
	return buf;
}

private RPCRequest* byte_to_rpc_request(byte* bytes){
	RPCRequest* rpcRequest = malloc(sizeof(RPCRequest));
	Buf* buf = create_buf(0, bytes);
	char* magic_string = buf_load(buf, sizeof(rpcRequest->magic));
	cpy(rpcRequest->magic, magic_string);
	free(magic_string);
	rpcRequest->cmd_length = buf_load_int(buf, sizeof(rpcRequest->cmd_length));
	rpcRequest->cmd_name = (char*)buf_load(buf, rpcRequest->cmd_length);
	rpcRequest->params = byte_to_params(buf);
	free_buf(buf);
	return 	rpcRequest;
}

private void free_rpc_request(RPCRequest* rpcRequest){
	free(rpcRequest->cmd_name);
	free(rpcRequest);
}

/** will free the inside params **/
private void destory_rpc_request(RPCRequest* rpcRequest){
	list_destory(rpcRequest->params, just_free);
	free_rpc_request(rpcRequest);
}

private Buf* rpc_response_to_byte(RPCResponse *rpcResponse){
	Buf *buf = init_buf();
	buf_cat(buf, rpcResponse->magic, sizeof(rpcResponse->magic));
	buf_cat(buf, &rpcResponse->status, sizeof(rpcResponse->status));
	buf_cat(buf, &rpcResponse->result_length, sizeof(rpcResponse->result_length));
	buf_cat(buf, rpcResponse->result, rpcResponse->result_length);
	return buf;
}

private RPCResponse* byte_to_rpc_response(byte* bytes){
	RPCResponse* rpcResponse = malloc(sizeof(RPCResponse));
	Buf* buf = create_buf(0, bytes);
	char* magic_string = buf_load(buf, sizeof(rpcResponse->magic));
	cpy(rpcResponse->magic, magic_string);
	free(magic_string);
	rpcResponse->status =  buf_load_int(buf, sizeof(rpcResponse->status));
	rpcResponse->result_length = buf_load_int(buf, sizeof(rpcResponse->result_length));
	rpcResponse->result = buf_load(buf, rpcResponse->result_length);
	free_buf(buf);
	return rpcResponse;
}

public boolean check_node_validity(char* conn, char* type){
	boolean result = false;
	char* role_info = connect_conn(conn, GET_ROLE_CMD, NULL);
	if(match(role_info, type)) return true;
	free(role_info);
	return result;
}


/** client part **/

public void* connect_thread(void* connParam_void){
	ConnParam* connParam = (ConnParam*)connParam_void;
	return connect_conn(connParam->conn, connParam->cmd, connParam->params);
}

/* need to free the result, the cmd will return NULL  */
public byte* connect_conn(char* conn, char* cmd, List* params){
	char* ip_address = m_get_ip_address(conn);
	int port = get_port(conn);
	if(params == NULL) params = list_create();
	char buf[CONN_BUF_SIZE];
	struct sockaddr_in servaddr;
	int sockfd, n;

	sockfd = socket(AF_INET, SOCK_STREAM, 0);

	bzero(&servaddr, sizeof(servaddr));
	servaddr.sin_family = AF_INET;
	inet_pton(AF_INET, ip_address, &servaddr.sin_addr);
	servaddr.sin_port = htons(port);

	connect(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr));
	//process rpc request
	RPCRequest* rpcRequest = create_rpc_request(cmd, params);
	Buf* rpc_request_buf = rpc_request_to_byte(rpcRequest);
	write(sockfd, get_buf_data(rpc_request_buf), get_buf_index(rpc_request_buf));
	n = read(sockfd, buf, CONN_BUF_SIZE);
	RPCResponse* rpcResponse = byte_to_rpc_response(buf);
	byte* result = rpcResponse->result;

	close(sockfd);
	free_rpc_request(rpcRequest);
	free(rpcResponse);
	destory_buf(rpc_request_buf);
	free(ip_address);
	return result;
}

public boolean connect_conn_boolean(char* conn, char* cmd, List* params){
	byte* result_string = connect_conn(conn, cmd, params);
	boolean result = stob(result_string);
	free(result_string);
	return result;
}

public int connect_conn_int(char* conn, char* cmd, List* params){
	byte* result_string = connect_conn(conn, cmd, params);
	int result = atoi(result_string);
	free(result_string);
	return result;
}

/** server part **/

private void start_daemon(int listenfd, Buf* (*handler_request)(char *cmd, List* params)){
	struct sockaddr_in cliaddr;
	int connfd, n;
	char buf[CONN_BUF_SIZE];

	while(1){
		socklen_t cliaddr_len = sizeof(cliaddr);
		connfd = accept(listenfd, (struct sockaddr *)&cliaddr, &cliaddr_len);
		n = read(connfd, buf, CONN_BUF_SIZE);
		RPCRequest* rpcRequest = byte_to_rpc_request(buf);
		logg("The request is cmd:%s.\n", rpcRequest->cmd_name);
		Buf* result_buf = handler_request(rpcRequest->cmd_name, rpcRequest->params);
		RPCResponse* rpcResponse = NULL;
		if(result_buf != NULL){
			logg("The result of this request is:%s\n", get_buf_data(result_buf));
			rpcResponse = create_rpc_response(
				SUCCESS_CONN, get_buf_index(result_buf), get_buf_data(result_buf));
		}else{
			logg("This request can not be handled.\n", NULL);
			rpcResponse = create_rpc_response(FAIL_CONN, 0, NULL);
		}
		Buf* rpcResponseBuf = rpc_response_to_byte(rpcResponse);
		write(connfd, get_buf_data(rpcResponseBuf), get_buf_index(rpcResponseBuf));
		close(connfd);

		destory_rpc_request(rpcRequest);
		destory_buf(result_buf);
		free(rpcResponse);
		destory_buf(rpcResponseBuf);
	}
}

public void startup(int servPort, Buf* (*handler_request)(char *cmd, List* params)){
	struct sockaddr_in servaddr;
	int listenfd = socket(AF_INET, SOCK_STREAM, 0);

	bzero(&servaddr, sizeof(servaddr));
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
	servaddr.sin_port = htons(servPort);

	int opt = 1;
	setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

	if(bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) != 0){
		printf("Failed to listen the port %d, and quit!\n", servPort);
		exit(-1);
	}

	listen(listenfd, 20);
	printf("Starting the daemon and Accepting connection...\n");
	start_daemon(listenfd, handler_request);
}

#ifdef CONN_TEST
void testcase_for_connect_conn(void){
	char* conn = "127.0.0.1:8000";
	char* cmd = "test_cmd";
	List* params =  generate_charactor_params(1, m_cpy("param1"));
	byte* result = connect_conn(conn, cmd, params);
	printf("%s\n", result);
}

Buf* handler_test_request(char *cmd, List* params){
	printf("%s\n", cmd);
	printf("%s\n", get_param(params, 0));
	char* result = "test";
	return create_buf(strlen(result), result);
}

void testcase_for_start_daemon(void){
	startup(8000, handler_test_request);
}

void testcase_for_rpc_request(void){
	List* params = generate_charactor_params(3 ,m_cpy("test1") ,m_cpy("test2") ,m_cpy("test3"));
	char* cmd = "test_cmd";
	RPCRequest* rpcRequest = create_rpc_request(cmd, params);
	Buf* buf = rpc_request_to_byte(rpcRequest);
	RPCRequest* newRPCRequest = byte_to_rpc_request(get_buf_data(buf));
	printf("%s\n", newRPCRequest->magic);
	printf("%s\n", newRPCRequest->cmd_name);
	printf("%s\n", get_param(newRPCRequest->params, 0));
	printf("%s\n", get_param(newRPCRequest->params, 1));
	printf("%s\n", get_param(newRPCRequest->params, 2));
	destory_rpc_request(rpcRequest);
	destory_rpc_request(newRPCRequest);
}

void testcase_for_rpc_response(void){
	RPCResponse* rpcResponse = create_rpc_response(SUCCESS_CONN,  5, m_cpy("test5"));
	Buf* buf = rpc_response_to_byte(rpcResponse);
	FILE *fp = fopen("test", "wb");
	fwrite(get_buf_data(buf), get_buf_index(buf), 1, fp);

	RPCResponse* newRPCResponse = byte_to_rpc_response(get_buf_data(buf));
	printf("%s\n", newRPCResponse->magic);
	printf("%d\n", newRPCResponse->status);
	printf("%s\n", newRPCResponse->result);
	destory_rpc_response(rpcResponse);
	destory_rpc_response(newRPCResponse);
	fclose(fp);
}

void testcase_for_params_byte(void){
	FILE *fp = fopen("test", "wb");
	List* params = generate_charactor_params(3 ,m_cpy("test1") ,m_cpy("test2") ,m_cpy("test3"));
	add_param(params, 4, m_cpy("test4"));

	Buf* buf = params_to_byte(params);
	printf("The size of buf:%d\n", get_buf_index(buf));
	fwrite(get_buf_data(buf), get_buf_index(buf), 1, fp);
	fclose(fp);

	Buf* new_buf = create_buf(0, get_buf_data(buf));
	List* newList = byte_to_params(new_buf);
	int i=0, index = list_size(newList);
	printf("size:%d\n", index);
	for(i=0; i<index; i++){
		char* item = list_next(newList);
		printf("%s\n", item);
	}

	list_destory(params, just_free);
	list_destory(newList, just_free);
}

void testcase_for_memcat(void){
	int testInt = 1;
	char *testChar = "ad";
	Buf *buf = init_buf(BIG_BUF_SIZE);
	buf_cat(buf, &testInt, sizeof(int));
	buf_cat(buf, testChar, strlen(testChar));
	FILE *fp = fopen("test", "wb");
	fwrite(get_buf_data(buf), sizeof(int) + strlen(testChar), 1, fp);
	fclose(fp);
	fp = fopen("test", "rb");
	Buf *buf2 = init_buf(BIG_BUF_SIZE);
	fread(get_buf_data(buf2), 6, 1, fp);
	int testInt2 = buf_load_int(buf2, 4);
	char *testChar2 = buf_load(buf2, 2);
	printf("%d %s\n", testInt2, testChar2);
	fclose(fp);
}

void test_suite(void){
	printf("The Test Suit of item is starting\n");
	printf("0) testcase_for_rpc_request\n");
	testcase_for_rpc_request();
	printf("1) testcase_for_rpc_response\n");
	testcase_for_rpc_response();
	printf("2) testcase_for_params_byte\n");
	testcase_for_params_byte();
	printf("3) testcase_for_memcat\n");
	testcase_for_memcat();
	printf("Completed Successfully\n");
}

int main(int argc, char *argv[]){
	test_suite();
	return 1;
}

#endif
