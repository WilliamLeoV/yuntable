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

#include "utils.h"
#include "item.h"
#include "malloc2.h"
#include "rpc.h"
#include "log.h"
#include "buf.h"

struct _RPCRequest{
        char magic[8];
        int cmd_length; /** for deserializing**/
        char *cmd_name;
        List *params;
};

struct _RPCResponse{
		char magic[8];
		int status_code; /*  */
		int result_length; /** used in serializing and deserializing**/
		byte* result;
};

#define RPC_REQUEST_MAGIC "rpcrqst"
#define RPC_RESPONSE_MAGIC "rpcresp"

/** The Buffer Pool for holding RPCResponse Data  **/
#define CONN_BUF_SIZE 1000 * 1000

/** If the status code is not qualified, the method will return **/
public char* get_error_message(int status_code){
		if(status_code == CONN_FAIL){
			return CONN_FAIL_MSG;
		}else if(status_code == REMOTE_FAIL){
			return REMOTE_FAIL_MSG;
		}else if(status_code == ERROR_NO_CMD){
			return ERROR_NO_CMD_MSG;
		}else if(status_code == ERROR_WRONG_CMD){
			return ERROR_WRONG_CMD_MSG;
		}else if(status_code == ERROR_NO_PARAM){
			return ERROR_NO_PARAM_MSG;
		}else if(status_code == ERROR_TABLET_NOT_EXIST){
			return ERROR_TABLET_NOT_EXIST_MSG;
		}else{
			return UNDEFINED_STATUS_CODE_MSG;
		}
}

public int get_status_code(RPCResponse* rpcResponse){
		return rpcResponse->status_code;
}

public int get_result_length(RPCResponse* rpcResponse){
		return rpcResponse->result_length;
}

public byte* get_result(RPCResponse* rpcResponse){
		return rpcResponse->result;
}

public byte* get_param(List* params, int index){
        return list_get(params, index);
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
        int i=0, size = 0;
        if(params != NULL){
        	size = list_size(params)/2;
        }
        buf_cat(buf, &size, sizeof(size));
        for(i=0; i<size; i++){
			//flush the size of item
			int item_size = btoi(list_next(params));
			buf_cat(buf, &item_size, sizeof(item_size));
			//flush the content of item
			buf_cat(buf, list_next(params), item_size);
        }
        if(params != NULL){
            list_rewind(params);
        }
        return buf;
}

/** this transformation will remove size for each item, so the total length will 1/2 **/
public List* byte_to_params(Buf* buf){
        List* list = list_create();
        int i=0, list_size = buf_load_int(buf);
        for(i=0; i<list_size; i++){
			int item_size = buf_load_int(buf);
			void *data = buf_load(buf, item_size);
			list_append(list, data);
        }
        return list;
}

public RPCRequest* create_rpc_request(char* cmd, List *params){
        RPCRequest* rpcRequest = malloc2(sizeof(RPCRequest));
        cpy(rpcRequest->magic, RPC_REQUEST_MAGIC);
        rpcRequest->cmd_name = m_cpy(cmd);
        rpcRequest->params = params;
        return rpcRequest;
}

public RPCResponse* create_rpc_response(int status, int result_length, byte* result){
		RPCResponse* rpcResponse = malloc2(sizeof(RPCResponse));
		cpy(rpcResponse->magic, RPC_RESPONSE_MAGIC);
		rpcResponse->status_code = status;
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
        RPCRequest* rpcRequest = malloc2(sizeof(RPCRequest));
        Buf* buf = create_buf(0, bytes);
        char* magic_string = buf_load(buf, sizeof(rpcRequest->magic));
        cpy(rpcRequest->magic, magic_string);
        free2(magic_string);
        rpcRequest->cmd_length = buf_load_int(buf);
        rpcRequest->cmd_name = (char*)buf_load(buf, rpcRequest->cmd_length);
        rpcRequest->params = byte_to_params(buf);
        free_buf(buf);
        return  rpcRequest;
}

/** will free the inside params **/
public void destory_rpc_request(RPCRequest* rpcRequest){
        list_destory(rpcRequest->params, just_free);
		frees(2, rpcRequest->cmd_name, rpcRequest);
}

/** will free inside result **/
public void destory_rpc_response(RPCResponse* rpcResponse){
        frees(2, rpcResponse->result, rpcResponse);
}

private Buf* rpc_response_to_byte(RPCResponse *rpcResponse){
		Buf *buf = init_buf();
		buf_cat(buf, rpcResponse->magic, sizeof(rpcResponse->magic));
		buf_cat(buf, &rpcResponse->status_code, sizeof(rpcResponse->status_code));
		buf_cat(buf, &rpcResponse->result_length, sizeof(rpcResponse->result_length));
		buf_cat(buf, rpcResponse->result, rpcResponse->result_length);
		return buf;
}

private RPCResponse* byte_to_rpc_response(byte* bytes){
		RPCResponse* rpcResponse = malloc2(sizeof(RPCResponse));
		Buf* buf = create_buf(0, bytes);
		char* magic_string = buf_load(buf, sizeof(rpcResponse->magic));
		cpy(rpcResponse->magic, magic_string);
		free2(magic_string);
		rpcResponse->status_code =  buf_load_int(buf);
		rpcResponse->result_length = buf_load_int(buf);
		rpcResponse->result = buf_load(buf, rpcResponse->result_length);
		free_buf(buf);
		return rpcResponse;
}

public boolean check_node_validity(char* conn, char* type){
        boolean result = false;
        RPCRequest* rpcRequest = create_rpc_request(GET_ROLE_CMD, NULL);
		RPCResponse* rpcResponse = connect_conn(conn, rpcRequest);
		int status_code = get_status_code(rpcResponse);
		if(status_code == SUCCESS){
			char* role_info = rpcResponse->result;
			if (match(role_info, type)) {
				result = true;
			}
		}else{
			printf("Have some problem when check node %s validity:%s!\n", conn,  get_error_message(status_code));
		}
		destory_rpc_request(rpcRequest);
		destory_rpc_response(rpcResponse);
        return result;
}

/** client part **/

/** Currently, mainly used in the server(Callee) side for dispatching the data faster **/
private void setTcpNoDelay(int sockfd)
{
		int yes = 1;
		if (setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) == -1)
		{
			logg(ISSUE, "setsockopt TCP_NODELAY: %s\n", strerror(errno));
		}
}

/** Currently, mainly used in the client(Caller) side for ending the call if the target is not online **/
private void setTcpKeepAlive(int sockfd)
{
		int yes = 1;
		if (setsockopt(sockfd, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)) == -1) {
			logg(ISSUE, "setsockopt SO_KEEPALIVE: %s\n", strerror(errno));
		}
}

private void setTcpReuse(int sockfd)
{
		int yes = 1;
		if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
			logg(ISSUE, "setsockopt SO_REUSE: %s\n", strerror(errno));
		}
}

/* need to free the result */
public RPCResponse* connect_conn(char* conn, RPCRequest* rpcRequest){
        RPCResponse* rpcResponse = NULL;
        char* ip_address = m_get_ip_address(conn);
        int port = get_port(conn);
        char buf[CONN_BUF_SIZE];
        struct sockaddr_in servaddr;
        int sockfd, n;

        sockfd = socket(AF_INET, SOCK_STREAM, 0);

        bzero(&servaddr, sizeof(servaddr));
        servaddr.sin_family = AF_INET;
        inet_pton(AF_INET, ip_address, &servaddr.sin_addr);
        servaddr.sin_port = htons(port);

        setTcpKeepAlive(sockfd);

        int ret = connect(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr));

        if (ret < 0) {
        	logg(ISSUE, "Failed to connect to server %s.", conn);
        	rpcResponse = create_rpc_response(CONN_FAIL, 0, NULL);
        }else{
			//process rpc request
			Buf* rpc_request_buf = rpc_request_to_byte(rpcRequest);
			write(sockfd, get_buf_data(rpc_request_buf), get_buf_index(rpc_request_buf));
			n = read(sockfd, buf, CONN_BUF_SIZE);
			//If n == 0, measn the remote node have encounter some bad problems
			if(n == 0){
				rpcResponse = create_rpc_response(REMOTE_FAIL, 0, NULL);
			}else{
				rpcResponse = byte_to_rpc_response(buf);
			}
			destory_buf(rpc_request_buf);
        }
        close(sockfd);

        free2(ip_address);
        return rpcResponse;
}

/** server part **/
private void start_daemon(int listenfd, RPCResponse* (*handler_request)(char *cmd, List* params)){
        struct sockaddr_in cliaddr;
        int connfd, n;
        char buf[CONN_BUF_SIZE];

        while(1){
			socklen_t cliaddr_len = sizeof(cliaddr);
			connfd = accept(listenfd, (struct sockaddr *)&cliaddr, &cliaddr_len);
			n = read(connfd, buf, CONN_BUF_SIZE);
			RPCRequest* rpcRequest = byte_to_rpc_request(buf);
			RPCResponse* rpcResponse = NULL;
			if(rpcRequest->cmd_name != NULL){
				logg(DEBUG, "The request is cmd:%s.", rpcRequest->cmd_name);
				rpcResponse = handler_request(rpcRequest->cmd_name, rpcRequest->params);
			}else{
				logg(ISSUE, "The request has not included necessary cmd.");
				rpcResponse = create_rpc_response(ERROR_NO_CMD, 0, NULL);
			}
			Buf* rpcResponseBuf = rpc_response_to_byte(rpcResponse);
			write(connfd, get_buf_data(rpcResponseBuf), get_buf_index(rpcResponseBuf));
			close(connfd);

			destory_rpc_request(rpcRequest);
			destory_rpc_response(rpcResponse);
			destory_buf(rpcResponseBuf);
        }
}

public void startup(int servPort, RPCResponse* (*handler_request)(char *cmd, List* params)){
        struct sockaddr_in servaddr;
        int listenfd = socket(AF_INET, SOCK_STREAM, 0);

        bzero(&servaddr, sizeof(servaddr));
        servaddr.sin_family = AF_INET;
        servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
        servaddr.sin_port = htons(servPort);

        setTcpReuse(listenfd);
        setTcpNoDelay(listenfd);

        if(bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) != 0){
			logg(ISSUE, "Failed to listen the port %d, Please change the port setting!", servPort);
			exit(-1);
        }

        listen(listenfd, 20);
        logg(INFO, "Starting the daemon and Accepting connection...");
        start_daemon(listenfd, handler_request);
}


#ifdef RPC_TEST
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
        RPCResponse* rpcResponse = create_rpc_response(SUCCESS,  5, m_cpy("test5"));
        Buf* buf = rpc_response_to_byte(rpcResponse);
        FILE *fp = fopen("test", "wb");
        fwrite(get_buf_data(buf), get_buf_index(buf), 1, fp);
		RPCResponse* newRPCResponse = byte_to_rpc_response(get_buf_data(buf));
		printf("%s\n", newRPCResponse->magic);
		printf("%d\n", newRPCResponse->status_code);
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
        Buf *buf = init_buf(1000);
        buf_cat(buf, &testInt, sizeof(int));
        buf_cat(buf, testChar, strlen(testChar));
        FILE *fp = fopen("test", "wb");
        fwrite(get_buf_data(buf), sizeof(int) + strlen(testChar), 1, fp);
        fclose(fp);
        fp = fopen("test", "rb");
        Buf *buf2 = init_buf(1000);
        fread(get_buf_data(buf2), 6, 1, fp);
        int testInt2 = buf_load_int(buf2);
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
