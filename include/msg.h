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

#ifndef MSG_H_
#define MSG_H_


enum {
    OK = 0,
    SUCCESS = 0,
    ERR_NO_CMD_INPUT= 1,
    ERR_CMD_NOT_COMPLETE,
    ERR_NULL_STRING,
    ERR_WRONG_ACTION,
    ERR_NO_COLUMN,
    ERR_NO_TABLE_NAME,
    ERR_NO_ROW_KEY,
    ERR_PUT,
    ERR_TABLE_NOT_CREATE,
    ERR_NO_MASTER,
    ERR_CLUSTER_FULL,
    ERR_WRONG_MASTER,
    ERR_WRONG_REGION,
    ERR_WRONG_ADD_CMD,
    ERR_NOTHING_FOUND,
    ERR_EXIST,
    ERR_NO_EXIST,
    ERR_INVAL,
    ERR_MAX,
};


#endif /* MSG_H_ */
