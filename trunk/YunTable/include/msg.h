#ifndef MSG_H_
#define MSG_H_

/**
 * This File includes all messages for console,
 **/

/** Error Messages **/
#define ERR_MSG_NO_CMD_INPUT "PLease attach cmd string if you want to use the silent mode."
#define ERR_MSG_CMD_NOT_COMPLETE "Please input the complete cmd."
#define ERR_MSG_NULL_STRING "The input string is NULL or invalid."
#define ERR_MSG_WRONG_ACTION "The action you just input is not supported. Please type \"help\" for more information."
#define ERR_MSG_NO_COLUMN "No column has been inputed."
#define ERR_MSG_NO_TABLE_NAME "No table name has been inputed."
#define ERR_MSG_NO_ROW_KEY "Should have a row key."
#define ERR_MSG_PUT "The Put Data action has failed."
#define ERR_MSG_TABLE_NOT_CREATED "pLease create the table first."
#define ERR_MSG_NO_MASTER "Please set up a master at first"
#define ERR_MSG_CLUSTER_FULL "Can not get a new region for this table because the whole cluster is full."
#define ERR_MSG_WRONG_MASTER "Either the inputted master is wrong or has some connection problem."
#define ERR_MSG_WRONG_REGION "Either the inputted region is wrong, has some connection problem or already existed."
#define ERR_MSG_WRONG_ADD_CMD "The Add Command is not valid, for example, the table name contains space"

/** Issue Messages **/
#define ISSUE_MSG_NOTHING_FOUND "Nothing has been found."
#define ISSUE_MSG_TABLE_ALREADY_EXISTED "The table has already existed."

/** Success Messages **/
#define SUCC_MSG_COMPLETED "The Action has been completed successfully."
#define SUCC_MSG_TABLE_CREATED "The new table has been created successfully."

#endif /* MSG_H_ */
