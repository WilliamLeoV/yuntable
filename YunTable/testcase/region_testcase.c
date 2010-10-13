#include "global.h"
#include "utils.h"
#include "item.h"
#include "list.h"
#include "region.h"
#include "conn.h"
#include "conf.h"

void test_suite(void){
	printf("The testsuit for region\n");
	char* region_conn =  "127.0.0.1:8302";
	char* table_name = "table1";
	char* column_family1 = DEFAULT_COLUMN_FAMILY;
	List* params1 = generate_charactor_params(2, table_name, column_family1);
	char* result1 = connect_conn(region_conn, ADD_NEW_TABLET_REGION_CMD, params1);
	boolean bool1 = stob(result1);
	printf("The result for add new tablet:%s\n", bool_to_str(bool1));
	free(result1);

	char* column_family2 = m_cpy("address");
	List* params2 = generate_charactor_params(2, table_name, column_family2);
	char* result2 = connect_conn(region_conn, ADD_NEW_TABLET_REGION_CMD, params2);
	boolean bool2 = stob(result2);
	printf("The result for add new tablet:%s\n", bool_to_str(bool2));
	free(result1);

	List* params3 = generate_charactor_params(1, table_name);
	char* column_family_list_string = connect_conn(region_conn, GET_COLUMN_FAMILYS_CMD, params3);
	printf("column_family_list_string:%s\n", column_family_list_string);

	char* avail_space_string =connect_conn(region_conn, AVAILABLE_SPACE_REGION_CMD, NULL);
	printf("avail_space_string:%s\n", avail_space_string);

	Item** items = malloc(sizeof(Item *) * 1);
	items[0] = m_create_item("1","2","3","4");
	Buf* buf = result_set_to_byte(m_create_result_set(1, items));
	add_param(params1, get_buf_index(buf), get_buf_data(buf));
	char* result5 =  connect_conn(region_conn, PUT_DATA_REGION_CMD, params1);
	printf("result5:%s\n", result5);

	char* row_key = m_cpy("1");
	List* params = generate_charactor_params(2, table_name, row_key);
	byte* result =  connect_conn(region_conn, QUERY_ROWS_REGION_CMD, params);
	ResultSet* resultSet = byte_to_result_set(result);
	printf("%s\n", get_value(resultSet->items[0]));

	printf("Completed Successfully\n");
}


int main(int argc, char *argv[]){
	test_suite();
	return 1;
}
