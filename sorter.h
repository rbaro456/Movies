/*****
*
*	Define structures and function prototypes for your sorter
*
*
*
******/

/*
***************************************************
Macro Constants
***************************************************
*/
///Suggestion: define a struct that mirrors a record (row) of the data set
#ifndef SORTER_H
#define SORTER_H

#define MAX_DB_SIZE 10000
#define MAX_STR 300
#define NUM_COLS 28 ///original number of columns
#define MAX_COLS 300


/*
***************************************************
Structs, Unions, Typedefs
***************************************************
*/

typedef	///entry (row) will have an array of these to represent its data
union _item_ptr{
	char (*s)[MAX_STR];	/// holds a string value
	int* i;  /// holds integer value
	float* f;  /// holds float value
}item_ptr;

typedef
struct _entry{
	char *types; ///keeps a reference of the types of each col: i, f, s, p
	item_ptr *items;  /// array that holds items for entry
	struct _entry *next;	///used for overflow
}Entry;

typedef
struct _db{
	char ** column_titles;  /// meta data for all columns
	char * column_types; ///keeps a reference of the types of each col: i(int), f(float), s(string)
	int num_cols, num_rows;
	int size;  /// do not need to worry about
	Entry ** entries;  /// contains array of entry pointers
	struct _db *next;	///used for overflow
}Db;


/*
***************************************************
functions in sorter.c
***************************************************
*/

int process_dir(char *dir_name, char *sort_col, char *out_dir); ///scans dir for dirs and .csv
int sort_csv(char *file_path, char *filename, char *sort_col, char *out_dir);
int populate_db(Db *db, FILE* fp); ///reads all data available in stdin into internal db
int determine_sort_col(Db* db, char * str); ///returns index of column name str, -1 if not present
int read_in_cols(Db *db, FILE* fp);	///asserts the column names are as described in project instructions
void print_cols(Db *db, FILE* fp);	///prints out column titles
void print_db(Db *db, FILE* fp);	///prints column titles, then calls print row on every row
void print_rows(Db *db, FILE* fp);
void print_row(Db *db, Entry *row, FILE* fp);	///prints contents of Entry (row) into one line
Db* make_new_db(void); ///file_path includes path and file name. filename is str of filename.csv
Entry* make_new_entry(void);
int dealloc_entry(Entry *entry);
int dealloc_db(Db *db);	///deallocs all linked databases, every entry and every pointer in each
void print_types(Db *db); ///for debugging, prints out the types of data in each column
int write_csv(Db *db, FILE *fdest);
void segf(int); ///handles seg faults

/*
***************************************************
functions in mergesort.c
***************************************************
*/

int my_mergesort(Db *db, int col, char type, int start, int stop);
int int_compare(item_ptr pa, item_ptr pb);
int float_compare(item_ptr pa, item_ptr pb);
int str_compare(item_ptr pa, item_ptr pb);

#endif
