#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/wait.h>
#include "sorter.h"

const char * assignment_column_titles[NUM_COLS] = {"color", "director_name", "num_critic_for_reviews", "duration", "director_facebook_likes", "actor_3_facebook_likes", "actor_2_name", "actor_1_facebook_likes", "gross","genres", "actor_1_name", "movie_title", "num_voted_users", "cast_total_facebook_likes", "actor_3_name", "facenumber_in_poster", "plot_keywords", "movie_imdb_link", "num_user_for_reviews", "language", "country", "content_rating", "budget", "title_year", "actor_2_facebook_likes", "imdb_score", "aspect_ratio", "movie_facebook_likes"};

// color,director_name,num_critic_for_reviews,duration,director_facebook_likes,actor_3_facebook_likes,actor_2_name,actor_1_facebook_likes,gross,genres,actor_1_name,movie_title,num_voted_users,cast_total_facebook_likes,actor_3_name,facenumber_in_poster,plot_keywords,movie_imdb_link,num_user_for_reviews,language,country,content_rating,budget,title_year,actor_2_facebook_likes,imdb_score,aspect_ratio,movie_facebook_likes

/* 
To Do:

- writeup




Ask professor:
	- only movie csv? exact titles?
	- can print watever to stdout? doesn't interfere with required output?
	- wait() was working without include sys/wait.h
	- catch segfault and do what? now exits

done:
- make it scan starting from dirs other than current (it does parse from argv)
- make it output file to other dir
- check for zombie/orphan processes (ps aux)
- double check dealloc db (make sure everything is freed, run fsanitize, valgrind)
- catch segfault, exit on empty file
- program currently prints a 0 in place of fields found empty
- add csv validity checker (now it gets caught in an infinite loop on empty .csv, doesn't check if all rows have same number of fields)
- correct STDOUT output for final submission (done, but will need redoing after extra credit)

*/



int main(int argc, char * argv[]){
	signal(SIGSEGV, segf);
	int pipefd[2];
	pipe(pipefd);
	if(argc<3){
		fputs("\nError: Not enough arguments entered.\nUsage: ./sorter -c <column_name>\n\n", stderr);
		return 0;
	}
	//parse args/flags
	char *sort_col = "";
	char *dir_name = ".";	//default to current dir
	char *out_dir = "./";  //default to current dir
	int i;
	for(i=1; i<argc; i++){
		if(argv[i][0] == '-'){
			switch(argv[i][1]){
				case 'c':
					sort_col = argv[++i];
// 					printf("col: %s\n", sort_col);
					break;
				case 'd':
// 					strcat(dir_name, argv[++i]);
					dir_name = argv[++i];
// 					printf("dir: %s\n", dir_name);
					break;
				case 'o':
// 					strcat(out_dir, argv[++i]);
					out_dir = argv[++i];
// 					printf("out_dir: %s\n", out_dir);
					break;
				default:
					printf("Unknown flag: %c\n", argv[i][1]);
			}
		}
	}

	
	return process_dir(dir_name, sort_col, out_dir);
	
}

int process_dir(char *dirname, char *sort_col, char *out_dir){
	
	char *dir_name = (char*)malloc(100*sizeof(char)); //will append to this to send to recursion
	int child_processes = 0;
	int *children_PIDS = (int*)malloc(255*sizeof(int));
	int *cur_childpid = children_PIDS;
	int PID = 0;
	strcpy(dir_name, dirname);
	strcat(dir_name, "/");
	DIR *d = opendir(dirname);
	struct dirent *dir_item = readdir(d);
// 	printf("dirname: %s\n", dirname);
	while (dir_item){
// 		if(strcmp(dir_item->d_name,".")==0) printf("this dir:\n");
// 		else if (strcmp(dir_item->d_name, "..")==0) printf("prev dir:\n");
// 		printf("In dir: %s dir_item: %s \n", dirname, dir_item->d_name);
		if(strcmp(dir_item->d_name,".")!=0 && strcmp(dir_item->d_name, "..")!=0 && dir_item->d_type==4){
// 			printf("Found dir: %s in dir: %s, forking process\n", dir_item->d_name, dir_name);
			strcat(dir_name, dir_item->d_name);
// 			printf("calling with dir_name: %s\n", dir_name);
			PID = fork();
			if(!PID){ //child
// 				printf("child calling process dir with dir_name: %s\n", dir_name);
// 				children_PIDS = (int*)malloc(255*sizeof(int));
// 				cur_childpid = children_PIDS;
				process_dir(dir_name, sort_col, out_dir);
// 				printf("child exiting\n");
				_exit(0);
			}else{ //parent
// 				if(child_processes==0) 
// 					printf("Initial PID: %d\nPIDS of all child processes: ", getpid());
// 				else
// 					printf(",");
				child_processes++;
// 				printf("CHILD PID: %d\n", PID);
				*(cur_childpid++) = PID;
			}
		}
		if(dir_item->d_name[strlen(dir_item->d_name)-4]=='.' && dir_item->d_name[strlen(dir_item->d_name)-3]=='c' && dir_item->d_name[strlen(dir_item->d_name)-2]=='s' && dir_item->d_name[strlen(dir_item->d_name)-1]=='v'){
// 			printf("Found .csv!\n");
			PID = fork();
			if(!PID){ //child
// 				printf("child forking process SORT on %s\n", dir_item->d_name); 
				strcat(dir_name, dir_item->d_name);
				sort_csv(dir_name, dir_item->d_name, sort_col, out_dir);
// 				child_processes++;
// 				printf("child exiting\n");
				_exit(0);
			}else{ //parent
// 				if(child_processes==0) 
// 					printf("Initial PID: %d\nPIDS of all child processes: ", getpid());
// 				else
// 					printf(",");
				child_processes++;
// 				printf("Child PID: %d\n", PID);
				*(cur_childpid++) = PID;
			}
		}
		strcpy(dir_name, dirname);
		strcat(dir_name, "/");
		dir_item = readdir(d);
		
	}
// 	printf("--Done with dir: %s\n", dirname);
	closedir(d);
	
// 	printf("144 Child Processes: %d\n", child_processes);
	int i;
	for(i=0; i<child_processes; i++){
		wait(NULL);
// 		printf("Child exited\n");
	}
	printf("Initial PID: %d\nPIDS of all child processes: ", getpid());
	if(child_processes>0){
		cur_childpid = children_PIDS;
		for(i=0; i<child_processes-1; i++){
			printf("%d,", *cur_childpid++);
		}
		printf("%d\n", *cur_childpid);
	}else
		printf("\n");
	printf("Total number of processes: %d\n", child_processes);
	free(children_PIDS);
	return 0;
}

int sort_csv(char *file_path, char *filename, char *sort_col, char *out_dir){
	
// 	printf("\n\n");
	printf("Initial PID: %d\nPIDS of all child processes: \nTotal number of processes: 0\n", getpid());
	FILE *fp = fopen(file_path, "r");
	Db *db = make_new_db();
	read_in_cols(db, fp);
// 	printf("Sorting file %s\n", filename);
	int assert = assert_cols(db);
	int col_num = determine_sort_col(db, sort_col);
	if(col_num == -1){
// 		printf("Specified column not present.\n");
		return 0; // change to exit when forking
// 	}else{
// 		printf("Specified column is index %d.\n", col_num);
	}
	populate_db(db, fp);
// 	printf("sort_csv: sort_col %s is %d\n", sort_col, col_num);
	//clear db, set all cell pointers to NULL
// 	print_db(STDOUT, db);
// 	printf("sort_csv: numcols:%d numrows:%d type:%c\n", db->num_cols, db->num_rows, db->column_types[col_num]);
	my_mergesort(db, col_num, db->column_types[col_num], 0, db->num_rows-1);
// 	print_db(STDOUT, db);
// 	printf("sorted\n");
	
	//create output filename
	char *postpend = (char*)malloc(strlen("-sorted-")+strlen(sort_col)+50);
	strcpy(postpend, "-sorted-");
	strcat(postpend, sort_col);
// 	printf("postpend:%s\n",postpend);
	char *dest_filename = (char*)malloc(strlen(filename)+strlen(postpend)+50);
// 	printf("dest_filename: 1 %s\n",dest_filename);
	strcpy(dest_filename, filename);
// 	printf("dest_filename: 2 %s strlen: %lu\n",dest_filename, strlen(dest_filename));
	dest_filename[strlen(dest_filename)-4] = '\0';
// 	printf("dest_filename: 3 %s strlen: %lu\n",dest_filename, strlen(dest_filename));
	strcat(dest_filename, postpend);
// 	printf("dest_filename: 4 %s strlen: %lu\n",dest_filename, strlen(dest_filename));
	strcat(dest_filename, ".csv");
// 	printf("dest_filename: %s\n",dest_filename);
// 	printf("dest_filename: %s, strlen: %lu \n",dest_filename, strlen(dest_filename));
	free(postpend);
	char *dest_path = (char*)malloc(sizeof(dest_filename)+200);
// 	printf("DEST_PATH %s\n", dest_path);
	strcpy(dest_path, out_dir);
	if(dest_path[strlen(dest_path)-1]!='/')
		strcat(dest_path, "/");
// 	printf("DEST_PATH %s\n", dest_path);
	strcat(dest_path, dest_filename);
// 	printf("DEST_PATH %s\n", dest_path);
	free(dest_filename);
	
	//write to output file
	FILE * fdest = fopen(dest_path, "w");
// 	write_csv(db, fdest);
	free(dest_path);
	print_db(db, fdest);
	fclose(fdest);
// 	printf("\n\n");
	dealloc_db(db);
	return 1;
}

int populate_db(Db *db, FILE* fp){
	int row, col, i, j, in_quotes;
	int next=0;
	int o = 1;
	char c, type;
	char *str;
	for(row=0; row<MAX_DB_SIZE; row++){	//for each row/(entry)
		if(o>0){
			db->num_rows++;
			db->size++;
// 			printf("%d\n", num_rows);
			// instantiate Entry
			db->entries[row] = make_new_entry();
			// make reference to item_ptr array (fields attribute in Entry, or array if diff cols)
			for(col=0; col<db->num_cols; col++){	//for each col
				str = (char*)calloc(MAX_STR, sizeof(char));
				type = 'i';
				db->entries[row]->items[col].s = (char (*)[MAX_STR])str;
				in_quotes = 0;
				for(i=0; i<MAX_STR && (o=fscanf(fp, "%c", &c))>0 && c!='\0' && (c!=',' || in_quotes==1); i++){
					if(c=='"'){
						if(in_quotes)
							in_quotes = 0;
						else
							in_quotes = 1;
					}else if(c=='\n' || c=='\r'){  //make next entry
						if(col==0){
							db->entries[row]->items[col].s = NULL;
							db->num_rows--; //getting rid of return at the end of the file
							goto Done; //line 113
						}
						if(col<db->num_cols-1){ //bad CSV
							fputs("Error: Row doesn't have enough columns. Bad CSV.\n", stderr);
							exit(0);
						}
						str[i] = '\0';
// 						printf("76 making new row\n");
						next++;
						if(db->column_types[col]=='\0'){
							db->column_types[col]=type;
						}else if(db->column_types[col]=='i' && type=='s'){
							db->column_types[col]='s';
						}else if(db->column_types[col]=='f' && type=='s'){
							db->column_types[col]='s';
						}
						if(type=='i'){
							int* num = (int*)malloc(sizeof(int));
							*num = atoi(str);
							db->entries[row]->items[col].i = num;
							free(str);
						}else if(type=='f'){
							float* num = (float*)malloc(sizeof(float));
							*num = atof(str);
							db->entries[row]->items[col].f = num;
							free(str);
						}
						db->entries[row]->types[col]=type;						
						goto Next_Row;	//go to next row in current entry, line 102
					}else if(c==',' && !in_quotes){ 
						if(type=='s') str[i] = '\0';
// 						printf("\n\n\n119 making new col\n\n");
// 						printf(", ");
						col++;
						break;	//go to next column in current entry
					}
					str[i] = c;
					//determine type of data(string, int, float)
					if(type=='i'){
						if(c=='.'){ //was int, now is float
							type = 'f';
						}else if(c<'0' || c>'9'){ //c is not an int, col is string
							if(c=='-' && i==0)
								;
							else
								type = 's';
						}
					}else if(type=='f'){
						if(c<'0' || c>'9'){ //c is not an int
							type = 's';
						}
					}
				}
// 				printf("populate: %s, type: %c\n", str, type);
				if(db->column_types[col]=='\0'){
// 					printf("2\n");
					db->column_types[col]=type;
				}else if(db->column_types[col]=='i' && type=='s'){
					db->column_types[col]='s';
				}else if(db->column_types[col]=='f' && type=='s'){
					db->column_types[col]='s';
// 				}else{
// 					db->column_types[col]=type;
				}
				
// 				printf("populate: db->column_types[%d]: %c\n", col, db->column_types[col]);
				// convert data to correct type
				if(type=='i'){
					int* num = (int*)malloc(sizeof(int));
					*num = atoi(str);
					db->entries[row]->items[col].i = num;
// 					printf("CONVERTING %s TO INT %d\n", str, *num);
					free(str);
				}else if(type=='f'){
					float* num = (float*)malloc(sizeof(float));
					*num = atof(str);
					db->entries[row]->items[col].f = num;
// 					printf("CONVERTING %s TO FLOAT %f\n", str, *num);
					free(str);
// 				}else{
// 					printf("STAYED STRING %s\n", str);
				}
				db->entries[row]->types[col]=type;
// 				types[col]=type;
// 			printf("went to, type:%c\n", type);
			if(type=='s'){ //clear trailing whitespace
				while(i>0 && (str[--i]==' '||str[i]=='\t')){
				}
				str[++i]='\0';
				i--;
// 				printf("str2 r:%d, c:%d, %c, %s\n",row, col, str[i], str);
				if(i>0 && (str[i]=='\"')){ //clear trailing whitespace inside quotes
					while(str[--i]==' '||str[i]=='\t'){
						str[i] = '\"';
						str[i+1] = '\0';
					}
				}
			}
			}
			Next_Row:
// 			printf("\nNEW ROW %d:\n", row);
			i++;
// 			print_types();
// 			print_row(db->entries[row]);
		}else{
// 			printf("num_rows: %d, next: %d, db_size:%d\n", num_rows, next, db->size);
			db->entries[row]=NULL;
			return 1;
		}
	}
	Done:
// 	printf("num_rows: %d, next: %d, db_size:%d\n", num_rows, next, db->size);
	if(row==MAX_DB_SIZE && o){
		db->next = (Db*)malloc(sizeof(Db));
		db->next->next = NULL;
		populate_db(db->next, fp);
	}
	return 1;
}

//find index of column selected by user to sort on
int determine_sort_col(Db* db, char * str){
	int i=0;
	while(i<db->num_cols){
// 		printf("determine_sort_col: comparing %s to %s, finding %d\n",db->column_titles[i], str,strcmp(db->column_titles[i], str));
		if(strcmp(db->column_titles[i], str)==0){
// 			printf("found col to sort: %d\n", i);
			return i;
		}
		i++;
	}
	return -1;
}

int read_in_cols(Db *db, FILE* fp){
	int i,j,o;
	char c = EOF;
	
// 	printf("read_in_cols: Reading cols in input...\n");
	//read column headings from file, populate db->column_titles list
	for(i=0, o=1; o && i<=MAX_COLS && (c!='\n' && c!='\r'); i++){
		db->num_cols++;
		db->column_titles[i] = (char*)malloc(MAX_STR*sizeof(char));
// 		printf("read_in_cols: db->num_cols:%d\n", db->num_cols);
		j=0;
		while((o=fscanf(fp, "%c", &c)) && (c!='\n' && c!='\r')){
			if(o<=0 || c==EOF){
// 				fprintf(stderr, "%c\n", c);
// 				fputs("Error: EMPTY FILE\n", stderr);
				_exit(0);
			}
// 			printf("READ: NUM COLS: %d\n", db->num_cols);
			if(c>=65 && c<=90) c+=32; //make char lower case.
// 			printf("Scanned: %c\n",c);
			if(c==','){	//reached end of current title
// 				printf("1\n");
				db->column_titles[i][j] = '\0';	//terminate string
// 				printf("read_in_cols: col title %d: %s\n", i, db->column_titles[i]);
				break;	//go to next column title
			}
// 			printf("2\n");
			db->column_titles[i][j] = c;
			j++;
		}
	}
	db->column_titles[--i][j] = '\0';
// 	printf("read_in_cols: col title %d: %s\n", i, db->column_titles[i]);
	// print_cols();
// 	printf("read_in_cols: Received %d cols\n", num_cols);
	
	return 1;
}

int assert_cols(Db *db){
	int i;
	if(db->num_cols != NUM_COLS){
// 		fprintf(stderr, "Error: PID %d number of Columns %d do not match orignal assignment %d. Continuing sort.\n", getpid(), db->num_cols, NUM_COLS);
		return 0;
	}
	for(i=0; i<NUM_COLS && i<db->num_cols; i++){
// 		fprintf(stderr, "cmp %s %s %d\n", assignment_column_titles[i], db->column_titles[i], strcmp(assignment_column_titles[i], db->column_titles[i]));
		if(strcmp(assignment_column_titles[i], db->column_titles[i])){
// 			fprintf(stderr, "Error: PID %d Columns[%d] do not match orignal assignment. Continuing sort.\n", getpid(), i);
			return 0;
		}
	}
	if(i==NUM_COLS){
// 		fprintf(stderr, "Error: PID %d all Columns match orignal assignment. Continuing sort.\n", getpid());
		return 1;
	}
	//should never happen
// 	fprintf(stderr, "\tError: PID %d still not asserted. Continuing sort.\n", getpid());
	return 0;
}

void print_cols(Db *db, FILE* fp){
// 	printf("\nColumn Titles: %d\n", num_cols);
	int i;
// 	printf("print_cols: db->numcols:%d\n", db->num_cols);
	for(i=0; i<db->num_cols; i++){
// 		printf("\nnumcols:%d i:%d:\n", db->num_cols, i);
// 		printf("%s", db->column_titles[i]);
		fprintf(fp, "%s", db->column_titles[i]);
		if(i<db->num_cols-1) fprintf(fp, ",");
	}
	fprintf(fp, "\n");
	return;
}

void print_db(Db *db, FILE* fp){ //prints whole database, one entry on each line
	print_cols(db, fp);
	print_rows(db, fp);
// 	printf("\nprinting db now:\n\n");
	if(db->next){
		print_rows(db->next, fp);
	}
	return;
}

void print_rows(Db *db, FILE* fp){
	int i;
	for(i=0; i<db->num_rows && i<MAX_DB_SIZE && db->entries[i]!=NULL; i++){
// 		printf("\n%d\n",i);
		print_row(db, db->entries[i], fp);
		if(i<(db->num_rows)-1) fprintf(fp, "\n");
	}
	
	return;
}

void print_row(Db *db, Entry *row, FILE* fp){	//prints contents of Entry (row) into one line
	int i, r;
	for(i=0, r=1; i<db->num_cols && row->items[i].s!=NULL;){
		if(db->column_types[i]=='i'){
// 			printf("%d", *((row->items[i]).i));
			fprintf(fp, "%d", *((row->items[i]).i));
// 			printf("d%c:%d",row->types[i] , *((row->items[i]).i));
		}else if(db->column_types[i]=='f'){
// 			printf("%1.3f", *((row->items[i]).f));
			fprintf(fp, "%1.3f", *((row->items[i]).f));
// 			printf("f%c:%f",row->types[i] , *((row->items[i]).f));
		}else{
// 			printf("%s", *((row->items[i]).s));
			if(*((row->items[i]).s)!=0)
				fprintf(fp, "%s", *((row->items[i]).s));
// 			printf("s%c:%s",row->types[i] , *((row->items[i]).s));
		}
		++i;
		if(row->items[i].s==NULL || (i==db->num_cols && row->next==NULL && ++r<db->num_rows)) 
// 			fprintf(fp, "\n");
			;
// 		else printf(",");
		else 
			fprintf(fp, ",");
	}
// 	printf("\r");
	
	if(row->next){
		fprintf(fp, "\n");
		print_row(db, row->next, fp);
	}
}

Db* make_new_db(void){
	Db *db = (Db*)malloc(sizeof(Db));
	db->num_cols = 0;
	db->num_rows = 0;
	db->column_titles = (char**)malloc(MAX_COLS*sizeof(char*));
	db->column_types = (char*)malloc(MAX_COLS*sizeof(char));
	db->entries = (Entry**)malloc(MAX_DB_SIZE*sizeof(Entry*));
	db->next = NULL;
	
	return db;
}

Entry* make_new_entry(void){
	
	Entry *entry = (Entry*)malloc(sizeof(Entry));
	entry->types = (char*)malloc(MAX_COLS);
	entry->items = (item_ptr*)malloc(MAX_COLS*sizeof(item_ptr));
	entry->next = NULL;
	
	return entry;
}

int dealloc_entry(Entry *entry){
	if(entry->next)
		dealloc_entry(entry->next);
	free(entry->types);
	free(entry->items);
	free(entry);
	return 0;
}

int dealloc_db(Db *db){
	int i, j;
	if(db->next) {
		dealloc_db(db->next);
		free(db->next);
	}
	for(i=0; i<db->num_cols; i++){
		free(db->column_titles[i]);
	}
	free(db->column_titles);
	free(db->column_types);
	
	for(i=0; i<MAX_DB_SIZE && i<db->num_rows; i++){
// 		for(j=0; j<db->num_cols; j++){//db->size
// 			printf("i:%d j:%d freeing \n",i,j);
			if(db->entries[i]){
// 				printf("freeing: i:%i, j:%i\n", i, j);
				dealloc_entry(db->entries[i]);
// 			}
		}
// 		free(db->entries[i]);
	}
	free(db);
	return 0;
}

void print_types(Db *db){
	int i;
	for(i=0; i<db->num_cols; i++){
		printf("%c: %c  ", 'a'+i, db->column_types[i]);
	}
	printf("\n");
}

void segf(int i){ //handles seg faults
	printf("PID:%d caused a segfault\n", getpid());
// 	signal(SIGSEGV, SIG_DFL);  //return to default signal handler
	exit(0);
}