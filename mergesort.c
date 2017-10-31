#include <stdio.h>
#include <string.h>
#include "sorter.h"

/*
    **************************************
	 			my_mergesort
	**************************************
	db is a struct. The rows are stored in db->entries, which is an array of pointers to Entry structs, each entry has the data in entry->items[i], which is an array of pointers of type item_ptr, which point to the data. This function receives a pointer to db. "col" is the index of the column to be sorted by: db->entries[i][col] is the data you will use to sort. "type" is the type of the data in that column (see line 26). "start" and "stop" are the indices for merge sort. The recursion is being called on line 37, inside the if statement.
	my_mergesort returns 1 if successful, 0 if something goes wrong.
*/


int my_mergesort(Db *db, int col, char type, int start, int stop){
	if(stop-start<0) {
		printf("Error in mergesort: stop-start < 0\n");
		return 0;
	}
	if(stop-start==0){ // base case
// 		printf("Base Case. returning 1\n");
		return 1;
	}
	int midpoint = start+(stop-start)/2;
// 	printf("Merge Sort: midpoint: %d. sending: %d,%d  %d,%d\n", midpoint, start, midpoint, midpoint+1, stop);
	
	//assign compare function based on type of data for column
	int (*compare)(item_ptr,item_ptr);
	if(type=='i'){
		compare = int_compare;
	}else if(type=='f'){
		compare = float_compare;
	}else{
		compare = str_compare;
	}
	
	//sort both halves, return if either fails:
	if(!my_mergesort(db, col, type, start, midpoint) || !my_mergesort(db, col, type,  midpoint+1, stop)){	
		printf("Merge Sort: Error: merging failed\n");
		return 0;
	}
	
	//do the sorting
	Entry *temparr[stop-start+1];
	int i, j, k;
	Db* which_db = db;
	int start_from = start;
	for(i=0; i<=stop-start; i++){
// 		printf("copying %d\n",i);
		while((start_from+i)>=MAX_DB_SIZE){
			which_db = which_db->next;
			start_from -= MAX_DB_SIZE;
// 			printf(".\n");
		}
		temparr[i] = which_db->entries[start_from+i];
	}
	which_db = db;
	
// 	for(i=0; i<stop-start+1; i++){
// 		printf("mergin: i: %d start:%d, stop:%d %d\n", i, start, stop, *(temparr[i]->items[col].i));
// 	}
	
// 	printf("iter, start: %d, stop:%d\n", start, stop);
	for(i=0, j=midpoint-start+1, k=start; j<=stop-start && i<midpoint-start+1;){
		while(k>=MAX_DB_SIZE){
			which_db = db->next;
			k -= MAX_DB_SIZE;
// 			printf("k.\n");
		}
// 		printf("merge: comparing i:%d to j:%d, k:%d, col:%d\n", i, j, k, col);
// 		printf("AAAAAAAAA%i to %i\n", *(temparr[i]->items[col].i), *(temparr[j]->items[col].i));
		if(compare(temparr[i]->items[col], temparr[j]->items[col])<1){
// 			printf("-1,0\n");
			which_db->entries[k] = temparr[i];
			i++;
			k++;
		}else{
// 			printf("1, j:%d, %d, %d\n", j, *(db->entries[k]->items->i) ,*(temparr[j]->items->i));
			which_db->entries[k] = temparr[j];
			j++;
			k++;
		}
	}
	
	//tack on remainder to back of sorted array
	while(i<midpoint-start+1){
// 		printf("incrementing i%d\n", i);
		which_db->entries[k] = temparr[i];
		i++;
		k++;
	}
	while(j<=stop-start){
// 		printf("incrementing j%u\n", j);
		which_db->entries[k] = temparr[j];
		j++;
		k++;
	}
	
	return 1;
}

int int_compare(item_ptr pa, item_ptr pb){
	int a = *(pa.i);
	int b = *(pb.i);
	if(a==b) return 0;
	else if(a<b) return -1;
	else return 1;
	
}

int float_compare(item_ptr pa, item_ptr pb){
	float a = *(pa.f);
	float b = *(pb.f);
	if(a==b) return 0;
	else if(a<b) return -1;
	else return 1;
	
}

int str_compare(item_ptr pa, item_ptr pb){
	char* a = *pa.s;
	char* b = *pb.s;
	if(a[0]=='\"') a++;	//skip quotes when applicable
	if(b[0]=='\"') b++;
	return strcmp(a, b);
}