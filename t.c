//writter: wang zhishang
//assignment 4

#include <stdio.h>
#include <dirent.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdbool.h>
#include "hashmap.h"

#define MAXLINE 1024

int numfiles = 0;
int numthreads;
int filesProcessed;
char **filenames;	//array	of file	names
char * directory;
hashmap* result;
hashmap* hms[100];

pthread_mutex_t mutex;
pthread_t	*threads; //array of threads

void processFile(int,long);


struct Args
{
	int start;
	int end;
};
struct entry
{
	char* word;
	int count;
};

unsigned long
hash(unsigned char *str)
{
    unsigned long hash = 5381;
    int c;

    while (c = *str++)
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return hash;
}

void* p1(void *arg) {
	long id = (long)arg;
	int myfile;
	printf("my id %d\n", id );
	bool moreFiles = true;
	while (moreFiles) {
	 	pthread_mutex_lock(&mutex);
	 	if(filesProcessed == numfiles)	{
			moreFiles = false;
			pthread_mutex_unlock(&mutex);
	 	}
	 	else {
			myfile = filesProcessed;
			filesProcessed++;
			pthread_mutex_unlock(&mutex);
			processFile(myfile,id);
			//pthread_mutex_unlock(&mutex);
	 	}
	}
	pthread_exit(NULL);
}

void processFile(int myfile,long threadId){
	char* filename = filenames[myfile];
	hashmap* hm  = hms[threadId];
	//hm = hashmapCreate(1024);
	int i;
	//set file path
	FILE *fp;
	char * fullpath;
	fullpath = malloc(strlen(directory)+strlen(filename)+1);
	fullpath[0] = '\0';
	strcat(fullpath,directory);
	strcat(fullpath,filename);

   	fp = fopen(fullpath,"r");
    char buf[50];

    while(fscanf(fp, "%s", buf) != EOF )
    {
    	//printf("the word: %s\n", buf);
    	char * tmp = malloc(sizeof(strlen(buf)+1));
    	strcpy(tmp,buf);
        long hashcode= hash(tmp);
        struct entry* e = (struct entry*)hashmapGet(hm,hashcode);
        if (e)
        {
        	e -> count ++;
        }else{
        	struct entry* new= (struct entry *)malloc(sizeof(struct entry));
        	new -> word = tmp;
			new -> count = 1;
			hashmapInsert(hm, new, hashcode);
        }
    }
   	fclose(fp);

 //   	struct entry* e1 = hashmapGet(hm,6953778718942);
	// if (!e1){
	// 	printf("not found\n");
	// }else{
	// 	printf("%s found!!!\n", e1->word);
	// }


}

void mergeTwoHM(hashmap* dest,hashmap* sourse){
	int i;
	long* entries = returnAll(sourse);
	struct entry* e1;
	struct entry* e2;
   	for (i = 0; i < hashmapCount(sourse); i++)
	{
		e1 = (struct entry*)hashmapGet(sourse,entries[i]);
		e2 = (struct entry*)hashmapGet(dest,entries[i]);
		if (e2)
		{
        	e2 -> count = e1 -> count + e2 -> count ;
        }else{
        	struct entry* new= (struct entry *)malloc(sizeof(struct entry));
        	new -> word = e1 -> word;
			new -> count = e1 -> count;
			hashmapInsert(dest, new, entries[i]);
        }
	}


}

void* merge(void* args){
	int mid;
	int* left = (int*)malloc(sizeof(int));
	int* right = (int*)malloc(sizeof(int));
	int* returnId = (int*)malloc(sizeof(int));
	struct Args* a = (struct Args*)args;
	int start = a->start;
	int end = a->end;
	if (end == start)
	{
		*returnId = start;
		pthread_exit((void*)returnId);
	}
	if (end - start < 2)
	{
		mergeTwoHM(hms[start], hms[end]);
		*returnId = start;
		pthread_exit((void*)returnId);
	}

	pthread_t p1;
	pthread_t p2;
	mid = (end - start)/2;
	a = (struct Args*)malloc(sizeof(struct Args*));
	a->start = start;
	a->end = mid;
	pthread_create(&p1,NULL,merge,(void*)a);
	a = (struct Args*)malloc(sizeof(struct Args*));
	a->start = mid+1;
	a->end = end;
	pthread_create(&p2,NULL,merge,(void*)a);

	pthread_join(p1, (void	**)	&left);
	pthread_join(p2, (void	**)	&right);

	mergeTwoHM(hms[*left], hms[*right]);
	*returnId = *left;
	pthread_exit((void*)returnId);


}

//./a.out 2 /Users/wangzhishang/sourse/data/
int	main(int argc, char *argv[]){
	if (argc < 3) exit(1);
	numthreads = atoi(argv[1]);
	threads = (pthread_t *) malloc(sizeof(pthread_t)*numthreads);
	long i;
	int j;
	//open dir and read filenames
	filenames = (char **) malloc(sizeof(char *)*100);
	DIR *d;
	struct dirent *entry;
	directory = argv[2];
	d = opendir(directory);
	while((entry = readdir(d)))	{
		if (entry->d_name[0] != '.') {
			char * name = entry->d_name;
			filenames[numfiles] = (char *) malloc(sizeof(char)	*(strlen(name)+1));
			strcpy(filenames[numfiles],name);
	 		numfiles++;
		}
	}
	closedir(d);

	//init hashmaps
	for (i = 0; i < numthreads; i++)
	{
		hms[i] = hashmapCreate(20000);
	}

	filesProcessed	=	0;

	pthread_mutex_init(&mutex, NULL);

	//phase one
	for	(i = 0 ; i < numthreads ; i++)
	{
	 	pthread_create(&threads[i],	NULL, p1, (void*)i);
	}

	for	(i = 0; i < numthreads ; i++)
	{
	 	pthread_join(threads[i], NULL);
	}
	pthread_mutex_destroy(&mutex);

	// for (i = 0; i < numthreads; i++)
	// {
	// 	hashmap* hm = hms[i];
	// 	printf("the hashmap count is %ld\n",hashmapCount(hm));
	// 	long* entries = returnAll(hm);
	// 	struct entry* e1;
 //   		for (j = 0; j < hash  mapCount(hm); j++)
	// 	{
	// 		e1 = (struct entry*)hashmapGet(hm,entries[j]);
	// 		printf("%s : %d\n",e1->word,e1->count);
	// 	}

	// }

	//merge the hashmaps
	pthread_t pt;
	int* returnId = (int*)malloc(sizeof(int));

	struct Args* a = malloc(sizeof(struct Args));
	a->start = 0;
	a->end = numthreads-1;
	pthread_create(&pt,NULL,merge,(void*)a);
	pthread_join(pt,(void**)&returnId);

	//file output
	FILE* out;
	out = fopen(argv[3], "w");
	hashmap* hm = hms[0];
	printf("the hashmap count is %ld\n",hashmapCount(hm));
	long* entries = returnAll(hm);
	struct entry* e1;
   	for (j = 0; j < hashmapCount(hm); j++)
	{
		e1 = (struct entry*)hashmapGet(hm,entries[j]);
		fprintf(out, "%s: %d\n",e1->word,e1->count);
		printf("%s : %d\n",e1->word,e1->count);
	}

	close(out);
	return 0;


}





