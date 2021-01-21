/*I declare that this piece of work which is the basis 
for recognition of achieving learning outcomes in the 
OPS1 course was completed on my own.[Michal Filipiak] [305778]*/
#define _XOPEN_SOURCE 500
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <dirent.h>
#include <ftw.h>
#include <time.h>
#include<sys/mman.h>
#define DEFAULT_MIN 1
#define DEFAULT_MAX 10 //change it to 1000
#define DEFUALT_INTERVAL 600
#define DEFAULT_RECURSION 0
#define DEFAULT_INPUT_SIZE 100
#define MAX_DESCRIPTORS 20
#define ERR(source) (fprintf(stderr,"%s:%d\n",__FILE__,__LINE__),\
                     perror(source),kill(0,SIGKILL),\
                                     exit(EXIT_FAILURE))

void usage(char *name) 
{
    printf("USAGE: %s [-r] [-m min=1] [-M max=1000] [-i interval=600] dir1 [dir2...]\n", name);
    exit(EXIT_FAILURE);
}

typedef struct _query_data
{
	char** dirs;
	int* queries;
	pthread_t tid;
	int q_count;
	int d_count;
}query_data;

typedef struct _data
{
	time_t big_bang;
	time_t start;
	time_t end;
	pthread_t tid;
	int status;
    char* dir;
    int r;
    int m;
    int M;
    int i;
    sigset_t mask;
} data;


typedef struct _entry
{
	char* directory;
	int* offsets;
	int offset_no;
}entry;

typedef struct _number
{
	entry* entries;
	int entries_no;

}number;

typedef struct _record
{
	number* numbers;
	char* root_dir;
	int r;
	int m;
	int M;

}record;

record idx;






char* strConcat(const char *s1, const char *s2)
{
	//strcat but without modifying arguments
    char *result = malloc(strlen(s1) + strlen(s2) + 1);
    if(result == NULL)ERR("MALLOC");
    strcpy(result, s1);
    strcat(result, s2);
    return result;
}


char* intToAsciiString(int number)
{
	int temp = number;
	int count = 0;
	while(temp)
	{
		temp/=10;
		count++;
	}
	char* res = (char*)malloc(sizeof(char)*(count+1));
	if(res==NULL) ERR("MALLOC");
	for(int i=count-1;i>=0;i--)
	{
		res[i] = '0' + number%10;
		number/=10;
	}
	res[count] = '\0';
	return res;
}

int searchFile(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
	if(typeflag == FTW_D && idx.r ==0 && strcmp(fpath,idx.root_dir)!=0)//change this somehow?
		return FTW_SKIP_SUBTREE;
	if(typeflag == FTW_F)
	{
		FILE* f;
		if((f = fopen(fpath, "r")) == NULL) ERR("FOPEN");
		char* file = (char*)malloc(sizeof(char)*(sb->st_size+1));
		if(file == NULL) ERR("MALLOC");
		if(fread(file, sizeof(char), sb->st_size,f)!=sb->st_size) ERR("FREAD");
		if(fclose(f)!=0) ERR("FCLOSE");
		file[sb->st_size] = '\0';
		for(int i=idx.m;i<=idx.M;i++)
		{
			char* ascii_i = intToAsciiString(i); //free this later
			char* position = strstr(file, ascii_i);
			entry ent;
			ent.offset_no=0;
			ent.offsets = NULL;
			while(position!=NULL)
			{
				int offset =  position-file;
				ent.offset_no++;
				int *tmp = realloc(ent.offsets, sizeof(int)*ent.offset_no);
				if(tmp)
					ent.offsets = tmp;
				else ERR("REALLOC");
				ent.offsets[ent.offset_no-1] = offset;
				position = strstr(file+offset+1,ascii_i);
			}
			if(ent.offset_no>0)
			{
				ent.directory = strConcat(fpath,"");
				
				number *num = &(idx.numbers[i-idx.m]);
				num->entries_no++;
				entry *tmp = (entry*)realloc(num->entries, sizeof(entry)*num->entries_no);
				if(tmp) 
					num->entries = tmp;
				else ERR("REALLOC");
				num->entries[num->entries_no-1] = ent;
			}
			free(ascii_i);
		}
		free(file);
	}
	return 0;

}

void writeIndex(char* dir)
{

	FILE *f;
	char* temp_index_dir = strConcat(dir, "/.temp_numf_index");

	if ((f = fopen(temp_index_dir,"w"))==NULL) ERR("FOPEN");

	for(int i=idx.m;i<=idx.M;i++)
	{
		if(fwrite(&i,sizeof(int),1,f)!=1) ERR("FWRITE");//write current number
	
		number *current_number = &idx.numbers[i-idx.m];

		if(fwrite(&current_number->entries_no,sizeof(int),1,f)!=1) ERR("FWRITE");//write number of directories

		for(int j=0;j<current_number->entries_no;j++)
		{
			entry *current_entry = &current_number->entries[j];

			int dir_size = strlen(current_entry->directory)+1;

			if(fwrite(&dir_size,sizeof(int),1,f)!=1) ERR("FWRITE");//write sizeof dir (with nullbyte)

			if(fwrite(current_entry->directory,sizeof(char)*dir_size,1,f)!=1) ERR("FWRITE");//write dir

			if(fwrite(&current_entry->offset_no,sizeof(int),1,f)!=1) ERR("FWRITE");//write no of offsets

			if(fwrite(current_entry->offsets,sizeof(int),current_entry->offset_no,f)!=current_entry->offset_no) ERR("FWRITE");//write offset	
		}
		
	}
	if(fclose(f)!=0)ERR("FCLOSE");
	char* proper_index_dir = strConcat(dir,"/.numf_index");
	if(rename(temp_index_dir,proper_index_dir) !=0) ERR("RENAME");
	free(proper_index_dir);
	free(temp_index_dir);	
}

void findQuery(char* dir,int n)
{
	char* index_dir = strConcat(dir,"/.numf_index");
	FILE *f;
	errno = 0;
	if((f=fopen(index_dir,"r"))==NULL)
	{
		free(index_dir);
		if(errno == ENOENT)
		{
			
			printf("There is no index file for directory: %s",dir);
			return;
		}
		else ERR("FOPEN");
	}
	free(index_dir);
	int current;
	int flag = 0;
	while(fread(&current,sizeof(int),1,f)==1)//reading number
	{
		if(current==n) flag=1;
		if(fread(&current,sizeof(int),1,f)!=1) ERR("FREAD");//reading number of directories
		int dir_no = current;
		for(int i=0;i<dir_no;i++)
		{
			if(fread(&current,sizeof(int),1,f)!=1) ERR("FREAD");//reading size of directory
			char *buffer;
			if((buffer = (char*)malloc(sizeof(char)*current))==NULL) ERR("MALLOC");
			if((fread(buffer,sizeof(char),current,f))!=current) ERR("FREAD");//reading directory
			if(flag) printf("%s: ",buffer);
			free(buffer);
			if(fread(&current,sizeof(int),1,f)!=1) ERR("FREAD");//reading number of offsets
			int offset_no = current;
			for(int i=0;i<offset_no;i++)
			{
				if(fread(&current,sizeof(int),1,f)!=1) ERR("FREAD");
				if(flag) printf("%d ",current);
			}
			if(flag) printf("\n");
		}		
		if(flag) break;
	}
	if(fclose(f)!=0) ERR("FCLOSE");
}

void scanDir(data* ch_data)
{
	
	int idx_size = ch_data->M - ch_data->m + 1;
	idx.m = ch_data->m;
	idx.M = ch_data->M;
	idx.r = ch_data->r;
	if((idx.numbers = (number*)malloc(sizeof(number)*idx_size))==NULL) ERR("MALLOC");
	idx.root_dir = strConcat(ch_data->dir,"");
	for(int i=0;i<idx_size;i++)
	{
		idx.numbers[i].entries_no = 0;
		idx.numbers[i].entries = NULL;
	}
	
	int flags = FTW_PHYS;
	if(ch_data->r == 0)
		flags |= FTW_ACTIONRETVAL;

	if(nftw(ch_data->dir, searchFile, MAX_DESCRIPTORS,flags)==-1)
		ERR("NFTW");
}


void *indexingThreadJob(void *voidPtr) 
{

	data *ch_data = voidPtr;
	printf("Starting indexing directory: %s\n",ch_data->dir);
	time(&ch_data->start);
	ch_data->status = 1;

	scanDir(ch_data);
	writeIndex(ch_data->dir);
	//freeGlobalData();
	ch_data->status = 0;
	time(&ch_data->end);
	printf("Finished indexing directory: %s\n",ch_data->dir);
	return NULL;
}

void *queryThreadJob(void *voidPtr)
{
	query_data *q_data = voidPtr;
	for(int i=0;i<q_data->q_count;i++)
	{
		printf("Number %d occurences:\n",q_data->queries[i]);
		for(int j=0;j<q_data->d_count;j++)
			findQuery(q_data->dirs[j],q_data->queries[i]);
	}
	return NULL;
}


void createIndexingThread(data *ch_data) 
{
    if(pthread_create(&ch_data->tid,NULL,indexingThreadJob,ch_data))
    	ERR("PTHREAD_CREATE");
}


void createQueryThread(query_data *q_data)
{
	if(pthread_create(&q_data->tid,NULL,queryThreadJob,q_data))
		ERR("PTHREAD_CREATE");
	if(pthread_join(q_data->tid,NULL)) ERR("PTHREAD_JOIN");
}

void waitForSignals(data* ch_data)
{

	char* pid_directory = strConcat(ch_data->dir, "/.numf_pid");
	int signo;
	for(;;)
	{
		

		if(sigwait(&(ch_data->mask), &signo)) ERR("SIGWAIT");
		switch(signo)
		{
			case SIGUSR1:
				if(ch_data->status == 1)
				{
					printf("Indexing in progress for directory: %s\n",ch_data->dir);
					printf("Time elapsed: %ld\n", time(NULL)- ch_data->start);
				}
				else printf("No indexing in progress for directory: %s\n",ch_data->dir);
				break;
			case SIGUSR2:
				if(ch_data->status ==0) 
					createIndexingThread(ch_data);
				else printf("Indexing already in progess for directory: %s\n",ch_data->dir);
				break;
			case SIGTERM:
				if(remove(pid_directory)<0) ERR("REMOVE");
				free(pid_directory);
				exit(EXIT_SUCCESS);
		}
		
		//fix this
		if((time(NULL)- ch_data->big_bang - ch_data->end)>ch_data->i && ch_data->status ==0)
		{
			printf("Time elapsed from the last indexing > %d\n",ch_data->i);
			createIndexingThread(ch_data);
		}
	}
	if(pthread_join(ch_data->tid,NULL)) ERR("PTHREAD_JOIN");
}



int createNumfPid(char* directory)
{
	char* pid_directory = strConcat(directory,"/.numf_pid");
	errno=0;
	int fd=open(pid_directory, O_RDWR | O_CREAT | O_EXCL, 0644);
	if(fd<0)
	{
		if(EEXIST == errno)
		{
			FILE *f;
			pid_t buffer;
			if((f = fopen(pid_directory,"r")) == NULL) ERR("FOPEN");
			if(fread(&buffer, sizeof(pid_t),1,f)!=1) ERR("FREAD");
			if(fclose(f)!= 0)ERR("FCLOSE");
			printf("ERROR: Other indexer is already processing this directory (PID:%d)\n",buffer);
			if(remove(pid_directory)<0) ERR("REMOVE");
			free(pid_directory);
			return 0;
		}
		else ERR("OPEN");
	}
	free(pid_directory);
	pid_t pid = getpid();
	write(fd,&pid,sizeof(pid_t));
	return 1;
}


int createNumfIndex(char* directory)
{
	char* index_directory = strConcat(directory, "/.numf_index");
	errno = 0;
	int fd = open(index_directory,  O_RDWR | O_CREAT | O_EXCL, 0644);
	free(index_directory);
   	if(fd<0)
   	{
   		if(EEXIST == errno)
   			return 0;
   		else ERR("OPEN");
   	}
   	if(close(fd)<0) ERR("CLOSE");

   	return 1;
}


void indexerMainProcess(data* ch_data)
{
	if(!createNumfPid(ch_data->dir)) exit(EXIT_FAILURE);
	if(createNumfIndex(ch_data->dir))
		createIndexingThread(ch_data);
	waitForSignals(ch_data);
}


void initializeQueryDirectories(query_data *q_data,char* dir)
{
	q_data->d_count++;
	char **tmp = realloc(q_data->dirs, sizeof(char*)*q_data->d_count);
	if(tmp)
		q_data->dirs = tmp;
	else ERR("REALLOC");
	q_data->dirs[q_data->d_count-1] = dir;
}



void createIndexerProcess(data* ch_data,query_data *q_data, int argc, char** argv) 
{
	pid_t pid;
	for(;optind<argc;optind++)
	{
		initializeQueryDirectories(q_data,argv[optind]);
    	if((pid = fork())<0) ERR("FORK");
    	if(!pid)
    	{
    		ch_data->dir = argv[optind];
    		printf("Created indexer process for directory:  %s\n",ch_data->dir);
    		indexerMainProcess(ch_data);
    		exit(EXIT_SUCCESS);
    	}
    }
}


sigset_t addMasks()
{
	sigset_t mask,oldmask;
	sigemptyset(&mask);
	sigaddset(&mask,SIGUSR1);
	sigaddset(&mask,SIGUSR2);
	sigaddset(&mask,SIGTERM);
	if(sigprocmask(SIG_BLOCK, &mask, &oldmask))ERR("SIG_BLOCK");
	return mask;
}


data* initializeIndexerData(int argc, char **argv)
{
	data* ch_data = (data*)malloc(sizeof(data));
	if(ch_data == NULL) ERR("MALLOC");
	ch_data->start = ch_data->end = 0;
	ch_data->mask = addMasks();
	ch_data->status = 0;
	ch_data->tid = 0;
	ch_data->m = DEFAULT_MIN;
    ch_data->M = DEFAULT_MAX;
    ch_data->i = DEFUALT_INTERVAL;
    ch_data->r = DEFAULT_RECURSION;
    time(&ch_data->big_bang);
    int opt;
    while((opt = getopt(argc,argv, "rm:M:i:")) != -1)
    {
    	switch(opt)
    	{
    		case 'r':
    			ch_data->r= 1;
    			break;
    		case 'm':
    			ch_data->m= atoi(optarg);
    			break;
    		case 'M':
    			ch_data->M = atoi(optarg);
    			break;
    		case 'i':
    			ch_data->i= atoi(optarg);
    			break;
    		default:
    			usage(argv[0]);
    			break;
    	}
    }
    return ch_data;
}

query_data* initializeQueryData()
{
	query_data *q_data=(query_data*)malloc(sizeof(query_data));
	if(q_data==NULL) ERR("MALLOC");
	q_data->q_count=0;
	q_data->queries = NULL;
	q_data->dirs = NULL;
	q_data->tid = 0;
	q_data->d_count = 0;
	return q_data;
}

void initializeQueryNumbers(query_data *q_data, char *token)
{
	token = strtok(NULL," ");
	while(token!=NULL)
	{
		q_data->q_count++;
		int *tmp = realloc(q_data->queries, sizeof(int)*q_data->q_count);
		if(tmp)
			q_data->queries = tmp;
		else ERR("REALLOC");
		q_data->queries[q_data->q_count-1] = atoi(token);
		token = strtok(NULL," ");
	}
}


void handleInputs(query_data *q_data)
{
	char input[DEFAULT_INPUT_SIZE];
	while(1)
	{

		if(fgets(input,DEFAULT_INPUT_SIZE,stdin)==NULL) ERR("FGETS");
		input[strlen(input)-1] = '\0';
		if(!strcmp(input,"exit"))
		{
			kill(0,SIGTERM);
			exit(EXIT_SUCCESS);
		}
		else if(!strcmp(input,"index"))
			kill(0,SIGUSR2);

		else if(!strcmp(input,"status"))
			kill(0,SIGUSR1);	
		else
		{
			char* token = strtok(input, " ");
			if(!token) printf("Wrong input\n");
			else
			{
				if(!strcmp(token,"query"))
				{
					initializeQueryNumbers(q_data,token);
					createQueryThread(q_data);
				}
				else printf("Wrong input\n");
			}
			
		}	
	}
}


int main(int argc,char** argv)
{
	data* ch_data = initializeIndexerData(argc,argv);
	query_data* q_data = initializeQueryData();
	createIndexerProcess(ch_data, q_data, argc, argv);
	handleInputs(q_data);
}