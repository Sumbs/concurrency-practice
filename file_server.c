#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#define N 5          // max number of files that can be used at a time
#define M 5          // max number of threads that can run at a time
#define MAXLEN 120   // max length of any file server command

/* Struct: Job
 * -----------
 *  used by worker thread to keep track of writers and readers.
 *  also used to decide whether job is allowed to terminate.
 */
typedef struct TJob {
  int workers;
  int count;
  int curr_job;
  pthread_cond_t done;         // condition variable for finished jobs
  pthread_mutex_t wlock;       // lock for updating worker count
  pthread_mutex_t global_lock; // lock for reading/writing
} tjob;

/* Struct: Jobdata
 * ---------------
 *  holds data that is passed to thread to be used in a job.
 */
typedef struct Jobdata {
  int cmdidx;
  int fileidx;
  int job_ID;
  int worker_ID;
  char string[50];
} jobdata;

/*
 * Global Variables
 */
char files[N][MAXLEN];          // keeps track of currently active jobs
tjob *jobs[N] = {0};            // contains data of jobs
pthread_t tid[M];               // worker threads
int threads[M] = {0};           // keeps track of currently working threads
FILE *cmdfile;                  // log file for commands
FILE *readfile;                 // log file for reads
FILE *emptyfile;                // log file for empty
pthread_mutex_t tlock[N];       // lock for accessing jobs
pthread_mutex_t qlock;          // lock for accessing working threads
pthread_mutex_t rlock;          // lock for read.txt
pthread_mutex_t elock;          // lock for empty.txt

char cmds[][MAXLEN] = { "read", "write", "empty", };
char space[2] = " ";

/* Function: pfiles
 * ----------------
 *  test function to print all currently active jobs.
 */
void pfiles() {
  for ( int i = 0; i < N; i++ ) {
    printf( "files[%d]: %s\n", i, files[i] );
  }
}

/* Function: pft
 * -------------
 *  test function to print all currently active workers.
 */
void pft() {
  printf( "active threads: " );
  for( int i = 0; i < M; i++ ) {
    if( threads[i] == 1 ) {
      printf( "%d, ", i );
    }
  }
  printf( "\n" );
}

/* Function: pwork
 * -------------
 *  test function to print an individual worker's data
 *
 *  data: data that is used by the worker
 */
void pwork( jobdata *data ) {
  printf( "thread working on fileidx = %d with:\n"
      "\tcommand = %s\n"
      "\tjob_ID = %d\n"
      "\tworker_ID = %d\n"
      "\tstring = %s\n"
      ,
      data->fileidx,
      cmds[data->cmdidx],
      data->job_ID,
      data->worker_ID,
      data->string );
}

/* Function: pjob
 * --------------
 *  test function to print the details of a specific job.
 *
 * idx: index of job in jobs array
 */
void pjob( int idx ) {
  tjob *job = jobs[idx];
  printf( "file <<%s>> currently has:\n"
      "\tworkers = %d\n"
      "\tcount = %d\n"
      "\tcurr_job = %d\n"
      ,
      files[idx],
      job->workers,
      job->count,
      job->curr_job );
}

/* Function: init
 * --------------
 *  initializes the following:
 *   - tlock: mutexes for the job array
 *   - qlock: mutex for the thread pool
 *   - rlock: mutex for read.txt
 *   - elock: mutex for empty.txt
 *   - files: each entry is set to whitespace
 *   - log files command.txt, read.txt, empty.txt
 */
void init() {
  for( int i = 0; i < N; i++ ) {
    if( pthread_mutex_init( &tlock[i], NULL ) != 0 ) {
      printf( "mutex init has failed\n" );
    }
  }
 
  if( pthread_mutex_init( &qlock, NULL ) != 0  ||
      pthread_mutex_init( &rlock, NULL ) != 0  ||
      pthread_mutex_init( &elock, NULL ) != 0 ) {
    printf( "mutex init has failed\n" );
  }

  for( int i = 0; i < N; i++ ) {
    strcpy( files[i], " " );
  }

  cmdfile = fopen( "commands.txt", "w" );
  readfile = fopen( "read.txt", "w" );
  emptyfile = fopen( "empty.txt", "w" );

  printf( "Initialization complete\n" );
}

/* Function: getcmd
 * ----------------
 *  used to obtain input from the user.
 *
 * buf: buffer where command is stored
 * nbuf: size of buffer
 *
 * returns: 0 when command is successfully accepted
 */
int getcmd( char *buf, int nbuf ) {
  printf( "> " );
  memset( buf, 0, nbuf );
  if ( fgets( buf, nbuf, stdin ) != NULL ) {
    return 0;
  }
  return 1;
}

/* Function: strindex
 * -----------------
 *  locates where a string is stored in an array.
 *
 * str: string to find
 * arr: array to search
 * narr: size of array
 *
 * returns: index of string in array when found, -1 otherwise
 */
int strindex( char *str, char arr[][MAXLEN], int narr ) {
  for( int i = 0; i < narr; i++ ) {
    if ( strcmp( str, arr[i] ) == 0 ) {
      return i;
    }
  }
  return -1;
}

/* Function: register_job
 * ----------------------
 *  saves the filepath of a file that will be read from or written to on the
 *  files array by looking for the first "open" spot.
 *
 * filepath: path of file to be saved
 *
 * returns: index of file in array if save is successful, -1 otherwise
 */
int register_job( char *filepath ) {
  for( int i = 0; i < N; i++ ) {
    if( strcmp( files[i], " " ) == 0 ) {
      strcpy( files[i], filepath );
      return i;
    } 
  }
  return -1;
}

/* Function: init_job
 * ------------------
 *  checks whether an existing job exists in the job array at the supplied
 *  index. if yes, then the job count is increased. otherwise, the job count is
 *  set to one and the tjob struct is initialized.
 *
 * idx: index of filepath in the files array that job will use
 * filepath: filepath that is redundantly set to the files array to avoid
 *           conflicts in case of consecutive job termination/initiation.
 *
 * returns: job count which functions as ID for corresponding thread
 */
int init_job( int idx, char *filepath ) {
  pthread_mutex_lock( &tlock[idx] );

  strcpy( files[idx], filepath );
  tjob* job = jobs[idx];

  // update job
  if( job != NULL ) {
    job->count++;
  }
  // create job
  else {
    job = malloc( sizeof( tjob ) );

    if ( pthread_mutex_init( &( job->wlock ), NULL ) != 0 ||
         pthread_mutex_init( &( job->global_lock ), NULL ) != 0 ) {
      printf( "mutex init has failed\n" );
    }

    if (pthread_cond_init( &( job->done ), NULL ) != 0) {
      printf( "cond init has failed" );
    }

    job->workers = 0;
    job->count = 1;
    job->curr_job = 1;
    jobs[idx] = job;
  }

  pthread_mutex_unlock( &tlock[idx] );
  return job->count;
}

/*
 * Function: register_worker
 * -------------------------
 *  saves the index of a worker thread that is dispatched.
 *
 * returns: index of thread in array if save is successful, -1 otherwise
 */
int register_worker() {
  pthread_mutex_lock( &qlock );
  for( int i = 0; i < M; i++ ) {
    if( threads[i] == 0 ) {
      threads[i] = 1;
      pthread_mutex_unlock( &qlock );
      return i;
    } 
  }
  pthread_mutex_unlock( &qlock );
  return -1;
}

/*
 * Function: init_worker
 * ---------------------
 *  prepares the data to be used by the worker for an operation.
 *
 * cmdidx: index of operation to be performed in cmds array
 * fileidx: index of file to be read in files array
 * job_ID: position of worker in queue
 * worker_ID: position of worker in thread pool
 * string: data to be written, if any
 *
 * returns: job data for worker
 */
jobdata *init_worker( int cmdidx, int fileidx, int job_ID, int worker_ID, char *string ) {
  jobdata *data = malloc( sizeof(jobdata) );
  data->cmdidx = cmdidx;
  data->fileidx = fileidx;
  data->job_ID = job_ID;
  data->worker_ID = worker_ID;
  strcpy( data->string, string );
  return data;
}

/* Function: finish_job
 * --------------------
 *  called by the worker after an operation. if there are other jobs in the
 *  queue, the worker broadcasts that that the job is now available. otherwise,
 *  memory for the job is deallocated. in any case, memory for the worker data
 *  is deallocated and the worker waits again in the thread pool.
 *
 * data: data that is used by the worker
 */
void finish_job( jobdata *data ) {
  int i = data->fileidx;

  // finish job
  pthread_mutex_lock( &tlock[i] );
  tjob *job = jobs[i];
  if ( job->workers == 0 ) {
    free( job );
    jobs[i] = NULL;
    strcpy( files[i], " " );
  }
  else {
    job->curr_job++;
    pthread_cond_broadcast( &(job->done) );
  }
  pthread_mutex_unlock( &tlock[i] );

  // retire worker
  pthread_mutex_lock( &qlock );
  threads[data->worker_ID] = 0;
  free( data );
  pthread_mutex_unlock( &qlock );
}

/* Function: enqueue_worker
 * ------------------------
 *  adds worker to job's total worker count
 *
 *  job: job which worker is part of
 */
void enqueue_worker( tjob *job ) {
  pthread_mutex_lock( &( job->wlock ) );
  job->workers++;
  pthread_mutex_unlock( &( job->wlock ) );
}

/* Function: dequeue_worker
 * ------------------------
 *  removes worker from job's total worker count
 *
 * job: job which worker is part of
 */
void dequeue_worker( tjob *job ) {
  pthread_mutex_lock( &( job->wlock ) );
  job->workers--;
  pthread_mutex_unlock( &( job->wlock ) );
}

/* Function: xsleep
 * ----------------
 *  puts thread to sleep for 1 second (80% chance) or 6 seconds (20% chance).
 */
void xsleep() {
  return;
  srand(time(0));
	int r = rand()%100;
  ( r < 80 ) ? sleep(6) : sleep(6);
}

/* Function: xread
 * ---------------
 *  reads a file (if it exists) and saves its content to read.txt
 *
 *  data: data that is used by the worker
 */
void xread( jobdata *data ) {
  FILE *file;
  tjob *job = jobs[data->fileidx];

  file = fopen( files[data->fileidx], "r" );
  if ( file == NULL ) {
    printf( "read %s: FILE DNE\n", files[data->fileidx] );
  }
  else {
    char ch;
    printf( "read %s: ", files[data->fileidx] );
    while (( ch = fgetc(file) ) != EOF ) {
      printf( "%c", ch );
    }
      printf( "\n" );
  }
}

void xwrite( jobdata *data ) {
  FILE *file;
  tjob *job = jobs[data->fileidx];
  char *string = data->string;

  //file = fopen( files[data->fileidx], "a" );
  usleep( ( unsigned int )( 1000 * 25 * strlen( string )));
  printf( "strlen: %ld\n", strlen( string ));
  printf( "slept for %d milliseconds\n", ( int ) (strlen( string ) * 25 ));
  /*
  char ch;
  printf( "read %s: ", files[data->fileidx] );
  while (( ch = fgetc(file) ) != EOF ) {
    printf( "%c", ch );
  }
  printf( "\n" );
  */
}

void xempty( jobdata *data ) {
  printf( "emptying\n" );
}

/* Function: dispatch_worker
 * -------------------------
 *  adds worker to job queue, puts it to sleep, then makes it wait for its turn
 *  to work on the job. after the worker completes its assigned operation,it is
 *  removed from job queue and cleanup is done.
 *
 * arg: data for worker cast to ( void * )
 */
void *dispatch_worker( void *arg ) {
  jobdata *data = (jobdata *) arg;
  tjob *job = jobs[data->fileidx];

  // begin operation
  enqueue_worker( job );
  xsleep();

  pthread_mutex_lock( &( job->global_lock ) );
  
  while ( job->curr_job != data->job_ID ) {
    pthread_cond_wait( &( job->done ), &( job->global_lock ) );
  }

  // execute command
  switch( data->cmdidx ) {
    case 0:
      xread( data );
      break;
    case 1:
      xwrite( data );
      break;
    case 2:
      xempty( data );
      break;
  }

  pthread_mutex_unlock( &( job->global_lock ) );

  // end operation
  dequeue_worker( job );
  finish_job( data );
}

int main() {
  char buf[MAXLEN]; // buffer where user input is stored
  char command[10], filepath[51], string[51];
  char *token;
  int cmdidx;
  int fileidx;
  int job_ID;
  int worker_ID;

  int testing_mode = 1;

  init();

  while( getcmd( buf, sizeof( buf ) ) == 0 ) {

    buf[strlen( buf ) - 1] = ' ';  // turn '\n' to space
    strcpy( string, " " );         // reset string

    strcpy( command, strtok_r( buf, space, &token ));

    // check if user wants to quit
    if ( strcmp( command, "quit" ) == 0 ) {
      break;
    }

    // validating command
    if(( cmdidx = strindex( command, cmds, 3 )) == -1 ) {
      printf( "Invalid command\n" );
      continue;
    }

    strcpy( filepath, strtok_r( NULL, space, &token ));

    // check job existence
    if(( fileidx = strindex( filepath, files, N )) == -1 ) {
      if(( fileidx = register_job( filepath )) == -1 ) {
        printf( "Too many jobs at once.\n" );
        return( -1 );
      }
    }

    // save string if operation is write
    if( cmdidx == 1 ) {
      strcpy( string, strtok_r( NULL, space, &token ));
    }
    
    // find an available thread in thread pool
    if(( worker_ID = register_worker()) == -1 ) {
      printf( "No more available threads.\n" );
      return( -1 );
    }

    // ready jobs and worker data
    job_ID = init_job( fileidx, filepath );
    jobdata *data = init_worker( cmdidx, fileidx, job_ID, worker_ID, string );

    // print job and worker status for testing
    if ( testing_mode ) {
      pft();
      pjob( fileidx );
      pwork( data );
    }

    // dispatch worker
    pthread_create( &tid[worker_ID], NULL, dispatch_worker, ( void * )data );
  }
}
