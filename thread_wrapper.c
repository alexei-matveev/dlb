#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>

void thread_function_(int *id);
void thread_control_();
void thread_mailbox_();

void th_inits_();
void th_create_mail_(int *name2);
void th_create_control_(int *name1);

void th_exit_();
void th_mutex_lock_(int *mutex);
void th_mutex_unlock_(int *mutex);
void th_cond_wait_(int *condition, int *mutex);
void th_cond_signal_(int *condition);

#define NMUTEXES 4
#define NCONDS 3

pthread_t threads[4];
pthread_mutex_t mutexes[NMUTEXES];
pthread_cond_t conds[NCONDS];

// why are these vars made global?
pthread_attr_t ThreadAttribute;
pthread_mutexattr_t attr;
pthread_condattr_t cattr;

void th_inits_()
{
  int rc;
  rc = pthread_attr_init(&ThreadAttribute);
  if (rc) {
    printf("ERROR: return code from pthread_attr_init() is %d\n", rc);
    exit(-1);
  }
  rc = pthread_attr_setdetachstate(&ThreadAttribute, PTHREAD_CREATE_DETACHED);
  if (rc) {
    printf("Error: pthread_attr_setdetachstate failed with %d\n", rc);
    exit(-1);
  }

  rc = pthread_attr_setscope(&ThreadAttribute, PTHREAD_SCOPE_SYSTEM);
  //rc = pthread_attr_setscope(&ThreadAttribute, PTHREAD_SCOPE_PROCESS);
  if (rc) {
    printf("Error:  pthread_attr_setscope failed with %d\n", rc);
    exit(-1);
  }
  rc = pthread_mutexattr_init(&attr);
  if (rc) {
    printf("Error:  pthread_mutexattr_init failed with %d\n", rc);
    exit(-1);
  }
  rc = pthread_condattr_init(&cattr);
  if (rc) {
    printf("Error:  pthread_condattr_init failed with %d\n", rc);
    exit(-1);
  }
  rc = pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_PRIVATE);
  if (rc) {
    printf("Error:  pthread_mutexattr_setpshared failed with %d\n", rc);
    exit(-1);
  }
  rc = pthread_condattr_setpshared(&cattr, PTHREAD_PROCESS_PRIVATE);//PTHREAD_PROCESS_SHARED
  if (rc) {
    printf("Error:  pthread_condattr_setpshared failed with %d\n", rc);
    exit(-1);
  }
  for (int i = 1; i <= NMUTEXES; i++)
  {
    rc = pthread_mutex_init(&mutexes[i], &attr);
    if (rc) {
      printf("Error: pthread_mutex_init failed with %d\n", rc);
      exit(-1);
    }
  }
  for (int i = 1; i <= NCONDS; i++)
  {
    rc = pthread_cond_init(&conds[i], &cattr);
    if (rc) {
      printf("Error: pthread_cond_init failed with %d\n", rc);
      exit(-1);
    }
  }
}

void th_create_control_(int * name1)
{
  int rc;
  rc = pthread_create(&threads[*name1], &ThreadAttribute,(void *(*)(void *)) thread_control_, NULL);
  if (rc) {
    printf("ERROR; return code from pthread_create() is %d\n", rc);
    //exit(-1);
  }
}

void th_create_mail_( int * name2)
{
  int rc;
  rc = pthread_create(&threads[*name2], &ThreadAttribute,(void *(*)(void *)) thread_mailbox_, NULL);
  if (rc) {
    printf("ERROR; return code from pthread_create() is %d\n", rc);
    //exit(-1);
  }
}

void th_exit_()
{
  pthread_exit(NULL);
}

void th_mutex_lock_(int *mutex)
{
  pthread_mutex_lock(&mutexes[*mutex]);
}

void th_mutex_unlock_(int * mutex)
{
  pthread_mutex_unlock(&mutexes[*mutex]);
}

void th_cond_wait_( int *condition, int *mutex)
{
  //printf("COND %d WAITS %d\n", *condition, *mutex);
  pthread_cond_wait(&conds[*condition], &mutexes[*mutex]);
}

void th_cond_signal_(int * condition)
{
  //printf("COND %d RELEASED\n", *condition);
  pthread_cond_signal (&conds[*condition]);
}
