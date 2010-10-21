#       include         <stdio.h>
#       include         <stdlib.h>
#       include         <pthread.h>
#       include         "thread_wrapper.h"
#       include         <errno.h>

//typedef pthread_t th_name_;
//typedef pthread_mutex_t th_mutex_;
//typedef pthread_cond_t th_condition_;

//th_name_ threads[4];
//th_mutex_ mutexes[4];
//th_condition_ conds[3];

//extern "C" void thread_function_(int * id);
//extern "C" void thread_control_();
//extern "C" void thread_mailbox_();
pthread_attr_t ThreadAttribute;
pthread_mutexattr_t attr;
pthread_condattr_t cattr;

extern "C" void *th_inits_()
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
  for (int i =1;i<=4;i++)
  {
   rc = pthread_mutex_init(&mutexes[i], &attr);
  if (rc) {
      printf("Error: pthread_mutex_init failed with %d\n", rc);
      exit(-1);
      }
  }
  for (int i =1;i<=3;i++)
  {
   rc = pthread_cond_init(&conds[i], &cattr);
  if (rc) {
      printf("Error: pthread_cond_init failed with %d\n", rc);
      exit(-1);
      }
  }
}

extern "C" void *th_create_control_(int * name1)
  {
int rc;
  rc = pthread_create(&threads[*name1], &ThreadAttribute,(void *(*)(void *)) thread_control_, NULL);
  if (rc) {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      //exit(-1);
      }
  }

extern "C" void *th_create_mail_( int * name2)
  {
int rc;
  rc = pthread_create(&threads[*name2], &ThreadAttribute,(void *(*)(void *)) thread_mailbox_, NULL);
  if (rc) {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      //exit(-1);
      }
  }

extern "C" void *th_create2_(void *function)
  {
  int rc;
  rc = pthread_create(&threads[1], NULL,(void *(*)(void *)) function,NULL);
  if (rc) {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      //exit(-1);
      }
  }

extern "C" void * th_exit_()
{
pthread_exit(NULL);
}

extern "C" void * th_mutex_lock_(int *mutex)
 {
 pthread_mutex_lock(&mutexes[*mutex]);
}

extern "C" void * th_mutex_unlock_(int * mutex)
 {
 pthread_mutex_unlock(&mutexes[*mutex]);
}

extern "C" void * th_cond_wait_( int *condition, int *mutex)
 {
 //printf("COND %d WAITS %d\n", *condition, *mutex);
 pthread_cond_wait(&conds[*condition], &mutexes[*mutex]);
 }

extern "C" void * th_cond_signal_(int * condition)
 {
 //printf("COND %d RELEASED\n", *condition);
 pthread_cond_signal (&conds[*condition]);
}
