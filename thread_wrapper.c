/*
  Copyright (c) 2010-2014 Astrid Nikodem, Alexei Matveev
*/
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>

void thread_control(); // extern, Fortran sub
void thread_mailbox(); // extern, Fortran sub
void thread_secretary(); // extern, Fortran sub

void th_inits();
void th_create_all();
void th_create_one();
void th_exit();
void th_join_all();

void th_mutex_lock(int *mutex);
void th_mutex_unlock(int *mutex);
void th_cond_wait(int *condition, int *mutex);
void th_cond_signal(int *condition);
void th_rwlock_rdlock(int *rwlock);
void th_rwlock_wrlock(int *rwlock);
void th_rwlock_unlock(int *rwlock);
void c_sleep( int * time);


#define NTHREADS 2
#define NMUTEXES 2
#define NCONDS 3
#define NRWLOCKS 1

pthread_t threads[NTHREADS];
pthread_mutex_t mutexes[NMUTEXES];
pthread_cond_t conds[NCONDS];
int cond_active[NCONDS];
int active_threads[NTHREADS];
pthread_rwlock_t rwlocks[NRWLOCKS];

void th_inits()
{
  int rc;
  int i;
  pthread_mutexattr_t mutex_attr;
  pthread_condattr_t cond_attr;
  pthread_rwlockattr_t rwlock_attr;

  // init mutexes:
  rc = pthread_mutexattr_init(&mutex_attr);
  assert(!rc);

  rc = pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_PRIVATE);
  assert(!rc);

  for ( i = 0; i < NMUTEXES; i++) {
    rc = pthread_mutex_init(&mutexes[i], &mutex_attr);
    assert(!rc);
  }

  rc = pthread_mutexattr_destroy(&mutex_attr);
  assert(!rc);

  // init condition variables:
  rc = pthread_condattr_init(&cond_attr);
  assert(!rc);

  rc = pthread_condattr_setpshared(&cond_attr, PTHREAD_PROCESS_PRIVATE);//PTHREAD_PROCESS_SHARED
  assert(!rc);


  for ( i = 0; i < NTHREADS; i++) {
    active_threads[i] = 0;
    }

  for ( i = 0; i < NCONDS; i++) {
    rc = pthread_cond_init(&conds[i], &cond_attr);
    assert(!rc);
    cond_active[i] = 0; // meaning false
  }

  rc = pthread_condattr_destroy(&cond_attr);
  assert(!rc);

  // init rwlocks:
  rc = pthread_rwlockattr_init(&rwlock_attr);
  assert(!rc);

  for ( i = 0; i < NRWLOCKS; i++) {
    rc = pthread_rwlock_init(&rwlocks[i], &rwlock_attr);
    assert(!rc);
  }

  rc = pthread_rwlockattr_destroy(&rwlock_attr);
  assert(!rc);

}

void th_create_all()
{
  int rc;
  pthread_attr_t ThreadAttribute;

  rc = pthread_attr_init(&ThreadAttribute);
  assert(!rc);

  rc = pthread_attr_setdetachstate(&ThreadAttribute, PTHREAD_CREATE_JOINABLE);
  assert(!rc);

  rc = pthread_attr_setscope(&ThreadAttribute, PTHREAD_SCOPE_SYSTEM);
  //rc = pthread_attr_setscope(&ThreadAttribute, PTHREAD_SCOPE_PROCESS);
  assert(!rc);

  rc = pthread_create(&threads[0], &ThreadAttribute, (void *(*)(void *)) thread_mailbox, NULL);
  assert(!rc);
  active_threads[0] = 1;

  rc = pthread_create(&threads[1], &ThreadAttribute, (void *(*)(void *)) thread_control, NULL);
  assert(!rc);
  active_threads[1] = 1;

  rc = pthread_attr_destroy(&ThreadAttribute);
  assert(!rc);
}

void th_create_one()
{
  int rc;
  pthread_attr_t ThreadAttribute;

  rc = pthread_attr_init(&ThreadAttribute);
  assert(!rc);

  rc = pthread_attr_setdetachstate(&ThreadAttribute, PTHREAD_CREATE_JOINABLE);
  assert(!rc);

  rc = pthread_attr_setscope(&ThreadAttribute, PTHREAD_SCOPE_SYSTEM);
  //rc = pthread_attr_setscope(&ThreadAttribute, PTHREAD_SCOPE_PROCESS);
  assert(!rc);

  rc = pthread_create(&threads[0], &ThreadAttribute, (void *(*)(void *)) thread_secretary, NULL);
  assert(!rc);
  active_threads[0] = 1;

  rc = pthread_attr_destroy(&ThreadAttribute);
  assert(!rc);
}

void th_exit()
{
  pthread_exit(NULL);
}

void th_join_all()
{
  int rc;
  int i;
  void *status;


  for ( i = 0; i < NTHREADS; i++) {
    if (active_threads[i]) {
      rc = pthread_join(threads[i], &status);
      assert(!rc);
      active_threads[i] = 0;
      }
    }
}

void th_mutex_lock(int *mutex)
{
  int rc;
  assert(*mutex >= 0 && *mutex < NMUTEXES);
  rc = pthread_mutex_lock(&mutexes[*mutex]);
  assert(!rc);
}

void th_mutex_unlock(int *mutex)
{
  int rc;
  assert(*mutex >= 0 && *mutex < NMUTEXES);
  rc = pthread_mutex_unlock(&mutexes[*mutex]);
  assert(!rc);
}

void th_cond_wait( int *condition, int *mutex)
{
  int rc;
  assert(*condition >= 0 && *condition < NCONDS);
  assert(*mutex >= 0 && *mutex < NMUTEXES);
  //printf("COND %d WAITS %d\n", *condition, *mutex);
  cond_active[*condition] = 1; // = true
  rc = pthread_cond_wait(&conds[*condition], &mutexes[*mutex]);
  assert(!rc);
  cond_active[*condition] = 0; // = false
}

void th_cond_signal(int *condition)
{
  int rc;
  assert(*condition >= 0 && *condition < NCONDS);
  //printf("COND %d RELEASED\n", *condition);
  if (cond_active[*condition]) {
    rc = pthread_cond_signal (&conds[*condition]);
    assert(!rc);
  }
}

void th_rwlock_rdlock(int *rwlock)
{
  int rc;
  assert(*rwlock >= 0 && *rwlock < NRWLOCKS);
  rc = pthread_rwlock_rdlock(&rwlocks[*rwlock]);
  assert(!rc);
}

void th_rwlock_wrlock(int *rwlock)
{
  int rc;
  assert(*rwlock >= 0 && *rwlock < NRWLOCKS);
  rc = pthread_rwlock_wrlock(&rwlocks[*rwlock]);
  assert(!rc);
}

void th_rwlock_unlock(int *rwlock)
{
  int rc;
  assert(*rwlock >= 0 && *rwlock < NRWLOCKS);
  rc = pthread_rwlock_unlock(&rwlocks[*rwlock]);
  assert(!rc);
}
void c_sleep( int *time)
{
usleep(*time);
}
