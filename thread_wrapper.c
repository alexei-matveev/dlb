#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <assert.h>

void thread_control(); // extern, Fortran sub
void thread_mailbox(); // extern, Fortran sub

void th_inits();

// Better make a single func th_create() that starts both
// threads unconditionally:
void th_create_mail(int *tid);
void th_create_control(int *tid);

// Then this should join both of them, does not need
// a thread ID:
void th_join_(int *tid);

void th_exit();
void th_mutex_lock(int *mutex);
void th_mutex_unlock(int *mutex);
void th_cond_wait(int *condition, int *mutex);
void th_cond_signal(int *condition);
void th_rwlock_rdlock(int *rwlock);
void th_rwlock_wrlock(int *rwlock);
void th_rwlock_unlock(int *rwlock);

#define NTHREADS 2
#define NMUTEXES 4
#define NCONDS 3
#define NRWLOCKS 1

pthread_t threads[NTHREADS];
pthread_mutex_t mutexes[NMUTEXES];
pthread_cond_t conds[NCONDS];
pthread_rwlock_t rwlocks[NRWLOCKS];

// why are these vars made global?
pthread_attr_t ThreadAttribute;

void th_inits()
{
  int rc;
  pthread_mutexattr_t mutex_attr;
  pthread_condattr_t cond_attr;
  pthread_rwlockattr_t rwlock_attr;

  rc = pthread_attr_init(&ThreadAttribute);
  assert(!rc);

  rc = pthread_attr_setdetachstate(&ThreadAttribute, PTHREAD_CREATE_JOINABLE);
  assert(!rc);

  rc = pthread_attr_setscope(&ThreadAttribute, PTHREAD_SCOPE_SYSTEM);
  //rc = pthread_attr_setscope(&ThreadAttribute, PTHREAD_SCOPE_PROCESS);
  assert(!rc);

  // init mutexes:
  rc = pthread_mutexattr_init(&mutex_attr);
  assert(!rc);

  rc = pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_PRIVATE);
  assert(!rc);

  for (int i = 0; i < NMUTEXES; i++) {
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

  for (int i = 0; i < NCONDS; i++) {
    rc = pthread_cond_init(&conds[i], &cond_attr);
    assert(!rc);
  }

  rc = pthread_condattr_destroy(&cond_attr);
  assert(!rc);

  // init rwlocks:
  rc = pthread_rwlockattr_init(&rwlock_attr);
  assert(!rc);

  for (int i = 0; i < NRWLOCKS; i++) {
    rc = pthread_rwlock_init(&rwlocks[i], &rwlock_attr);
    assert(!rc);
  }

  rc = pthread_rwlockattr_destroy(&rwlock_attr);
  assert(!rc);
}

void th_create_control(int *tid)
{
  int rc;

  assert(*tid >= 0 && *tid < NTHREADS);
  rc = pthread_create(&threads[*tid], &ThreadAttribute,(void *(*)(void *)) thread_control, NULL);
  assert(!rc);
}

void th_create_mail(int *tid)
{
  int rc;

  assert(*tid >= 0 && *tid < NTHREADS);
  rc = pthread_create(&threads[*tid], &ThreadAttribute,(void *(*)(void *)) thread_mailbox, NULL);
  assert(!rc);
}

void th_exit()
{
  pthread_exit(NULL);
}

void th_join_(int *tid)
{
  int rc;
  void *status;

  assert(*tid >= 0 && *tid < NTHREADS);
  rc = pthread_join(threads[*tid], &status);
  assert(!rc);
}

void th_mutex_lock(int *mutex)
{
  assert(*mutex >= 0 && *mutex < NMUTEXES);
  pthread_mutex_lock(&mutexes[*mutex]);
}

void th_mutex_unlock(int *mutex)
{
  assert(*mutex >= 0 && *mutex < NMUTEXES);
  pthread_mutex_unlock(&mutexes[*mutex]);
}

void th_cond_wait( int *condition, int *mutex)
{
  assert(*condition >= 0 && *condition < NCONDS);
  assert(*mutex >= 0 && *mutex < NMUTEXES);
  //printf("COND %d WAITS %d\n", *condition, *mutex);
  pthread_cond_wait(&conds[*condition], &mutexes[*mutex]);
}

void th_cond_signal(int *condition)
{
  assert(*condition >= 0 && *condition < NCONDS);
  //printf("COND %d RELEASED\n", *condition);
  pthread_cond_signal (&conds[*condition]);
}

void th_rwlock_rdlock(int *rwlock) {
  assert(*rwlock >= 0 && *rwlock < NRWLOCKS);
  pthread_rwlock_rdlock(&rwlocks[*rwlock]);
}

void th_rwlock_wrlock(int *rwlock) {
  assert(*rwlock >= 0 && *rwlock < NRWLOCKS);
  pthread_rwlock_wrlock(&rwlocks[*rwlock]);
}

void th_rwlock_unlock(int *rwlock) {
  assert(*rwlock >= 0 && *rwlock < NRWLOCKS);
  pthread_rwlock_unlock(&rwlocks[*rwlock]);
}

