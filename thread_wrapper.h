#ifndef pthread_wrapp
#define pthread_wrapp

typedef pthread_t th_name_;
typedef pthread_mutex_t th_mutex_;
typedef pthread_cond_t th_condition_;

th_name_ threads[4];
th_mutex_ mutexes[4];
th_condition_ conds[3];

extern "C" void thread_function_(int * id);
extern "C" void thread_control_();
extern "C" void thread_mailbox_();

extern "C" void *th_inits_();
extern "C" void *th_create_mail_(int * name2);
extern "C" void *th_create_control_(int * name1);

extern "C" void *th_create_(int * name);
extern "C" void * th_exit_();
extern "C" void * th_mutex_lock_(int *mutex);
extern "C" void * th_mutex_unlock_(int * mutex);
extern "C" void * th_cond_wait_( int *condition, int *mutex);
extern "C" void * th_cond_signal_(int * condition);
#endif
