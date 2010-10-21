!===============================================================
! Public interface of module
!===============================================================
module dlb
  !---------------------------------------------------------------
  !
  !  Purpose: takes care about dynamical load balancing,
  !
  !           INTERFACE to others:
  !           call dlb_init() - once before first acces
  !           call dlb_setup(init_job) - once every time a dlb should
  !                               be used, init_job should be the part
  !                               of the current proc of an initial job
  !                               distribution
  !           dlb_give_more(n, jobs) :
  !              n should be the number of jobs requested at once, the next
  !              time dlb_give_more is called again, all jobs from jobs should
  !              be finished, jobs are at most n, they are jsut the number of the
  !              jobs, which still have to be transformed between each other,
  !              it should be done the error slice from jobs(0) +1 to jobs(1)
  !              if dlb_give_more returns jobs(0)==jobs(1) there are on no proc
  !              any jobs left
  !
  !          the algorithm: similar to dlb_module algorithm but with explicit mpi messages
  !                         they are sended and received by two seperate threads, thus the main
  !                         thread can work without noticing them on the jobs
  !                         Thread 2 (CONTROL) will be woke up if job_storage is empty
  !                         it sends a message to another proc, asking for more work, does so till it
  !                         got something or everything is terminated
  !                         Thread 3 (MAILBOX) checks all the time for messages, to which is responses
  !                         mostly by himself, but he informs (wakes up) CONTROL if he got more work
  !                         so that CONTROL takes care if another request has to be put and to deal
  !                         with the main thread
  !
  !          termination algorithm: called (at least once) "Fixed Energy Distributed Termination
  !                                 Algorithm"
  !                        to avoid confusion, here the term "energy" is not used, talking about
  !                         respoinsibility (resp) instead, every system starts with a part of responsibility
  !                         given to him, if procs steal from him, they have later to send him a
  !                         message saying how many of his jobs, they've done, they always report to
  !                         the proc who had the resp first, thus source is given away with job
  !                         each proc lowers his resp about the values given back from any proc and
  !                         about the jobs he has done himself, when finished them, if he has his resp
  !                         at zero, he sends a message to termination_master
  !                         if termination master has a message from all the procs, that their resp is 0
  !                         he sends a message to all procs, telling them to terminated the algorithm
  !
  !          THIS MAY BE IMPLEMENTED LATER, POSSIBLY IN A SEPARATE FILE:
  !          another possibility for the algorithm: with setting master_server = true another the algorithm is
  !                                                 is sliglty changed. In this case only one (the
  !                                                 termination_master) is  allowed to be asked for new jobs.
  !                                                 It would not give back half of them, of course, but only a
  !                                                 fraction. Several different algorithms for it can be choosen
  !                                                 by chunk_m. They are similar to the algorithm provided by
  !                                                 OpenMP parallel-do (chunk_m = 1 gives a fixed amount of work, respectively
  !                                                 chunksize * m back, chunk_m = 2 does the guided variant with
  !                                                 giving back 1/n_procs of what is still there (as long as it is
  !                                                 more than m), m is the amount of jobs wanted at once
  !                                                 termination here is much easier: if termination_master can not
  !                                                 send any jobs back, it sends termination to the proc. It collects
  !                                                 all the procs he has send a termination to, if he has all and his
  !                                                 own CONTROL has send him a job request he terminates also.
  !
  !           thread are included via wrapper around c pthread routines, The wrappers all start with th and are
  !           located in the thread_wrapper.c file. There are routines for starting and ending threads:
  !            th_create_control, th_create_mail, th_exit, h_join; some for mutexe (blocking of global variables):
  !            th_mutex_lock, th_mutex_unlock; some for conditions (wake and sleep of threads):th_create_mail,
  !            th_cond_signal; and one for setting all the attributes to the threads, mutexes and conditions: th_inits
  !
  !  Module called by: ...
  !
  !
  !  References:
  !
  !
  !  Author: AN
  !  Date: 09/10
  !
  !
  !----------------------------------------------------------------
  !== Interrupt of public interface of module =====================
  !----------------------------------------------------------------
  ! Modifications
  !----------------------------------------------------------------
  !
  ! Modification (Please copy before editing)
  ! Author: ...
  ! Date:   ...
  ! Description: ...
  !
  !----------------------------------------------------------------
  use dlb_common, only: i4_kind, r8_kind, comm_world
  use dlb_common, only: assert_n, time_stamp, time_stamp_prefix ! for debug only
  use iso_c_binding
  implicit none
  include 'mpif.h'
  !use type_module ! type specification parameters
  save            ! save all variables defined in this module
  private         ! by default, all names are private
  !== Interrupt end of public interface of module =================

  !------------ public functions and subroutines ------------------
  public :: dlb_init, dlb_finalize, dlb_setup, dlb_give_more
  !
  ! public :: thread_control, thread_mailbox ! needed for the c-wrapper on the pthreads
  !
  ! These two are not used anywhere in fortran sources, but are formally a part
  ! of the public module interface as they are "bind(C)" and called from
  ! C-sources.
  !
  !================================================================
  ! End of public interface of module
  !================================================================


  interface
    !
    ! These interfaces need to be consistent with implementations
    ! in thread_wrapper.c
    !
    subroutine th_inits() bind(C)
    end subroutine th_inits

    subroutine th_exit() bind(C)
    end subroutine th_exit

    subroutine th_join_all() bind(C)
    end subroutine th_join_all

    subroutine th_create_all() bind(C)
    end subroutine th_create_all

    subroutine th_mutex_lock(lock) bind(C)
      use iso_c_binding
      implicit none
      integer(C_INT), intent(in) :: lock
    end subroutine th_mutex_lock

    subroutine th_mutex_unlock(lock) bind(C)
      use iso_c_binding
      implicit none
      integer(C_INT), intent(in) :: lock
    end subroutine th_mutex_unlock

    subroutine th_cond_signal(condition) bind(C)
      use iso_c_binding
      implicit none
      integer(C_INT), intent(in) :: condition
    end subroutine th_cond_signal

    subroutine th_cond_wait(condition, mutex) bind(C)
      use iso_c_binding
      implicit none
      integer(C_INT), intent(in) :: condition, mutex
    end subroutine th_cond_wait

    subroutine th_rwlock_rdlock(rwlock) bind(C)
      use iso_c_binding
      implicit none
      integer(C_INT), intent(in) :: rwlock
    end subroutine th_rwlock_rdlock

    subroutine th_rwlock_wrlock(rwlock) bind(C)
      use iso_c_binding
      implicit none
      integer(C_INT), intent(in) :: rwlock
    end subroutine th_rwlock_wrlock

    subroutine th_rwlock_unlock(rwlock) bind(C)
      use iso_c_binding
      implicit none
      integer(C_INT), intent(in) :: rwlock
    end subroutine th_rwlock_unlock
  end interface

  !------------ Declaration of types ------------------------------

  !------------ Declaration of constants and variables ----
  integer(kind=i4_kind), parameter  :: DONE_JOB = 1, NO_WORK_LEFT = 2, RESP_DONE = 3 !for distingishuing the messages
  integer(kind=i4_kind), parameter  :: WORK_REQUEST = 4, WORK_DONAT = 5 ! messages for work request
  integer(kind=i4_kind), parameter  :: SJOB_LEN = 3 ! Length of a single job in interface
  integer(kind=i4_kind), parameter  :: L_JOB = 2  ! Length of job to give back from interface
  integer(kind=i4_kind), parameter  :: NRANK = 3 ! Number in job, where rank of origin proc is stored
  integer(kind=i4_kind), parameter  :: J_STP = 1 ! Number in job, where stp (start point) is stored
  integer(kind=i4_kind), parameter  :: J_EP = 2 ! Number in job, where ep (end point) is stored
  integer(kind=i4_kind), parameter  :: JOBS_LEN = SJOB_LEN  ! Length of complete jobs storage

  ! IDs of mutexes, use base-0 indices:
  integer(kind=i4_kind), parameter :: LOCK_JS   = 0
  integer(kind=i4_kind), parameter :: LOCK_NJ   = 1

  ! IDs for condition variables, use base-0 indices:
  integer(kind=i4_kind), parameter :: COND_JS_UPDATE  = 0
  integer(kind=i4_kind), parameter :: COND_NJ_UPDATE  = 1
  integer(kind=i4_kind), parameter :: COND_JS2_UPDATE = 2

  ! thread IDs, use base-0 indices:
  integer(kind=i4_kind), parameter :: MAILBOX = 0
  integer(kind=i4_kind), parameter :: CONTROL = 1

  integer(kind=i4_kind), parameter  ::  MSGTAG = 166 ! message tag for all MPI communication

  logical, parameter :: masterserver = .false. ! changes to different variant (master slave concept for comparision)


  integer(kind=i4_kind)            :: my_rank, n_procs ! some synonyms, They will be initialized once and afterwards
                                        ! be read only

  integer(kind=i4_kind)             ::  termination_master ! the one who gathers the finished my_resp's
                                         ! and who tells all, when it is complety finished
                                         ! in case of the variant with master as server of jobs, its also the master

  integer(kind=i4_kind)             :: job_storage(jobs_len) ! store all the jobs, belonging to this processor
  integer(kind=i4_kind)             :: new_jobs(SJOB_LEN) ! stores new job, just arrived from other proc
  integer(kind=i4_kind)             :: start_job(SJOB_LEN) ! job_storage is changed a lot, backup for
                                     ! finding out, if someone has stolen something, or how many jobs one
                                     ! has done, after initalization only used by CONTROL
  integer(kind=i4_kind)             :: my_resp ! number of jobs, this processor is responsible for (given
                                              ! in setup
  logical                           :: terminated ! for termination algorithm
  logical, allocatable              :: all_done(:) ! only allocated on termination_master, stores which proc's
                                                   ! jobs are finished. If with masterserver: which proc has terminated
  logical                           :: i_am_waiting ! Thead 0 (main thread) is waiting for CONTROL

  ! some variables are shared between the three threads, there are also locks and conditions to obtain
  ! following a list, which of the three threads (MAIN, CONTROL, MAILBOX) does read or write on them
  !
  ! "BIG KERNEL LOCK", rwlock for global data (rdlock/wrlock/unlock):
  !
  ! * terminated: this is the flag, telling everybody, that it's done
  !             read: ALL
  !             write: CONTROL, MAILBOX
  ! * my_resp: read: CONTROL, MAILBOX
  !          write: CONTROL, MAILBOX
  !          for termination algorithm, CONTROL and MAILBOX may have knowledge about finished jobs
  !
  ! Those below are the mutexes that are used in combination with signals:
  !
  ! LOCK_JS:
  ! * job_storage: read: ALL
  !             write: ALL
  !             initialization (before start of the other threads) is without lock!
  !             holds job range of current unfinished jobs, asssigend to this processor
  ! * i_am_waiting: read: CONTROL
  !               write: CONTROL, MAIN
  !               MAIN tells CONTROL, that it is waiting for jobs, thus CONTROL
  !               should better search some and not wait in turn for MAIN to wake it
  !
  ! LOCK_NJ:
  ! * new_jobs:   read: CONTROL          locked by LOCK_NJ
  !             write: CONTROL, MAILBOX
  !             MAILBOX stores here messages, gotten for CONTROL, CONTROL "deletes" them after reading
  !
  ! termination_master, my_rank, n_procs and the paramater: they will be only read, after they have been
  !                initalized in a one thread context
  ! start_jobs: initialized in one thread context, after separation belonging to CONTROL only
  !
  ! the follwing conditions are used to tell:
  ! COND_JS_UPDATE: CONTROL waits here (if there have been enough jobs in the last entry)
  !                 for the other to took some jobs out of job_storage
  ! COND_JS2_UPDATE: if MAIN finds no more jobs in job_storage, it waits here if CONTROL gets
  !                any, CONTROL should not be waiting at that time but busily searching
  !                for more jobs
  !                MAIN tells by i_am_waiting that it has run out of jobs (and got no termination)
  ! COND_NJ_UPDATE: CONTROL waits for answer to a job request, got answer by MAILBOX
  !
  ! For debugging and counting trace
  ! used only on one thread
  integer(kind=i4_kind)             :: count_messages, count_requests, count_offers ! how many messages arrived, MAILBOX
  integer(kind=i4_kind)             :: my_resp_start, my_resp_self, your_resp !how many jobs doen where, CONTROL
  integer(kind=i4_kind)             :: many_tries, many_searches !how many times asked for jobs, CONTROL
  integer(kind=i4_kind)             :: many_zeros !how many times asked for jobs, CONTROL
  double precision  :: timemax
  !----------------------------------------------------------------
  !------------ Subroutines ---------------------------------------
contains

  subroutine dlb_init()
    !  Purpose: initalization of needed stuff
    !           is in one thread context
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: dlb_common_my_rank
    implicit none
    !** End of interface *****************************************
    integer(kind=i4_kind)       ::  alloc_stat, ierr
    !------------ Declaration of local variables -----------------
    call MPI_COMM_RANK( comm_world, my_rank, ierr )
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 1)
    call MPI_COMM_SIZE(comm_world, n_procs, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 2)

    ! for prefixing debug messages with proc rank set this var:
    dlb_common_my_rank = my_rank

    termination_master = n_procs - 1
    if (my_rank == termination_master) then
      allocate(all_done(n_procs), stat = alloc_stat)
      !ASSERT(alloc_stat==0)
      call assert_n(alloc_stat==0, 1)
    endif
    call th_inits()
  end subroutine dlb_init

  subroutine dlb_finalize()
    !  Purpose: cleaning up everything, after last call
    !
    ! Context: main thread after joining other two.
    !
    !------------ Modules used ------------------- ---------------
    implicit none
    integer(kind=i4_kind)    :: alloc_stat
    !** End of interface *****************************************

    if (allocated(all_done)) then
      deallocate(all_done, stat=alloc_stat)
      !ASSERT(alloc_stat==0)
      call assert_n(alloc_stat==0, 1)
    endif
  end subroutine dlb_finalize

  subroutine dlb_give_more(n, my_job)
    !  Purpose: Returns next bunch of up to n jobs, if jobs(J_EP)<=
    !  jobs(J_STP) there are no more jobs there, else returns the jobs
    !  done by the procs should be jobs(J_STP) + 1 to jobs(J_EP) in
    !  the related job list
    !  first the jobs are tried to get from the local storage of the
    !  current proc
    !  if there are not enough it will wait for either new ones to arrive
    !  or termination, the return value of my_jobs should only contain
    !  my_jobs(J_STP) == my_jobs(J_EP) if all procs are terminated
    !
    ! Context: main thread.
    !
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: n
    integer(kind=i4_kind), intent(out  ) :: my_job(L_JOB)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(i4_kind), target             :: jobs(SJOB_LEN)
    !------------ Executable code --------------------------------
    ! First try to get a job from local storage
    call th_mutex_lock(LOCK_JS)
    call local_tgetm(n, jobs)
    call th_mutex_unlock(LOCK_JS)
    call time_stamp("finished first local search",3)
    ! if local storage only gives empty job: cycle under checking for termination
    ! only try new job from local_storage after J_STP told that there are any
    call th_mutex_lock(LOCK_JS)
    do while (jobs(J_STP) >= jobs(J_EP) .and. .not. termination())
       ! found no job in first try, now wait for change before doing anything
       ! CONTROL will make a wake up
       call th_cond_signal(COND_JS_UPDATE)
       call time_stamp("MAIN wakes CONTROL urgently",3)
       i_am_waiting = .true.
       call th_cond_wait(COND_JS2_UPDATE, LOCK_JS)
       i_am_waiting = .false.
       call local_tgetm(n, jobs)
    enddo
    call th_mutex_unlock(LOCK_JS)
    call time_stamp("finished loop over local search",3)
    ! here we should have a valid job slice with at least one valid job
    ! or a terminated algorithm
    ! only the start and endpoint of job slice is needed external
    if (jobs(J_STP) >= jobs(J_EP)) then
       call th_join_all()
       ! now only one thread left, thus all variables belong him:
       print *, my_rank, "C: tried", many_searches, "for new jobs andstealing", many_tries
       print *, my_rank, "C: locked", many_zeros
       print *, my_rank, "C: longest wait for answer", timemax
       !print *, my_rank, "CONTROL: tried", many_searches, "times to get new jobs, by trying to steal", many_tries
       !print *, my_rank, "CONTROL: done", my_resp_self, "of my own, gave away", my_resp_start - my_resp_self, "and stole", your_resp
       !print *,my_rank, "MAILBOX: got ", count_messages, "messages with", count_requests, "requests and", count_offers, "offers"
    endif
      ! if true means MAIN did not intent to come back (check termination is
      ! too dangerous, because MAIN may still have work for one go and thus
      ! would try to join the thread in the next cycle again
    my_job = jobs(:L_JOB)
  end subroutine dlb_give_more

  logical function termination()
    ! Purpose: make lock around terminated, but have it as one function
    !
    ! Context: main, control, and mailbox threads.
    !
    ! Locks: rdlock
    !
    implicit none
    ! *** end of interface ***

    call rdlock()
    termination = terminated
    call unlock()
  end function termination

  integer(i4_kind) function decrease_resp(n)
    ! Purpose: make lock around my_resp, decrease it by done jobs
    !          and return updated value for further inspections
    !
    ! Context: control, and mailbox threads.
    !
    ! Locks: wrlock
    !
    implicit none
    integer(i4_kind), intent(in)  :: n
    ! *** end of interface ***

    call wrlock()
    my_resp = my_resp - n
    decrease_resp = my_resp
    call unlock()
  end function decrease_resp

  subroutine thread_mailbox() bind(C)
    ! Puropse: This routine should contain all that should be done by
    !          the thread MAILBOX
    !          Thus: mainly check for messages and try to finish the
    !                sended messages (as long as not terminated)
    !                as termination will be done by a message send here
    !                there is no fear that MAILBOX will be stuck while
    !                all others already finished
    !               the clean up of the finished messges is also contained
    !
    ! Context: entry to mailbox thread.
    !
    !------------ Modules used ------------------- ---------------
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: i, stat(MPI_STATUS_SIZE)
    integer(kind=i4_kind)                :: ierr
    integer(kind=i4_kind),allocatable    :: requ_m(:) !requests storages for MAILBOX
    !------------ Executable code --------------------------------

    do while (.not. termination())
      call check_messages(requ_m)
      if (allocated(requ_m)) call test_requests(requ_m)
    enddo

    ! now finish all messges still available, no matter if they have been received
    if (allocated(requ_m)) then
      do i = 1, size(requ_m,1)
        call MPI_CANCEL(requ_m(i), ierr)
        !ASSERT(ierr==MPI_SUCCESS)
        call assert_n(ierr==MPI_SUCCESS, 4)
        call MPI_WAIT(requ_m(i),stat, ierr)
        !ASSERT(ierr==MPI_SUCCESS)
        call assert_n(ierr==MPI_SUCCESS, 4)
      enddo
      deallocate(requ_m)
    endif

    ! MAILBOX should be the first thread to get the termination
    ! if CONTROL is stuck somewhere waiting, this here will
    ! wake it up, so that it can find out about the termination
    ! CONTROL should then wake up MAIN if neccessary

    call th_mutex_lock( LOCK_NJ)
    call th_cond_signal(COND_NJ_UPDATE)
    call th_mutex_unlock( LOCK_NJ)

    call th_mutex_lock( LOCK_JS)
    call th_cond_signal(COND_JS_UPDATE)
    call th_mutex_unlock( LOCK_JS)

    call time_stamp("exitmailbox", 5)

    call th_exit() ! will be joined on MAIN thread
  end subroutine thread_mailbox

  subroutine test_requests(requ)
    ! Purpose: tests if any of the messages stored in requ have been
    !          received, than remove the corresponding request
    !          requ is local request of the corresponding thread
    !
    ! Context: mailbox thread, control thread.
    !
    !------------ Modules used ------------------- ---------------
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer, allocatable :: requ(:)
    integer(kind=i4_kind)                :: j,i,req, stat(MPI_STATUS_SIZE), len_req, len_new
    integer(kind=i4_kind)                :: alloc_stat, ierr
    integer(kind=i4_kind),allocatable :: requ_int(:)
    logical     :: flag
    logical, allocatable :: finished(:)
    !------------ Executable code --------------------------------
    len_req = size(requ)
    allocate(finished(len_req), requ_int(len_req), stat = alloc_stat)
    !ASSERT(alloc_stat==0)
    call assert_n(alloc_stat==0, 4)
    finished = .false.
    len_new = len_req
    do i = 1, len_req
      req = requ(i)
      call MPI_TEST(req, flag, stat, ierr)
      !ASSERT(ierr==MPI_SUCCESS)
      call assert_n(ierr==MPI_SUCCESS, 4)
      if (flag) then
        finished(i) = .true.
        len_new = len_new - 1
      endif
    enddo
    requ_int(:) = requ(:)
    deallocate(requ, stat = alloc_stat)
    !ASSERT(alloc_stat==0)
    call assert_n(alloc_stat==0, 4)
    if (len_new > 0) then
      allocate(requ(len_new), stat = alloc_stat)
      !ASSERT(alloc_stat==0)
      call assert_n(alloc_stat==0, 4)
      j = 0
      do i = 1, len_req
        if (.not. finished(i)) then
          j = j + 1
          requ(j) = requ_int(i)
        endif
      enddo
    endif
    deallocate(requ_int, finished, stat = alloc_stat)
    !ASSERT(alloc_stat==0)
    call assert_n(alloc_stat==0, 4)
  end subroutine test_requests

  subroutine thread_control() bind(C)
    !  Purpose: main routine for thread CONTROL
    !          what it does: it waits for any change in the job_storage
    !          and finds out when it has been emptied
    !          then it gets active and searches for jobs by the other
    !          procs
    !          here it has to be careful to find out if everything has been terminated
    !          it selects the one to get a job form and sends a request, then it waits
    !          for MAILBOX notifying that it got some new jobs
    !          they may be empty (MAILBOX just copies them), then it has to try someone else
    !          if it got new jobs, find out if MAIN has arrived at getting jobs already and
    !          wake him up if yes. Then start all over
    !          remove jobs form new_jobs, as it may get back there not only after message of jobs
    !          but also after termination message (MAILBOX will wake it up)
    !          after termination first check if MAIN is waiting, wake it, than exit
    !
    ! Context: entry to control thread.
    !
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: select_victim
    implicit none
    !------------ Declaration of formal parameters ---------------
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: i, v, ierr, stat(MPI_STATUS_SIZE)
    integer(kind=i4_kind)                :: message(1 + SJOB_LEN), requ_wr
    integer(kind=i4_kind)                :: my_jobs(SJOB_LEN)
    integer(kind=i4_kind),allocatable    :: requ_c(:) !requests storages for CONTROL
    double precision :: timestart, timeend
    !------------ Executable code --------------------------------
    many_tries = 0
    many_searches = 0
    many_zeros = 0
    my_resp_self = 0
    your_resp = 0
    timemax = 0.0
    ! message is always the same (WORK_REQUEST)
    message = 0
    message(1) = WORK_REQUEST

    ! first lock
    call th_mutex_lock(LOCK_JS)
    do while (.not. termination()) ! while active
      if (.not. i_am_waiting) then
        call time_stamp("CONTROL waits jobs to end",5)
        call th_cond_wait(COND_JS_UPDATE, LOCK_JS)  ! unlockes LOCK_JS while waiting for change in it
        endif
        call time_stamp("CONTROL woke up", 5)
      !endif

      ! verify that CONTROL woke up correctly
      if (job_storage(J_STP) >= job_storage(J_EP)) then !point where CONTROL actually has something to do
        call time_stamp("COTNROL starts working", 4)
        many_searches = many_searches + 1 ! just for debugging

        ! not if masterserver is wanted, then there is no need for the termination algorithm, master knows
        ! where are all jobs by its own
        if (.not. masterserver) then
           call report_or_store(job_storage(:SJOB_LEN), requ_c)
        endif

        my_jobs = job_storage(:SJOB_LEN)
        call th_mutex_unlock(LOCK_JS)

        call th_mutex_lock(LOCK_NJ) ! LOCKED NJ but unlocked LOCK_JS
        do while (.not. termination() .and. (my_jobs(J_STP) >= my_jobs(J_EP))) ! two points to stop: if there are again new jobs
                                                                               ! or if all is finished
          many_tries = many_tries + 1 ! just for debugging

          if (masterserver) then !!! masterserver variant, here send all job request to master
            v = termination_master
          else ! if not masterserver, there needs to be done a bit more to find out who is the
               ! victim
            v = select_victim(my_rank, n_procs)
          endif

          timestart = MPI_WTIME()
          call time_stamp("CONTROL sends message",5)
          call MPI_ISEND(message, 1+SJOB_LEN, MPI_INTEGER4, v, MSGTAG, comm_world, requ_wr, ierr)
          !ASSERT(ierr==MPI_SUCCESS)
          call assert_n(ierr==MPI_SUCCESS, 4)
          if (allocated(requ_c)) call test_requests(requ_c)
          call time_stamp("CONTROL waits for reply", 5)
          call th_cond_wait(COND_NJ_UPDATE, LOCK_NJ) !while waiting is unlocked for MAILBOX
          timeend = MPI_WTIME()
          if ((timeend - timestart) > timemax) timemax = timeend - timestart
          call time_stamp("COTNROL got reply", 5)
            my_jobs = new_jobs
            new_jobs(J_STP) = 0
            new_jobs(J_EP) = 0

          ! at this point message should have been arrived (there is an answer back!)
          ! but it may also be that the answer message is from termination master
          ! thus cancel to get sure, that there is no dead lock in the MPI_WAIT
          call MPI_CANCEL(requ_wr, ierr)
          !ASSERT(ierr==MPI_SUCCESS)
          call assert_n(ierr==MPI_SUCCESS, 4)

          call MPI_WAIT(requ_wr, stat, ierr)
          !ASSERT(ierr==MPI_SUCCESS)
          call assert_n(ierr==MPI_SUCCESS, 4)
          if (my_jobs(J_STP) >= my_jobs(J_EP)) many_zeros = many_zeros + 1
        enddo
        call th_mutex_unlock(LOCK_NJ) ! unlock LOCK_NJ

        call th_mutex_lock(LOCK_JS)
        job_storage(:SJOB_LEN) = my_jobs
        start_job = my_jobs
      endif

      if (i_am_waiting) then
        ! tell MAIN that there is something in the storage
        ! as it will wait at cond_js2_update if run out if jobs (and not terminated)
        call time_stamp("CONTROL wake MAIN",5)
        i_am_waiting = .false.
        call th_cond_signal(COND_JS2_UPDATE)
      endif
    enddo !while active
    call th_mutex_unlock(LOCK_JS)

    ! shut down
    if (allocated(requ_c)) then
      do i = 1, size(requ_c,1)
        call MPI_CANCEL(requ_c(i), ierr)
        !ASSERT(ierr==MPI_SUCCESS)
        call assert_n(ierr==MPI_SUCCESS, 4)

        call MPI_WAIT(requ_c(i),stat, ierr)
        !ASSERT(ierr==MPI_SUCCESS)
        call assert_n(ierr==MPI_SUCCESS, 4)
      enddo
      deallocate(requ_c)
    endif

    call th_mutex_lock(LOCK_JS)
    call th_cond_signal(COND_JS2_UPDATE) ! free MAIN if waiting
    call th_mutex_unlock(LOCK_JS)

    call time_stamp("CONTROL exit",4)
    call th_exit() ! will be joined on MAIN
  end subroutine thread_control

  subroutine check_messages(requ_m)
    !  Purpose: checks if any message has arrived, checks for messages:
    !          Someone finished stolen job slice
    !          Someone has finished its responsibilty (only termination_master)
    !          There are no more jobs (message from termination_master to finish)
    !          Job Request from another proc
    !          Job sended by another proc (after CONTROL sended a request)
    !          Except the last one, they are all handled only by this thread
    !          only after got job, put it in intermediate storage and wake CONTROL
    !
    ! Context: mailbox thread.
    !
    ! Locks: LOCK_NJ,  wrlock * 2
    !        - through divide_jobs(): LOCK_JS, wrlock.
    !
    ! Signals: COND_NJ_UPDATE
    !        - through divide_jobs(): COND_JS_UPDATE.
    !
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer, allocatable :: requ_m(:)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, stat(MPI_STATUS_SIZE), req
    integer(kind=i4_kind)                :: message(1 + SJOB_LEN)
    integer(kind=i4_kind)                :: my_resp_local
    !------------ Executable code --------------------------------

    ! check and wait for any message with messagetag dlb
    call MPI_RECV(message, 1+SJOB_LEN, MPI_INTEGER4, MPI_ANY_SOURCE, MSGTAG, comm_world, stat, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, my_rank)

    !print *, my_rank, "got message", message, "from", stat(MPI_SOURCE)
    call time_stamp("got message", 4)
    count_messages = count_messages + 1

    select case(message(1))

    case (DONE_JOB) ! someone finished stolen job slice
      !ASSERT(message>0)
      call assert_n(message(2)>0, 4)

      if (decrease_resp(message(2)) == 0) then
        call send_resp_done( requ_m)
      endif

    case (RESP_DONE) ! finished responsibility
      ! arrives only on termination master:
      call assert_n(my_rank == termination_master, 19)

      call check_termination(message(2))

    case (NO_WORK_LEFT) ! termination message from termination master
      !ASSERT(message(2)==0)
      call assert_n(message(2)==0, 4)
      !ASSERT(stat(MPI_SOURCE)==termination_master)
      call assert_n(stat(MPI_SOURCE)==termination_master, 4)

      call wrlock()
      terminated = .true.
      call unlock()

    case (WORK_DONAT) ! got work from other proc
      call th_mutex_lock( LOCK_NJ)
      count_offers = count_offers + 1
      new_jobs = message(2:)
      call th_cond_signal(COND_NJ_UPDATE)
      call th_mutex_unlock( LOCK_NJ)  

    case (WORK_REQUEST) ! other proc wants something from my jobs
      count_requests = count_requests + 1

      if (divide_jobs(stat(MPI_SOURCE), req)) then
        call add_request(req, requ_m)
      endif

    case default
      ! This message makes no sense in this context, thus give warning
      ! and continue (maybe the actual calculation has used it)
      print *, "ERROR:", my_rank," got message with unexpected content:", message
      stop "got unexpected message"

    end select
  end subroutine check_messages

  subroutine add_request(req, requ)
    ! Purpose: stores unfinished requests
    !
    ! Context: control thread, mailbox thread.
    !
    ! Locks: none.
    !
    implicit none
    integer, intent(in) :: req
    integer, allocatable :: requ(:)
    ! *** end of interface ***

    integer, allocatable :: req_int(:)
    integer :: alloc_stat, len_req

    len_req = 0
    if (allocated(requ)) then
      len_req = size(requ,1)
      allocate(req_int(len_req), stat = alloc_stat)
      !ASSERT(alloc_stat==0)
      call assert_n(alloc_stat==0, 4)
      req_int = requ
      deallocate(requ, stat = alloc_stat)
      !ASSERT(alloc_stat==0)
      call assert_n(alloc_stat==0, 4)
    endif
    allocate(requ(len_req +1), stat = alloc_stat)
    !ASSERT(alloc_stat==0)
    call assert_n(alloc_stat==0, 4)
    if (len_req > 0) requ(:len_req) = req_int
    requ(len_req +1) = req
  end subroutine add_request

  subroutine check_termination(proc)
    !  Purpose: only on termination_master, checks if all procs
    !           have reported termination
    !
    ! Context: mailbox thread, control. (termination master only)
    !
    ! Locks: wrlock on "terminated" flag.
    !
    ! Signals: none.
    !
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in)    :: proc
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, i, alloc_stat, req_self, stat(MPI_STATUS_SIZE)
    integer(kind=i4_kind), allocatable   :: request(:), stats(:,:)
    integer(kind=i4_kind)                :: receiver, message(1+SJOB_LEN)
    !------------ Executable code --------------------------------
    ! all_done stores the procs, which have already my_resp = 0
    all_done(proc+1) = .true.

    ! check if there are still some procs not finished
    if (.not. all(all_done)) RETURN

    ! there will be only a send to the other procs, telling them to terminate
    ! thus the termination_master sets its termination here
    call wrlock()
    terminated = .true.
    call unlock()

    ! masterserver handles termination seperatly, this here is only for its own termination
    ! I'm not sure what MPI does with requests of the size 0, thus quit if there is only one
    ! processor
    if (masterserver .or. n_procs == 1) RETURN

    allocate(request(n_procs -1), stats(n_procs -1, MPI_STATUS_SIZE),&
    stat = alloc_stat)
    !ASSERT(alloc_stat==0)
    call assert_n(alloc_stat==0,9)
    message(1) = NO_WORK_LEFT
    message(2:) = 0
    do i = 0, n_procs-2
    receiver = i
    ! skip the termination master (itsself)`
    if (i >= termination_master) receiver = i+1
    call time_stamp("send termination", 5)
    call MPI_ISEND(message, 1+SJOB_LEN, MPI_INTEGER4, receiver, MSGTAG,comm_world ,request(i+1), ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    enddo
    call MPI_WAITALL(size(request), request, stats, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
  end subroutine check_termination

  subroutine send_resp_done(requ)
    !  Purpose: my_resp holds the number of jobs, assigned at the start
    !           to this proc, so this proc is responsible that they will
    !           be finished, if my_resp == 0, I should tell termination_master
    !           that, termination master will collect all the finished resp's
    !           untill it gots all of them back
    !
    ! Context: mailbox  and control thread.
    !
    ! Locks: none.
    !
    ! Signals: none.
    !
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    !** End of interface *****************************************
    integer(kind=i4_kind), allocatable   :: requ(:)
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, req
    integer(kind=i4_kind)                :: message(1+SJOB_LEN)
    !------------ Executable code --------------------------------

    message(1) = RESP_DONE
    message(2) = my_rank
    message(3:) = 0
    call MPI_ISEND(message, 1+SJOB_LEN, MPI_INTEGER4, termination_master, MSGTAG,comm_world, req, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    call add_request(req, requ)
  end subroutine send_resp_done

  subroutine local_tgetm(m, my_jobs)
    !  Purpose: takes m jobs from the left from object job_storage
    !           in the first try there is no need to wait for
    !           something going on in the jobs
    !
    ! Context: MAIN thread.
    !
    ! Locks: complete function is locked by LOCK_JS from outside
    !
    ! Conditions: COND_JS_UPDATE
    !              waits on COND_JS2_UPDATE
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: reserve_workm
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: m
    integer(kind=i4_kind), intent(out  ) :: my_jobs(SJOB_LEN)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: w
    !------------ Executable code --------------------------------
    w = reserve_workm(m, job_storage) ! how many jobs to get
    my_jobs = job_storage(:SJOB_LEN) ! first SJOB_LEN hold the job
    my_jobs(J_EP)  = my_jobs(J_STP) + w
    job_storage(J_STP) = my_jobs(J_EP)
    if (job_storage(J_STP) >= job_storage(J_EP)) then
      call time_stamp("MAIN wakes CONTROL",3)
      call th_cond_signal(COND_JS_UPDATE)
    endif
    !endif
  end subroutine local_tgetm

  subroutine report_or_store(my_jobs, requ)
    !  Purpose: If a job is finished, this cleans up afterwards
    !           Needed for termination algorithm, there are two
    !           cases, it was a job of the own responsibilty or
    !           one from another, first case just change my number
    !            second case, send to victim, how many of his jobs
    !            were finished
    !
    ! Context: control thread.
    !
    ! Locks: wrlock,
    !        wrlock through decrease_resp()
    !
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in  ) :: my_jobs(SJOB_LEN)
    integer(kind=i4_kind), allocatable  :: requ(:)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, req
    integer(kind=i4_kind)                :: my_resp_local
    integer(kind=i4_kind)                :: num_jobs_done, message(1 + SJOB_LEN)
    !------------ Executable code --------------------------------
    call time_stamp("finished a job",4)
    ! my_jobs hold recent last point, as proc started from beginning and
    ! steal from the back, this means that from the initial starting point on
    ! (stored in start_job) to this one, all jobs were done
    ! if my_jobs(J_EP)/= start_job(J_EP) someone has stolen jobs
    num_jobs_done = my_jobs(J_EP) - start_job(J_STP)
    !if (num_jobs_done == 0) return ! there is non job, thus why care
    if (start_job(NRANK) == my_rank) then
      my_resp_self = my_resp_self + num_jobs_done
      if(decrease_resp(num_jobs_done)== 0) then ! if all my jobs are done
         call send_resp_done(requ)
      endif
    else
      your_resp = your_resp + num_jobs_done
      ! As all isends have to be closed sometimes, storage of
      ! the request handlers is needed
      message(1) = DONE_JOB
      message(2) = num_jobs_done
      message(3:) = 0
      call time_stamp("send message to source", 6)
      call MPI_ISEND(message,1 + SJOB_LEN, MPI_INTEGER4, start_job(NRANK),&
                                  MSGTAG,comm_world, req, ierr)
      !ASSERT(ierr==MPI_SUCCESS)
      call assert_n(ierr==MPI_SUCCESS, 4)
      call add_request(req, requ)
    endif
  end subroutine report_or_store

  logical function divide_jobs(partner, requ)
    !  Purpose: share jobs from job_storage with partner, tell
    !           partner what he got
    !
    ! Context: mailbox thread.
    !
    ! Locks: LOCK_JS.
    !
    ! Signals: COND_JS_UPDATE.
    !
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: divide_work, divide_work_master
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: partner
    integer(kind=i4_kind), intent(out  ) :: requ
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, w
    integer(kind=i4_kind)                :: g_jobs(SJOB_LEN), message(1 + SJOB_LEN)
    !------------ Executable code --------------------------------
    divide_jobs = .true.
    call th_mutex_lock(LOCK_JS)

    if (masterserver) then
      !!! variant with masterserver: here only a part of the jobs available may be
      !!! given away
      w = divide_work_master(job_storage, n_procs)
    else
      !!! chare the jobs equally distributed
      w = divide_work(job_storage)
    endif

    g_jobs = job_storage
    message(1) = WORK_DONAT
    if (w == 0) then ! nothing to give, set empty

      !!! variant with master, if master cannot give work back, there is no more
      ! work for the proc, thus tell him to terminate
      if (masterserver) then
         message(1) = NO_WORK_LEFT
         call check_termination(partner)
         ! don't send too many messages to myself, anyhow, termination master
         ! has to wait, till all procs got termination send back
         if (partner == termination_master) then
           divide_jobs = .false.
           call th_mutex_unlock(LOCK_JS)
           return
         endif
      endif
      !!! end variant with master

      g_jobs(J_EP)  = 0
      g_jobs(J_STP) = 0
      g_jobs(NRANK) = -1
    else ! take the last w jobs of the job-storage
      g_jobs(J_STP)  = g_jobs(J_EP) - w
      job_storage(J_EP) = g_jobs(J_STP)
    endif
    message(2:) = g_jobs
    call time_stamp("chare jobs with other",5)
    call MPI_ISEND(message, 1+SJOB_LEN, MPI_INTEGER4, partner, MSGTAG, comm_world, requ, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    if (job_storage(J_STP) >= job_storage(J_EP)) then
      call time_stamp("MAILBOX wake CONTROL", 4)
      call th_cond_signal(COND_JS_UPDATE)
    endif
    !endif
    call th_mutex_unlock(LOCK_JS)
  end function divide_jobs

  subroutine dlb_setup(job)
    !  Purpose: initialization of a dlb run, each proc should call
    !           it with inital jobs. The inital jobs should be a
    !           static distribution of all available jobs, each job
    !           should be given to one and only one of the procs
    !           jobs should be given as a job range (STP, EP), where
    !           all jobs should be the numbers from START to END, with
    !           START <= STP <= EP <= END
    !
    ! Starts other Threads, runs on MAIN
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: job(L_JOB)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    !------------ Executable code --------------------------------
    ! these variables are for the termination algorithm
    terminated = .false.
    i_am_waiting = .false.
    count_messages = 0
    count_offers = 0
    count_requests = 0
    if (allocated(all_done)) all_done  = .false.
    ! set starting values for the jobs, needed also in this way for
    ! termination, as they will be stored also in the job_storage
    ! start_job should only be changed if all current jobs are finished
    ! and there is a try to steal new ones
    start_job(:L_JOB) = job
    start_job(NRANK) = my_rank
    ! needed for termination
    my_resp = start_job(J_EP) - start_job(J_STP)
    my_resp_start = my_resp
    ! Job storage holds all the jobs currently in use
    job_storage(:SJOB_LEN) = start_job
    ! from now on, there are several threads, so chared objects have to
    ! be locked/unlocked in order to use them!!
    call th_create_all()
    call time_stamp("finished setup", 3)
  end subroutine dlb_setup

  subroutine rdlock()
    implicit none
    ! *** end of interface ***

    call th_rwlock_rdlock(0)
  end subroutine rdlock

  subroutine wrlock()
    implicit none
    ! *** end of interface ***

    call th_rwlock_wrlock(0)
  end subroutine wrlock

  subroutine unlock()
    implicit none
    ! *** end of interface ***

    call th_rwlock_unlock(0)
  end subroutine unlock

end module dlb
