!===============================================================
! Public interface of module
!===============================================================
module dlb_impl
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
# include "dlb.h"
  use dlb_common, only: i4_kind, r8_kind, comm_world
  use dlb_common, only: time_stamp, time_stamp_prefix ! for debug only
  use dlb_common, only: add_request, test_requests, end_requests, send_resp_done
  use dlb_common, only: DONE_JOB, NO_WORK_LEFT, RESP_DONE, JLENGTH, L_JOB, JOWNER, JLEFT, JRIGHT, MSGTAG
  use dlb_common, only: WORK_DONAT, WORK_REQUEST
  use dlb_common, only: my_rank, n_procs, termination_master, set_start_job, set_empty_job
  use dlb_common, only: dlb_common_setup, has_last_done, send_termination
  use dlb_common, only: clear_up
  use dlb_common, only: masterserver
  use dlb_common, only: end_communication
  use iso_c_binding
  use dlb_impl_thread_common
  USE_MPI
  implicit none
  !use type_module ! type specification parameters
  save            ! save all variables defined in this module
  private         ! by default, all names are private
  !== Interrupt end of public interface of module =================
  ! Program from outside might want to know the thread-safety-level required form DLB
  integer(kind=i4_kind), parameter, public :: DLB_THREAD_REQUIRED = MPI_THREAD_MULTIPLE

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

  !------------ Declaration of types ------------------------------

  !------------ Declaration of constants and variables ----
  ! IDs of mutexes, use base-0 indices:
  integer(kind=i4_kind), parameter :: LOCK_NJ   = 1

  ! IDs for condition variables, use base-0 indices:
  integer(kind=i4_kind), parameter :: COND_JS_UPDATE  = 0
  integer(kind=i4_kind), parameter :: COND_NJ_UPDATE  = 1
  integer(kind=i4_kind), parameter :: COND_JS2_UPDATE = 2

  ! thread IDs, use base-0 indices:
  integer(kind=i4_kind), parameter :: MAILBOX = 0
  integer(kind=i4_kind), parameter :: CONTROL = 1

   ! for the complete termination, count_ask will be filled by CONTROL and interpreted by MAILBOX, proc_asked_last 
   ! is used by both of them
   integer(kind=i4_kind),allocatable    :: count_ask(:)
   integer(kind=i4_kind)                :: proc_asked_last

  integer(kind=i4_kind)             :: new_jobs(JLENGTH) ! stores new job, just arrived from other proc
  integer(kind=i4_kind)             :: already_done ! belongs to CONTROL after initalising, stores how
                                      ! many jobs of the current interval have been already calculated
  integer(kind=i4_kind)             :: start_job(JLENGTH) ! job_storage is changed a lot, backup for
                                     ! finding out, if someone has stolen something, or how many jobs one
                                     ! has done, after initalization only used by CONTROL
                                              ! in setup
  logical                           :: i_am_waiting ! Thead 0 (main thread) is waiting for CONTROL

  ! These variables are also essentiel but as they are also needed in dlb_impl_thread_common and to avoid
  ! cyclic dependencies they are stored there:
  !integer(kind=i4_kind), parameter :: LOCK_JS   = 0 !declared in dlb_impl_thread_common
  !logical, parameter :: masterserver = .false. ! changes to different variant (master slave concept for comparision)
  !integer(kind=i4_kind)             :: job_storage(jobs_len) ! store all the jobs, belonging to this processor
  !logical                           :: terminated ! for termination algorithm

  ! some variables are shared between the three threads, there are also locks and conditions to obtain
  ! following a list, which of the three threads (MAIN, CONTROL, MAILBOX) does read or write on them
  !
  ! "BIG KERNEL LOCK", rwlock for global data (rdlock/wrlock/unlock):
  !
  ! * terminated: this is the flag, telling everybody, that it's done
  !             read: ALL
  !             write: CONTROL, MAILBOX
  ! * reported_by, reported_to:
  !          read: CONTROL, MAILBOX
  !          write: CONTROL, MAILBOX
  !          for termination algorithm, CONTROL and MAILBOX may have knowledge about finished jobs
  !          stored in dlb_common, after other threads have been initalized, will be only accessed
  !          by functions report_to/report_by
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
  ! * already_done: read: CONTROL, MAIN
  !                 write: CONTROL, MAIN
  !                 MAIN stores here how many jobs of the current job interval it has done,
  !                 CONTROL needs and resets this information when reporting jobs for the
  !                 termination algorithm
  !
  ! LOCK_NJ:
  ! * new_jobs:   read: CONTROL          locked by LOCK_NJ
  !             write: CONTROL, MAILBOX
  !             MAILBOX stores here messages, gotten for CONTROL, CONTROL "deletes" them after reading
  ! * count_ask: read: MAILBOX
  !              write: CONTROL
  ! * proc_asked_last: read: MAILBOX
  !                    write: CONTROL, MAILBOX
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
  integer(kind=i4_kind)             :: many_tries, many_searches !how many times asked for jobs, CONTROL
  integer(kind=i4_kind)             :: many_zeros !how many times asked for jobs, CONTROL
  double precision  :: timemax
  ! further for main
  double precision  :: main_wait_all, main_wait_max, main_wait_last
  double precision  :: max_work, last_work, average_work, num_jobs
  !----------------------------------------------------------------
  !------------ Subroutines ---------------------------------------
contains

  subroutine dlb_init()
    !  Purpose: initalization of needed stuff
    !           is in one thread context
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: output_border
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    call dlb_thread_init()
    if (my_rank == 0 .and. 2 < output_border) then
        print *, "DLB init: using variant 'thread multiple'"
        print *, "DLB init: This variant needs additinalal threads from Pthreads"
        print *, "DLB init: Please ensure that MPI has at least MPI_THREAD_MULTIPLE"
    endif
  end subroutine dlb_init

  subroutine dlb_finalize()
    !  Purpose: cleaning up everything, after last call
    !
    ! Context: main thread after joining other two.
    !
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: dlb_common_finalize
    implicit none
    !** End of interface *****************************************
    call dlb_common_finalize()
  end subroutine dlb_finalize

  subroutine dlb_give_more(n, my_job)
    !  Purpose: Returns next bunch of up to n jobs, if my_job(2)<=
    !  my_job(1) there are no more jobs there, else returns the jobs
    !  done by the procs should be my_job(1) + 1 to my_job(2) in
    !  the related job list
    !  first the jobs are tried to get from the local storage of the
    !  current proc
    !  if there are not enough it will wait for either new ones to arrive
    !  or termination, the return value of my_jobs should only contain
    !  my_job(1) == my_job(2) if all procs are terminated
    !
    ! Context: main thread.
    !
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: output_border
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: n
    integer(kind=i4_kind), intent(out  ) :: my_job(:) ! (2)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(i4_kind), target             :: jobs(JLENGTH)
    double precision                     :: start_timer
    double precision,save                :: leave_timer = -1
    !------------ Executable code --------------------------------

    ASSERT(size(my_job)==2)

    if (num_jobs > 0) then ! for debugging
        last_work = MPI_Wtime() - leave_timer
        if (last_work > max_work) max_work = last_work
        average_work = average_work + last_work
    endif
    ! First try to get a job from local storage
    call th_mutex_lock(LOCK_JS)
    call local_tgetm(n, jobs)
    call th_mutex_unlock(LOCK_JS)
    call time_stamp("finished first local search",3)
    ! if local storage only gives empty job: cycle under checking for termination
    ! only try new job from local_storage after JLEFT told that there are any
    call th_mutex_lock(LOCK_JS)
    do while (jobs(JLEFT) >= jobs(JRIGHT) .and. .not. termination())
       ! found no job in first try, now wait for change before doing anything
       ! CONTROL will make a wake up
       call th_cond_signal(COND_JS_UPDATE)
       call time_stamp("MAIN wakes CONTROL urgently",3)
       i_am_waiting = .true.
       start_timer = MPI_Wtime() ! for debugging
       call th_cond_wait(COND_JS2_UPDATE, LOCK_JS)
       i_am_waiting = .false.
       main_wait_last = MPI_Wtime() - start_timer ! for debugging
       ! store debugging information:
       main_wait_all = main_wait_all + main_wait_last
       if (main_wait_last > main_wait_max) main_wait_max = main_wait_last
       ! end store debugging information
       call local_tgetm(n, jobs)
    enddo
    call th_mutex_unlock(LOCK_JS)
    call time_stamp("finished loop over local search",3)
    ! here we should have a valid job slice with at least one valid job
    ! or a terminated algorithm
    ! only the start and endpoint of job slice is needed external
    if (jobs(JLEFT) >= jobs(JRIGHT)) then
       call th_join_all()
       ! now only one thread left, thus all variables belong him:
       if (1 < output_border) then
           print *, my_rank, "C: tried", many_searches, "for new jobs andstealing", many_tries
           print *, my_rank, "C: zeros", many_zeros
           print *, my_rank, "C: longest wait for answer", timemax
           print *, my_rank, "M: got ", count_messages, "messages with", count_requests, "requests and", count_offers, "offers"
           write(*, '(I3, " C: longest wait for answer", G20.10)'), my_rank, timemax
           write(*, '(I3, " M: waited (all, max, last)", G20.10, G20.10, G20.10) '), my_rank, &
                  main_wait_all, main_wait_max, main_wait_last
           write(*, '(I3, " M: work slices lasted (average, max, last)", G20.10, G20.10, G20.10)'), my_rank,&
              average_work/ num_jobs, max_work, last_work
       endif
    endif
      ! if true means MAIN did not intent to come back (check termination is
      ! too dangerous, because MAIN may still have work for one go and thus
      ! would try to join the thread in the next cycle again

    ! only the start and endpoint of job slice are needed outside:
    my_job(1) = jobs(JLEFT)
    my_job(2) = jobs(JRIGHT)
    leave_timer = MPI_Wtime() ! for debugging
    num_jobs = num_jobs + 1 ! for debugging
  end subroutine dlb_give_more

  subroutine thread_secretary() bind(C)
  end subroutine

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
    use dlb_common, only: output_border
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: stat(MPI_STATUS_SIZE)
    integer(kind=i4_kind)                :: ierr, alloc_stat
    integer(kind=i4_kind)                :: message(1 + JLENGTH)
    integer(kind=i4_kind),allocatable    :: requ_m(:) !requests storages for MAILBOX
    integer(kind=i4_kind)                :: lm_source(n_procs) ! remember which job request
                                                   ! I got
    !------------ Executable code --------------------------------

    lm_source = -1
    do while (.not. termination())
      ! check and wait for any message with messagetag dlb
      call MPI_RECV(message, 1+JLENGTH, MPI_INTEGER4, MPI_ANY_SOURCE, MSGTAG, comm_world, stat, ierr)
      ASSERT(ierr==MPI_SUCCESS)

      if (5 < output_border) print *, my_rank, "got message", message, "from", stat(MPI_SOURCE)
      call time_stamp("got message", 4)
      count_messages = count_messages + 1
      call check_messages(requ_m, message, stat, lm_source)
      call test_requests(requ_m)
    enddo

    ! MAILBOX should be the first thread to get the termination
    ! if CONTROL is stuck somewhere waiting, this here will
    ! wake it up, so that it can find out about the termination
    ! CONTROL should then wake up MAIN if neccessary

    call th_mutex_lock( LOCK_NJ)
    call clear_up(count_ask, proc_asked_last, lm_source, requ_m)
    deallocate(count_ask, stat = alloc_stat)
    ASSERT(alloc_stat==0)
    call th_cond_signal(COND_NJ_UPDATE)
    call th_mutex_unlock( LOCK_NJ)
    call end_communication()
    call end_threads()
    ! To ensure that no processor has already started with the next dlb iteration
    call MPI_BARRIER(comm_world, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    call th_mutex_lock( LOCK_JS)
    call th_cond_signal(COND_JS_UPDATE)
    call th_mutex_unlock( LOCK_JS)

    call time_stamp("exitmailbox", 5)

    call th_exit() ! will be joined on MAIN thread
  end subroutine thread_mailbox

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
    integer(kind=i4_kind)                :: v, ierr, stat(MPI_STATUS_SIZE)
    integer(kind=i4_kind)                :: message(1 + JLENGTH) ! there will be always
                                                 ! only one job request around, thus saved
    integer(kind=i4_kind)                :: requ_wr
    integer(kind=i4_kind)                :: my_jobs(JLENGTH)
    integer(kind=i4_kind),allocatable    :: requ_c(:) !requests storages for CONTROL
    double precision :: timestart, timeend
    !------------ Executable code --------------------------------
    many_tries = 0
    many_searches = 0
    many_zeros = 0
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
      if (job_storage(JLEFT) >= job_storage(JRIGHT)) then !point where CONTROL actually has something to do
        call time_stamp("COTNROL starts working", 4)
        many_searches = many_searches + 1 ! just for debugging

        ! not if masterserver is wanted, then there is no need for the termination algorithm, master knows
        ! where are all jobs by its own
        if (.not. masterserver) then
           call report_or_store(job_storage(JOWNER), already_done, requ_c)
           already_done = 0
        endif

        my_jobs = job_storage(:JLENGTH)
        call th_mutex_unlock(LOCK_JS)

        call th_mutex_lock(LOCK_NJ) ! LOCKED NJ but unlocked LOCK_JS
        do while (.not. termination() .and. (my_jobs(JLEFT) >= my_jobs(JRIGHT))) ! two points to stop: if there are again new jobs
                                                                               ! or if all is finished
          many_tries = many_tries + 1 ! just for debugging

          if (masterserver) then !!! masterserver variant, here send all job request to master
            v = termination_master
          else ! if not masterserver, there needs to be done a bit more to find out who is the
               ! victim
            v = select_victim(my_rank, n_procs)
          endif
          ! store informations on the last proc we have asked:
          ! first his number
          proc_asked_last = v
          ! then the request-message counter for the proc, we
          ! want to send to (consider overflow of integer)
          if (count_ask(v+1) < 1e9) then
            count_ask(v+1) = count_ask(v+1) + 1
          else
            count_ask(v+1) = 0
          endif
          ! send along the request-messge counter to identify the last received
          ! message of each proc in the termination phase
          message(2) = count_ask(v+1)

          timestart = MPI_WTIME()
          call time_stamp("CONTROL sends message",5)
          call MPI_ISEND(message, 1+JLENGTH, MPI_INTEGER4, v, MSGTAG, comm_world, requ_wr, ierr)
          ASSERT(ierr==MPI_SUCCESS)
          call test_requests(requ_c)
          call time_stamp("CONTROL waits for reply", 5)
          call th_cond_wait(COND_NJ_UPDATE, LOCK_NJ) !while waiting is unlocked for MAILBOX
          timeend = MPI_WTIME()
          if ((timeend - timestart) > timemax) timemax = timeend - timestart
          call time_stamp("CONTROL got reply", 5)
            my_jobs = new_jobs
            new_jobs(JLEFT) = 0
            new_jobs(JRIGHT) = 0

          ! at this point message should have been arrived (there is an answer back!)
          ! but it may also be that the answer message is from termination master
          ! the message will be answered any how, so just wait
          call MPI_WAIT(requ_wr, stat, ierr)
          ASSERT(ierr==MPI_SUCCESS)
          if (my_jobs(JLEFT) >= my_jobs(JRIGHT)) many_zeros = many_zeros + 1
        enddo
        call th_mutex_unlock(LOCK_NJ) ! unlock LOCK_NJ

        call th_mutex_lock(LOCK_JS)
        job_storage(:JLENGTH) = my_jobs
        ! reset for a new interval
        already_done = 0
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
    call end_requests(requ_c)

    call th_mutex_lock(LOCK_JS)
    call th_cond_signal(COND_JS2_UPDATE) ! free MAIN if waiting
    call th_mutex_unlock(LOCK_JS)

    call time_stamp("CONTROL exit",4)
    call th_exit() ! will be joined on MAIN
  end subroutine thread_control

  subroutine check_messages(requ_m, message, stat, lm_source)
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
    use dlb_common, only: report_by, reports_pending
    use dlb_common, only: print_statistics
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer, allocatable :: requ_m(:)
    integer, intent(in) :: message(1 + JLENGTH), stat(MPI_STATUS_SIZE)
    integer(kind=i4_kind), intent(inout) :: lm_source(:)
    !** End of interface *****************************************

    integer(i4_kind) :: pending

    select case(message(1))

    case (DONE_JOB) ! someone finished stolen job slice
      ASSERT(message(2)>0)
      ASSERT(stat(MPI_SOURCE)/=my_rank)

      !
      ! Handle fresh cumulative report:
      !
      call wrlock()
          ! this does modify global vars (in dlb_common):
          call report_by(stat(MPI_SOURCE), message(2))

          ! this is read-only:
          pending = reports_pending()
      call unlock()

      if ( pending == 0 ) then
        call send_resp_done( requ_m)
      endif

    case (RESP_DONE) ! finished responsibility
      ! arrives only on termination master:
      if( my_rank /= termination_master )then
          stop "my_rank /= termination_master"
      endif

      call check_termination(message(2))

    case (NO_WORK_LEFT) ! termination message from termination master
      ASSERT(message(2)==0)
      if( stat(MPI_SOURCE) /= termination_master )then
          stop "stat(MPI_SOURCE) /= termination_master"
      endif

      call print_statistics()
      call wrlock()
      terminated = .true.
      call unlock()

    case (WORK_DONAT) ! got work from other proc
      call th_mutex_lock( LOCK_NJ)
      count_offers = count_offers + 1
      new_jobs = message(2:)
      proc_asked_last = -1 ! set back, which proc to wait for
      call th_cond_signal(COND_NJ_UPDATE)
      call th_mutex_unlock( LOCK_NJ)  

    case (WORK_REQUEST) ! other proc wants something from my jobs
      count_requests = count_requests + 1
      ! store the intern message number, needed for knowing at the end the
      ! status of the last messages on their way
      lm_source(stat(MPI_SOURCE)+1) = message(2)

      call divide_jobs(stat(MPI_SOURCE), requ_m)

    case default
      ! This message makes no sense in this context, thus give warning
      ! and continue (maybe the actual calculation has used it)
      print *, "ERROR:", my_rank," got message with unexpected content:", message
      stop "got unexpected message"

    end select
  end subroutine check_messages

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
    use dlb_common, only: steal_local, length, set_empty_job
    use dlb_common, only: empty
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(i4_kind), intent(in)  :: m
    integer(i4_kind), intent(out) :: my_jobs(JLENGTH)
    !** End of interface *****************************************

    integer(i4_kind) :: remaining(JLENGTH)

    if ( steal_local(m, job_storage, remaining, my_jobs) ) then
        !
        ! Stealing successfull:
        !
        job_storage = remaining
    else
        !
        ! It is OK to return an empty job range from here:
        !
        my_jobs = set_empty_job()
    endif

    already_done = already_done + length(my_jobs)

    if ( empty(job_storage) ) then
      call time_stamp("MAIN wakes CONTROL",3)
      call th_cond_signal(COND_JS_UPDATE)
    endif
  end subroutine local_tgetm

  subroutine report_or_store(owner, num_jobs_done, requ)
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
    !        wrlock through decrease_resp_locked()
    !
    use dlb_common, only: report_to, reports_pending
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(i4_kind), intent(in)   :: owner
    integer(i4_kind), intent(in)   :: num_jobs_done
    integer(i4_kind), allocatable  :: requ(:)
    !** End of interface *****************************************

    integer(i4_kind) :: pending

    call time_stamp("finished a job",4)
    ! my_jobs hold recent last point, as proc started from beginning and
    ! steal from the back, this means that from the initial starting point on
    ! (stored in start_job) to this one, all jobs were done
    ! if my_jobs(JRIGHT)/= start_job(JRIGHT) someone has stolen jobs
    !if (num_jobs_done == 0) return ! there is non job, thus why care

    call wrlock()
        !
        ! Report scheduled jobs (modifies globals in dlb_common):
        !
        call report_to(owner, num_jobs_done)

        pending = reports_pending()
    call unlock()

    if ( owner == my_rank ) then
      if( pending == 0) then ! if all my jobs are done
         call send_resp_done(requ)
      endif
    endif
  end subroutine report_or_store

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
    use dlb_impl_thread_common, only: thread_setup, th_create_all
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: job(L_JOB)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer :: alloc_stat
    !------------ Executable code --------------------------------
    ! these variables are for the termination algorithm
    terminated = .false.
    i_am_waiting = .false.
    ! for debugging
    count_messages = 0
    count_offers = 0
    count_requests = 0
    main_wait_all = 0
    main_wait_max = 0
    main_wait_last = 0
    max_work = 0
    last_work = 0
    average_work = 0
    num_jobs = 0
    ! end for debugging
    allocate(count_ask(n_procs), stat = alloc_stat)
    ASSERT (alloc_stat==0)
    count_ask = -1
    proc_asked_last = -1
    ! set starting values for the jobs, needed also in this way for
    ! termination, as they will be stored also in the job_storage
    ! start_job should only be changed if all current jobs are finished
    start_job = set_start_job(job)
    call dlb_common_setup(start_job(JRIGHT) - start_job(JLEFT))
    ! Job storage holds all the jobs currently in use
    job_storage(:JLENGTH) = start_job
    ! from now on, there are several threads, so shared objects have to
    ! be locked/unlocked in order to use them!!
    call thread_setup()
    call th_create_all()
    call time_stamp("finished setup", 3)
  end subroutine dlb_setup

end module dlb_impl
