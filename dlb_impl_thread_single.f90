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
  !                         all mpi communication takes place in a separate thread (called SECRETARY)
  !                         as main difference to the three thread variant, here the MPI_RECV is non
  !                         blocking, meaning that the loop over it has to be made per hand (as not all
  !                         cluster provide easily the chance to have the blocking receive ideal for several
  !                         threads). This allows also for SECRETARY to take over the jobs from CONTROL, so that
  !                         now all MPI calls are on the same thread. Thus only MPI_THREAD_SERIALIZED is needed here.
  !
  !          termination algorithm: called (at least once) "Fixed Energy Distributed Termination
  !                                 Algorithm"
  !                        to avoid confusion, here the term "energy" is not used, talking about
  !                         respoinsibility (resp) instead, every system starts with a part of responsibility
  !                         given to him, if procs steal from him, they have later to send him a
  !                         message saying how many of his jobs, they have done, they always report to
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
  use dlb_common, only: DONE_JOB, NO_WORK_LEFT, RESP_DONE, JLENGTH, L_JOB, JOWNER, JLEFT, JRIGHT
  use dlb_common, only: WORK_DONAT, WORK_REQUEST
  use dlb_common, only: my_rank, n_procs, termination_master, set_start_job, set_empty_job
  use dlb_common, only: dlb_common_setup
  use dlb_common, only: masterserver
  use dlb_common, only: end_communication
  use dlb_common, only: clear_up
  use iso_c_binding
  use dlb_impl_thread_common
  USE_MPI
  implicit none
  !use type_module ! type specification parameters
  save            ! save all variables defined in this module
  private         ! by default, all names are private
  !== Interrupt end of public interface of module =================
  ! Program from outside might want to know the thread-safety-level required form DLB
  integer(kind=i4_kind), parameter, public :: DLB_THREAD_REQUIRED = MPI_THREAD_SERIALIZED

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
  ! these variables are also needed but stored in dlb_impl_thread_common, to avoid cyclic binding:
  !integer(kind=i4_kind), parameter  :: WORK_REQUEST = 4, WORK_DONAT = 5 ! messages for work request
  !integer(kind=i4_kind), parameter  :: JOBS_LEN = JLENGTH  ! Length of complete jobs storage

  ! IDs of mutexes, use base-0 indices:
  !integer(kind=i4_kind), parameter :: LOCK_JS   = 0

  !logical, parameter :: masterserver = .false. ! changes to different variant (master slave concept for comparision)
  !integer(kind=i4_kind)             :: job_storage(jobs_len) ! store all the jobs, belonging to this processor
  !logical                           :: terminated ! for termination algorithm

  ! IDs for condition variables, use base-0 indices:
  integer(kind=i4_kind), parameter :: COND_JS2_UPDATE  = 0

  ! thread IDs, use base-0 indices:
  integer(kind=i4_kind), parameter :: SECRETARY = 0

  logical :: main_waits


  integer(kind=i4_kind)             :: already_done ! stores how many jobs of the current interval have
                                      ! been already calculated, needed for the termination algorithm
                                      ! after initalizing should be only used with LOCK_JS
                                      ! MAIN stores here how many jobs it has already done, SECRETARY uses
                                      ! it for reporting the finished job to their owner

  ! there are two  variables shared between the two threads
  ! terminated is blocked by a global data lock rwlock ("BIG KERNEL LOCK"), only SECRETARY will also write to it
  ! job_storage is blocked by LOCK_JS, there is a condition COND_JS2_UPDATE, which can be used of SECRETARY to tell
  ! MAIN that it is time to check again in job_storage (because new jobs have arrived or all is terminated)
  !
  ! For debugging and counting trace
  ! used only on SECRETARY, but are written out lateron from MAIN (after
  ! SECRETARY has terminated) we do not want concurently prints of different threads, so
  ! secretary must not print them itsself
  integer(kind=i4_kind)             :: count_messages, count_requests, count_offers
  integer(kind=i4_kind)             :: many_tries, many_searches
  integer(kind=i4_kind)             :: many_zeros
  double precision  :: timemax
  ! They are only for MAIN
  double precision  :: main_wait_all, main_wait_max, main_wait_last
  double precision  :: max_work, last_work, average_work, num_jobs
  double precision  :: dlb_time, second_last_work, min_work
  !----------------------------------------------------------------
  !------------ Subroutines ---------------------------------------
contains

  subroutine dlb_init()
    !  Purpose: initalization of needed stuff
    !           is in one thread context
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: OUTPUT_BORDER
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    call dlb_thread_init()
    if (my_rank == 0 .and. 0 < OUTPUT_BORDER) then
        print *, "DLB init: using variant 'thread single'"
        print *, "DLB init: This variant needs an additinal thread for mpi messages"
        print *, "DLB init: This thread is generated with Pthreads"
        print *, "DLB init: Please ensure that MPI has at least MPI_THREAD_SERIALIZED"
    endif
  end subroutine dlb_init

  subroutine dlb_finalize()
    !  Purpose: cleaning up everything, after last call
    !
    ! Context: main thread after joining other.
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
    !  done by the procs should be my_jobs(1) + 1 to jobs(2) in
    !  the related job list
    !  first the jobs are tried to get from the local storage of the
    !  current proc
    !  if there are not enough it will wait for either new ones to arrive
    !  or termination, the return value of my_jobs should only contain
    !  my_jobs(1) == my_jobs(2) if all procs are terminated
    !
    ! Context: main thread.
    !
    use dlb_common, only: empty, OUTPUT_BORDER
    use dlb_common, only: set_empty_job, steal_local, length
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: n
    integer(kind=i4_kind), intent(out  ) :: my_job(:) ! (2)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(i4_kind), target             :: jobs(JLENGTH)
    integer(i4_kind)                     :: remaining(JLENGTH)
    double precision                     :: start_timer
    double precision,save                :: leave_timer = -1
    !------------ Executable code --------------------------------

    ASSERT(size(my_job)==2)

    if (num_jobs > 0) then ! for debugging
        second_last_work = last_work
        last_work = MPI_Wtime() - leave_timer
        if (last_work > max_work) max_work = last_work
        if (last_work < min_work) min_work = last_work
        average_work = average_work + last_work
    endif

    ! in case we do not enter into the DO WHILE loop:
    jobs = set_empty_job()

    !
    ! Modify local storage with this lock only:
    !
    call th_mutex_lock(LOCK_JS)
    do while ( empty(jobs) .and. .not. termination() )
        !
        ! Try getting jobs from local storage:
        !
        if ( steal_local(n, job_storage, remaining, jobs) ) then
            !
            ! Stealing successfull:
            !
            job_storage = remaining
        else
            !
            ! Found no job in this try, now wait for change before doing anything.
            ! CONTROL thread will wake MAIN thread up:
            !
            start_timer = MPI_Wtime() ! for debugging

            main_waits = .true. ! MAIN is waiting, control does not need to sleep

            !
            ! This makes MAIN thread give up on storage lock and go to sleep
            ! waiting to be woken up by CONTROL thread. Lock is aquired again on
            ! waking up:
            !
            call th_cond_wait(COND_JS2_UPDATE, LOCK_JS)

            main_waits = .false. ! MAIN is back again

            !
            ! The rest is time measurements for debugging
            ! FIXME: maybe use FPP_TIMERs instead?
            !
            main_wait_last = MPI_Wtime() - start_timer ! for debugging
            ! store debugging information:
            main_wait_all = main_wait_all + main_wait_last
            if (main_wait_last > main_wait_max) main_wait_max = main_wait_last
            ! end store debugging information
        endif
    enddo

    !
    ! This counter is used for reporting & termination detection:
    ! (used by MAIN and CONTROL threads, should be protected by lock)
    !
    already_done = already_done + length(jobs)

    call th_mutex_unlock(LOCK_JS)

    call time_stamp("finished loop over local search",3)

    ! only the start and endpoint of job slice are needed outside:
    my_job(1) = jobs(JLEFT)
    my_job(2) = jobs(JRIGHT)

    ! here we should have a valid job slice with at least one valid job
    ! or a terminated algorithm
    if ( empty(jobs) ) then
      ! if true means MAIN did not intent to come back (check termination is
      ! too dangerous, because MAIN may still have work for one go and thus
      ! would try to join the thread in the next cycle again
       call th_join_all()
       ! now only one thread left, thus all variables belong him:
       if (1 < OUTPUT_BORDER) then
           dlb_time = MPI_Wtime() - dlb_time ! for debugging
           print *, my_rank, "S: tried", many_searches, "for new jobs andstealing", many_tries
           print *, my_rank, "S: zeros", many_zeros
           write(*, '(I3, " S: longest wait for answer", G20.10)'), my_rank, timemax
           write(*, '(I3, " M: waited (all, max, last)", G20.10, G20.10, G20.10) '), my_rank, &
                  main_wait_all, main_wait_max, main_wait_last
           write(*, '(I3, " M: work slices lasted (average, max, min)", G20.10, G20.10, G20.10)'), my_rank,&
              average_work/ num_jobs, max_work, min_work
           write(*, '(I3, " M: the two of my last work slices lasted", G20.10, G20.10)'), my_rank,&
                  second_last_work, last_work
           write(*, '(I3, " M: time spend in dlb", G20.10, "time processor was working " , G20.10)'), my_rank,&
                         dlb_time, average_work
       endif
    endif

    leave_timer = MPI_Wtime() ! for debugging
    num_jobs = num_jobs + 1 ! for debugging
  end subroutine dlb_give_more

  subroutine thread_secretary() bind(C)
    ! Puropse: This routine should contain all that should be done by
    !          the thread SECRETARY
    !          Thus: mainly check for messages and try to finish the
    !                sended messages (as long as not terminated)
    !                and test if there are still some jobs available
    !                if not send reuqest
    !                as termination will be done by a message send here
    !                there is no fear that SECRETARY will be stuck
    !                as it is a loop
    !
    ! Context: entry to secretary thread.
    !
    !------------ Modules used ------------------- ---------------
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind),allocatable    :: requ_m(:) !requests storages for SECRETARY
    integer(kind=i4_kind)                :: lm_source(n_procs) ! remember which job request
                                               ! I got
    integer(kind=i4_kind)                :: count_ask(n_procs), proc_asked_last ! remember
                                          ! which job request I send (and to whom the last)
    integer(kind=i4_kind)                :: ierr
    double precision                     :: timestart ! just for debugging
    logical :: has_jr_on ! a request for more work is on its way
    logical :: go_sleep ! give more time to main
    !------------ Executable code --------------------------------
    count_messages = 0
    count_offers = 0
    count_requests = 0
    many_tries = 0
    many_searches = 0
    many_zeros = 0
    timemax = 0.0
    has_jr_on = .false.
    count_ask = -1
    lm_source = -1

    ! FIXME: still not clear why:
    ! Check if we have really gotten anything, else report already here that there is nothing as
    ! elsewhere getting to the function test_resp_done with zero jobs done is surpressed
    ! Needed in case that n_procs > Number of jobs
    call test_resp_done(requ_m)

    call task_messages(has_jr_on, requ_m, lm_source,count_ask, proc_asked_last,&
         many_zeros, timestart, timemax)
    if (.not. has_jr_on) call task_local_storage(has_jr_on, requ_m, count_ask, proc_asked_last, timestart)
    do while (.not. termination())
      call th_mutex_lock(LOCK_JS)
      go_sleep = .not. main_waits ! if main has nothing left to do, no need to go to sleep
      call th_mutex_unlock(LOCK_JS)
      if(go_sleep) call c_sleep(1000) !microseconds
      ! check for any message with messagetag dlb
      call task_messages(has_jr_on, requ_m, lm_source, count_ask, proc_asked_last,&
          many_zeros, timestart, timemax)
      call test_requests(requ_m)
      if (.not. has_jr_on) call task_local_storage(has_jr_on, requ_m, count_ask, proc_asked_last, timestart)
    enddo

    ! now finish all messges still available, no matter if they have been received
    call clear_up(count_ask, proc_asked_last, lm_source, requ_m)
    call end_communication()
    call end_threads()
    ! To ensure that no processor has already started with the next dlb iteration
    call MPI_BARRIER(comm_world, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    call th_mutex_lock( LOCK_JS)
    call th_cond_signal(COND_JS2_UPDATE)
    call th_mutex_unlock( LOCK_JS)

    call time_stamp("exitsecretary", 5)

    call th_exit() ! will be joined on MAIN thread
  end subroutine thread_secretary

  !Compiler wants them all  three, even if only one is used
  subroutine thread_mailbox() bind(C)
  end subroutine

  subroutine thread_control() bind(C)
  end subroutine thread_control


  subroutine test_resp_done(requ)
    !  Purpose: tests if the responsibility is done, when n new
    ! jobs are finished
    !
    ! Context: secretary thread.
    !
    !
    ! Locks: wrlock, through check_termination
    !        NEEDS to be in a JS_LOCK context
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: reports_pending
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), allocatable  :: requ(:)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    !------------ Executable code --------------------------------

      if( reports_pending() == 0 ) then ! if all my jobs are done
        if (my_rank == termination_master) then
          call check_termination(my_rank)
        else
          call send_resp_done( requ)
        endif
      endif
  end subroutine test_resp_done

  subroutine task_messages(wait_answer, requ, lm_source, count_ask, proc_asked_last,&
             many_zeros, timestart, timemax)
    !  Purpose: checks if any message has arrived, check_message will
    !           then select the response to the content
    !           if no more messages are found it will finish the task
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: iprobe
    implicit none
    !------------ Declaration of formal parameters ---------------
    logical, intent(inout)               :: wait_answer
    integer(kind=i4_kind),allocatable    :: requ(:)
    integer(kind=i4_kind), intent(inout) :: lm_source(:), proc_asked_last, count_ask(:)
    integer(kind=i4_kind), intent(inout) :: many_zeros! just for debugging
    double precision, intent(inout)      :: timestart, timemax ! just for debugging
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: stat(MPI_STATUS_SIZE)
    integer(i4_kind)                     :: src, tag
    !------------ Executable code --------------------------------

    ! check for any message
    do while ( iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, stat) )

        src = stat(MPI_SOURCE)
        tag = stat(MPI_TAG)

      call time_stamp("got message", 4)
      count_messages = count_messages + 1

        call check_messages(src, tag, requ, wait_answer, lm_source, count_ask, proc_asked_last,&
          many_zeros, timestart, timemax)
    enddo
  end subroutine task_messages

  subroutine task_local_storage(wait_answer, requ, count_ask, proc_asked_last, timestart)
    !  Purpose: checks if there are still jobs in the local storage
    !          if not send a message to another proc asking for jobs
    !          and remember that there is already one on its way
    !          does also the my_resp lowerage of the termination algorithm
    !          as a finished job means, someone has to know it
    use dlb_common, only: empty
    implicit none
    !------------ Declaration of formal parameters ---------------
    logical, intent(out)               :: wait_answer
    integer(kind=i4_kind), intent(inout) :: count_ask(:)
    integer(kind=i4_kind), intent(out) :: proc_asked_last
    double precision, intent(inout)    :: timestart !inout? we do not change it
                                         ! in any case
    integer(kind=i4_kind),allocatable    :: requ(:)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    !------------ Executable code --------------------------------
    wait_answer = .false.
    call th_mutex_lock(LOCK_JS)
    if ( empty(job_storage) ) then
      wait_answer = .true.
      many_searches = many_searches + 1 !global debug tool
      if (.not. masterserver .and. already_done > 0) then
        ! masterserver has different termination algorithm
        ! only report finished jobs, when there are still any
        call report_or_store(job_storage(JOWNER), already_done, requ)
        already_done = 0
      endif
      timestart = MPI_WTIME()
      call send_request(requ, count_ask, proc_asked_last)
    endif
    call th_mutex_unlock(LOCK_JS)
  end subroutine task_local_storage

  subroutine send_request(requ, count_ask, proc_asked_last)
    !  Purpose: send job request to another proc
    !           victim is choosen by the routine select_victim
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: select_victim, isend
    implicit none
    !------------ Declaration of formal parameters ---------------
    !** End of interface *****************************************
    integer(kind=i4_kind),allocatable    :: requ(:)
    integer(kind=i4_kind),intent(inout)     :: count_ask(:)
    integer(kind=i4_kind),intent(out)     :: proc_asked_last
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: v
    integer(kind=i4_kind), save          :: message(JLENGTH) ! is made save to ensure
                                             ! that it is not overwritten before message arrived
                                             ! there is always only one request sended, thus it will
                                             ! be for sure finished, when it is needed for the next request
    integer(kind=i4_kind)                :: requ_wr
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

    call time_stamp("SECRETARY sends message",5)

    ! send along the request-messge counter to identify the last received
    ! message of each proc in the termination phase
    message(:) = 0
    message(1) = count_ask(v+1)

    call isend(message, v, WORK_REQUEST, requ_wr)

    call add_request(requ_wr, requ)
  end subroutine send_request

  subroutine check_messages(src, tag, requ_m, wait_answer, lm_source,count_ask, proc_asked_last,&
            many_zeros, timestart, timemax)
    !  Purpose: checks which message has arrived, checks for messages:
    !          Someone finished stolen job slice
    !          Someone has finished its responsibility (only termination_master)
    !          There are no more jobs (message from termination_master to finish)
    !          Job Request from another proc
    !          Job sended by another proc (after a request was sended)
    !
    ! Context: secretary thread.
    !
    ! Locks:  wrlock
    !        - through divide_jobs(): LOCK_JS, wrlock.
    !
    ! Signals: COND_NJ2_UPDATE
    !
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: report_by, empty
    use dlb_common, only: print_statistics
    use dlb_common, only: recv
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer, intent(in)                  :: src, tag ! source and tag of the pending msg
    integer, allocatable :: requ_m(:)
    logical, intent(inout) :: wait_answer
    integer(kind=i4_kind), intent(inout) :: lm_source(:)
    integer(kind=i4_kind), intent(inout) :: count_ask(:), proc_asked_last
    integer(kind=i4_kind), intent(inout) :: many_zeros! just for debugging
    double precision, intent(inout)      :: timestart, timemax ! just for debugging
    integer(kind=i4_kind)                :: my_jobs(JLENGTH)
    integer(kind=i4_kind)                :: timeend
    !** End of interface *****************************************

    integer(i4_kind) :: message(JLENGTH), stat(MPI_STATUS_SIZE)

    call recv(message, src, tag, stat)

    select case(tag)

    case (DONE_JOB) ! someone finished stolen job slice
      ASSERT(message(1)>0)
      ASSERT(src/=my_rank)

      !
      ! Handle fresh cumulative report:
      !
      call report_by(src, message(1))

      call test_resp_done(requ_m)

    case (RESP_DONE) ! finished responsibility
      ! arrives only on termination master:
      if( my_rank /= termination_master )then
          stop "my_rank /= termination_master"
      endif

      call check_termination(message(1))

    case (NO_WORK_LEFT) ! termination message from termination master
      ASSERT(message(1)==0)
      ASSERT(src==termination_master)

      call print_statistics()
      call wrlock()
      terminated = .true.
      call unlock()
      wait_answer = .true. !No use to send a new request for work right now

    case (WORK_DONAT) ! got work from other proc
      timeend = MPI_WTIME()
      if ((timeend - timestart) > timemax) timemax = timeend - timestart
      count_offers = count_offers + 1
      my_jobs = message(:)
      if ( empty(my_jobs) .and. .not. termination() ) then
        timestart = MPI_WTIME()
        call send_request(requ_m, count_ask, proc_asked_last)
        many_zeros = many_zeros + 1
      else
        wait_answer = .false.
        proc_asked_last = -1  ! set back, which proc to wait for
        call th_mutex_lock( LOCK_JS)
        job_storage(:JLENGTH) = my_jobs
        already_done = 0
        call th_cond_signal(COND_JS2_UPDATE)
        call th_mutex_unlock(LOCK_JS)
      endif

    case (WORK_REQUEST) ! other proc wants something from my jobs
      count_requests = count_requests + 1
      ! store the intern message number, needed for knowing at the end the
      ! status of the last messages on their way
      lm_source(src + 1) = message(1)
      call divide_jobs(src, requ_m)

    case default
      ! This message makes no sense in this context, thus give warning
      ! and continue (maybe the actual calculation has used it)
      print *, "ERROR:", my_rank," got message with unexpected content:", message
      stop "got unexpected message"

    end select
  end subroutine check_messages

  subroutine report_or_store(owner, num_jobs_done, requ)
    !  Purpose: If a job is finished, this cleans up afterwards
    !           Needed for termination algorithm, there are two
    !           cases, it was a job of the own responsibilty or
    !           one from another, first case just change my number
    !            second case, send to victim, how many of his jobs
    !            were finished
    !
    ! Context: secretary thread.
    !
    ! Locks: wrlock, through check_termination
    !
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: report_to
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(i4_kind), intent(in)   :: owner
    integer(i4_kind), intent(in)   :: num_jobs_done
    integer(i4_kind), allocatable  :: requ(:)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    !------------ Executable code --------------------------------
    call time_stamp("finished a job",4)
    ! my_jobs hold recent last point, as proc started from beginning and
    ! steal from the back, this means that from the initial starting point on
    ! (stored in start_job) to this one, all jobs were done
    ! if my_jobs(JRIGHT)/= start_job(JRIGHT) someone has stolen jobs

    !
    ! Make an incremental report:
    !
    call report_to(owner, num_jobs_done)

    if ( owner == my_rank ) then
      call test_resp_done(requ)
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
    ! Starts other Thread, runs on MAIN
    use dlb_common, only: length
    use dlb_impl_thread_common, only: thread_setup, th_create_one
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: job(L_JOB)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: start_job(JLENGTH)
    !------------ Executable code --------------------------------
    dlb_time = MPI_Wtime() ! for debugging
    ! these variables are for the termination algorithm
    terminated = .false.
    main_waits = .false.
    ! For debugging and keeping trace:
    main_wait_all = 0
    main_wait_max = 0
    main_wait_last = 0
    max_work = 0
    min_work = dlb_time
    last_work = 0
    second_last_work = 0
    average_work = 0
    num_jobs = 0
    ! end for debugging
    ! set starting values for the jobs
    start_job = set_start_job(job)
    ! already done should contain how many of the jobs have been done already
    already_done = 0
    call dlb_common_setup(length(start_job)) ! sets none finished on termination_master
    ! Job storage holds all the jobs currently in use
    job_storage(:JLENGTH) = start_job
    ! from now on, there are several threads, so chared objects have to
    ! be locked/unlocked in order to use them!!
    call thread_setup()
    call th_create_one()
    call time_stamp("finished setup", 3)
  end subroutine dlb_setup

end module dlb_impl
