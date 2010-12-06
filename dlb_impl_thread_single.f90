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
  use dlb_common, only: add_request, test_requests, end_requests, send_resp_done, report_job_done
  use dlb_common, only: DONE_JOB, NO_WORK_LEFT, RESP_DONE, JLENGTH, L_JOB, JOWNER, JLEFT, JRIGHT, MSGTAG
  use dlb_common, only: WORK_DONAT, WORK_REQUEST
  use dlb_common, only: my_rank, n_procs, termination_master, set_start_job, set_empty_job
  use dlb_common, only: dlb_common_setup
  use dlb_common, only: masterserver
  use dlb_common, only: decrease_resp
  use dlb_common, only: end_communication
  use dlb_common, only: clear_up
  use iso_c_binding
  use thread_handle
  USE_MPI
  implicit none
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

  !------------ Declaration of types ------------------------------

  !------------ Declaration of constants and variables ----
  ! these variables are also needed but stored in thread_handle, to avoid cyclic binding:
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
  ! SECRETARY has terminated) we don't want concurently prints of different threads, so
  ! secretary mustn't print them itsself
  integer(kind=i4_kind)             :: count_messages, count_requests, count_offers
  integer(kind=i4_kind)             :: my_resp_start, my_resp_self, your_resp
  integer(kind=i4_kind)             :: many_tries, many_searches
  integer(kind=i4_kind)             :: many_zeros
  double precision  :: timemax
  !----------------------------------------------------------------
  !------------ Subroutines ---------------------------------------
contains

  subroutine dlb_init()
    !  Purpose: initalization of needed stuff
    !           is in one thread context
    !------------ Modules used ------------------- ---------------
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    call dlb_thread_init()
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
    !  Purpose: Returns next bunch of up to n jobs, if jobs(JRIGHT)<=
    !  jobs(JLEFT) there are no more jobs there, else returns the jobs
    !  done by the procs should be jobs(JLEFT) + 1 to jobs(JRIGHT) in
    !  the related job list
    !  first the jobs are tried to get from the local storage of the
    !  current proc
    !  if there are not enough it will wait for either new ones to arrive
    !  or termination, the return value of my_jobs should only contain
    !  my_jobs(JLEFT) == my_jobs(JRIGHT) if all procs are terminated
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
    integer(i4_kind), target             :: jobs(JLENGTH)
    !------------ Executable code --------------------------------
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
       call th_cond_wait(COND_JS2_UPDATE, LOCK_JS)
       call local_tgetm(n, jobs)
    enddo
    call th_mutex_unlock(LOCK_JS)
    call time_stamp("finished loop over local search",3)
    ! here we should have a valid job slice with at least one valid job
    ! or a terminated algorithm
    if (jobs(JLEFT) >= jobs(JRIGHT)) then
      ! if true means MAIN did not intent to come back (check termination is
      ! too dangerous, because MAIN may still have work for one go and thus
      ! would try to join the thread in the next cycle again
       call th_join_all()
       ! now only one thread left, thus all variables belong him:
       print *, my_rank, "S: tried", many_searches, "for new jobs andstealing", many_tries
       print *, my_rank, "S: zeros", many_zeros
       print *, my_rank, "S: longest wait for answer", timemax
    endif
    ! only the start and endpoint of job slice is needed external
    my_job = jobs(:L_JOB)
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
    logical :: has_jr_on
    !------------ Executable code --------------------------------
    count_messages = 0
    count_offers = 0
    count_requests = 0
    many_tries = 0
    many_searches = 0
    many_zeros = 0
    my_resp_self = 0
    your_resp = 0
    timemax = 0.0
    has_jr_on = .false.
    count_ask = -1
    lm_source = -1
    call test_resp_done(0, requ_m, my_rank) ! needed if it is initialized with zero jobs for
    ! this proc, the reporing of how many jobs are done for zero jobs is stopped before
    ! it reaches the part were it is tested if the responsibility is done
    call task_messages(has_jr_on, requ_m, lm_source,count_ask, proc_asked_last)
    if (.not. has_jr_on) call task_local_storage(has_jr_on, requ_m, count_ask, proc_asked_last)
    do while (.not. termination())
      call c_sleep(1000) !microseconds
      ! check for any message with messagetag dlb
      call task_messages(has_jr_on, requ_m, lm_source, count_ask, proc_asked_last)
      call test_requests(requ_m)
      if (.not. has_jr_on) call task_local_storage(has_jr_on, requ_m, count_ask, proc_asked_last)
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


  subroutine test_resp_done(n, requ, rank)
    !  Purpose: tests if the responsibility is done, when n new
    ! jobs are finished
    !
    ! Context: secretary thread.
    !
    !
    ! Locks: wrlock, through check_termination
    !        NEEDS to be in a JS_LOCK context
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), allocatable  :: requ(:)
    integer(kind=i4_kind), intent(in)   :: n
    integer(kind=i4_kind), intent(in)   :: rank
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    !------------ Executable code --------------------------------

      if(decrease_resp(n, rank)== 0) then ! if all my jobs are done
        if (my_rank == termination_master) then
          call check_termination(my_rank)
        else
          call send_resp_done( requ)
        endif
      endif
  end subroutine test_resp_done

  subroutine task_messages(wait_answer, requ, lm_source, count_ask, proc_asked_last)
    !  Purpose: checks if any message has arrived, check_message will
    !           then select the response to the content
    !           if no more messages are found it will finish the task
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    logical, intent(inout)               :: wait_answer
    integer(kind=i4_kind),allocatable    :: requ(:)
    integer(kind=i4_kind), intent(inout) :: lm_source(:), proc_asked_last, count_ask(:)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, stat(MPI_STATUS_SIZE)
    logical                              :: flag
    integer(kind=i4_kind)                :: message(1 + JLENGTH)
    !------------ Executable code --------------------------------
    ! check for any message
    call MPI_IPROBE(MPI_ANY_SOURCE, MSGTAG, comm_world,flag, stat, ierr)
    ASSERT(ierr==MPI_SUCCESS)
    do while (flag)!got a message
      call time_stamp("got message", 4)
      count_messages = count_messages + 1
      call MPI_RECV(message, 1+JLENGTH, MPI_INTEGER4, MPI_ANY_SOURCE, MSGTAG, comm_world, stat, ierr)
      !print *, my_rank, "received ",stat(MPI_SOURCE),"'s message", message
      call check_messages(requ, message, stat, wait_answer, lm_source, count_ask, proc_asked_last )
      call MPI_IPROBE(MPI_ANY_SOURCE, MSGTAG, comm_world,flag, stat, ierr)
      ASSERT(ierr==MPI_SUCCESS)
    enddo
  end subroutine task_messages

  subroutine task_local_storage(wait_answer, requ, count_ask, proc_asked_last)
    !  Purpose: checks if there are still jobs in the local storage
    !          if not send a message to another proc asking for jobs
    !          and remember that there is already one on its way
    !          does also the my_resp lowerage of the termination algorithm
    !          as a finished job means, someone has to know it
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    logical, intent(out)               :: wait_answer
    integer(kind=i4_kind), intent(inout) :: count_ask(:)
    integer(kind=i4_kind), intent(out) :: proc_asked_last
    integer(kind=i4_kind),allocatable    :: requ(:)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    !------------ Executable code --------------------------------
    wait_answer = .false.
    call th_mutex_lock(LOCK_JS)
    if (job_storage(JLEFT) >= job_storage(JRIGHT)) then
      wait_answer = .true.
      many_searches = many_searches + 1 !global debug tool
      if (.not. masterserver .and. already_done > 0) then
        ! masterserver has different termination algorithm
        ! only report finished jobs, when there are still any
        call report_or_store(requ,already_done, job_storage(JOWNER))
      endif
      call send_request(requ, count_ask, proc_asked_last)
    endif
    call th_mutex_unlock(LOCK_JS)
  end subroutine task_local_storage

  subroutine send_request(requ, count_ask, proc_asked_last)
    !  Purpose: send job request to another proc
    !           victim is choosen by the routine select_victim
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: select_victim
    implicit none
    !------------ Declaration of formal parameters ---------------
    !** End of interface *****************************************
    integer(kind=i4_kind),allocatable    :: requ(:)
    integer(kind=i4_kind),intent(inout)     :: count_ask(:)
    integer(kind=i4_kind),intent(out)     :: proc_asked_last
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: v, ierr
    integer(kind=i4_kind), save          :: message(1 + JLENGTH) ! is made save to ensure
                                             ! that it is not overwritten before message arrived
                                             ! there is always only one request sended, thus it will
                                             ! be for sure finished, when it is needed for the next request
    integer(kind=i4_kind)                :: requ_wr
    many_tries = many_tries + 1 ! just for debugging
    message = 0
    message(1) = WORK_REQUEST

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

    call time_stamp("SECRETARY sends message",5)
    call MPI_ISEND(message, 1+JLENGTH, MPI_INTEGER4, v, MSGTAG, comm_world, requ_wr, ierr)
    ASSERT(ierr==MPI_SUCCESS)
    call add_request(requ_wr, requ)
  end subroutine send_request

  subroutine check_messages(requ_m, message, stat, wait_answer, lm_source,count_ask, proc_asked_last)
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
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer, allocatable :: requ_m(:)
    logical, intent(inout) :: wait_answer
    integer(kind=i4_kind), intent(inout) :: lm_source(:)
    integer(kind=i4_kind), intent(inout) :: count_ask(:), proc_asked_last
    integer, intent(in) :: message(1 + JLENGTH), stat(MPI_STATUS_SIZE)
    integer(kind=i4_kind)                :: my_jobs(JLENGTH)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    !------------ Executable code --------------------------------
    select case(message(1))

    case (DONE_JOB) ! someone finished stolen job slice
      ASSERT(message(2)>0)

      call test_resp_done(message(2), requ_m, stat(MPI_SOURCE))

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

      call wrlock()
      terminated = .true.
      call unlock()
      wait_answer = .true. !No use to send a new request for work right now

    case (WORK_DONAT) ! got work from other proc
      count_offers = count_offers + 1
      my_jobs = message(2:)
      if ((my_jobs(JLEFT) >= my_jobs(JRIGHT)) .and. .not. termination()) then
        call send_request(requ_m, count_ask, proc_asked_last)
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
      lm_source(stat(MPI_SOURCE)+1) = message(2)
      call divide_jobs(stat(MPI_SOURCE), requ_m)

    case default
      ! This message makes no sense in this context, thus give warning
      ! and continue (maybe the actual calculation has used it)
      print *, "ERROR:", my_rank," got message with unexpected content:", message
      stop "got unexpected message"

    end select
  end subroutine check_messages

  subroutine report_or_store(requ, num_jobs_done, rank)
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
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), allocatable  :: requ(:)
    integer(kind=i4_kind), intent(inout):: num_jobs_done
    integer(kind=i4_kind), intent(in)   :: rank
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    !------------ Executable code --------------------------------
    call time_stamp("finished a job",4)
    ! my_jobs hold recent last point, as proc started from beginning and
    ! steal from the back, this means that from the initial starting point on
    ! (stored in start_job) to this one, all jobs were done
    ! if my_jobs(JRIGHT)/= start_job(JRIGHT) someone has stolen jobs
    if (rank == my_rank) then
      my_resp_self = my_resp_self + num_jobs_done
      call test_resp_done(num_jobs_done, requ, my_rank)
    else
      your_resp = your_resp + num_jobs_done
      ! As all isends have to be closed sometimes, storage of
      ! the request handlers is needed
      call report_job_done(num_jobs_done, rank)
    endif
    num_jobs_done = 0
  end subroutine report_or_store


   subroutine local_tgetm(m, my_jobs)
    !  Purpose: takes m jobs from the left from object job_storage
    !           in the first try there is no need to wait for
    !           something going on in the jobs
    !
    ! Context: MAIN thread.
    !
    ! Locks: complete function is locked by LOCK_JS from outside
    !
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: reserve_workm, split_at
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: m
    integer(kind=i4_kind), intent(out  ) :: my_jobs(JLENGTH)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: w, sp
    integer(i4_kind)              :: remaining(JLENGTH)
    !------------ Executable code --------------------------------
    w = reserve_workm(m, job_storage) ! how many jobs to get
    sp = job_storage(JLEFT) + w
    call split_at(sp, job_storage, my_jobs, remaining)
    job_storage = remaining
    already_done = already_done + w
  end subroutine local_tgetm

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
    !------------ Modules used ------------------- ---------------
    use thread_handle, only: thread_setup, th_create_one
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: job(L_JOB)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: start_job(JLENGTH)
    !------------ Executable code --------------------------------
    ! these variables are for the termination algorithm
    terminated = .false.
    ! set starting values for the jobs
    start_job = set_start_job(job)
    ! already done should contain how many of the jobs have been done already
    already_done = 0
    call dlb_common_setup(start_job(JRIGHT) - start_job(JLEFT)) ! sets none finished on termination_master
    ! needed for termination
    my_resp_start = start_job(JRIGHT) - start_job(JLEFT)
    ! Job storage holds all the jobs currently in use
    job_storage(:JLENGTH) = start_job
    ! from now on, there are several threads, so chared objects have to
    ! be locked/unlocked in order to use them!!
    call thread_setup()
    call th_create_one()
    call time_stamp("finished setup", 3)
  end subroutine dlb_setup

end module dlb_impl
