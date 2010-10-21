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
  !          another possibility for the algorithm: with setting master_server = true another the algorithm is
  !                                                 algorithm is sliglty changed, in this case only one (the
  !                                                 termination_master) is  allowed to be asked for new jobs
  !                                                 he would not give back half of them of course but only a
  !                                                 fraction, several different algorithm for it can be choosen
  !                                                 by chunk_m, thy are mainly like the algorithm provided by
  !                                                 openMP parloop (chunk_m =1 gives a fixed amount of work, respectively
  !                                                 chunksize * m back, chunk_m = 2 does the guided variant with
  !                                                 giving back /n_procs of what is still there (as long as it is
  !                                                 more than m), m is the amount of jobs wanted at once
  !                                                 termination here is much easier: if termination_master can not
  !                                                 send any jobs back, he sends termination to the proc, he collects
  !                                                 all the procs he has send a termination to, if he has all and his
  !                                                 own CONTROL has send him a job request he terminates also
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
  !#include 'thread_wrapper.h'
  !use mpi
  use iso_c_binding
  implicit none
  include 'mpif.h'
  !use type_module ! type specification parameters
  save            ! save all variables defined in this module
  private         ! by default, all names are private
  !== Interrupt end of public interface of module =================

  !------------ public functions and subroutines ------------------
  public dlb_init, dlb_finalize, dlb_setup, dlb_give_more !for using the module
  public thread_control1, thread_mailbox1 ! needed for the c-wrapper on the pthreads
  public :: time_stamp
  !================================================================
  ! End of public interface of module
  !================================================================

  !------------ Declaration of types ------------------------------
! ONLY FOR DEBBUGING WITHOUT PARAGAUSS
! integer with 4 bytes, range 9 decimal digits
integer, parameter :: integer_kind = selected_int_kind(9)
integer, parameter :: i4_kind = integer_kind
! real with 8 bytes, precision 15 decimal digits
integer, parameter :: double_precision_kind = selected_real_kind(15)
integer, parameter :: r8_kind = double_precision_kind
integer, parameter :: comm_world = MPI_COMM_WORLD


! END ONLY FOR DEBUGGING

  ! first three variables for the use with the other form
  logical, parameter  :: master_server = .false.
  integer(kind=i4_kind), parameter :: chunk_m = 2
  integer(kind=i4_kind)             :: chunksize !how many jobs per slice
  !------------ Declaration of constants and variables ----
  integer(kind=i4_kind), parameter  :: DONE_JOB = 1, NO_WORK_LEFT = 2, RESP_DONE = 3 !for distingishuing the messages
  integer(kind=i4_kind), parameter  :: WORK_REQUEST = 4, WORK_DONAT = 5 ! messages for work request
  integer(kind=i4_kind), parameter  :: SJOB_LEN = 3 ! Length of a single job in interface
  integer(kind=i4_kind), parameter  :: L_JOB = 2  ! Length of job to give back from interface
  integer(kind=i4_kind), parameter  :: NRANK = 3 ! Number in job, where rank of origin proc is stored
  integer(kind=i4_kind), parameter  :: J_STP = 1 ! Number in job, where stp (start point) is stored
  integer(kind=i4_kind), parameter  :: J_EP = 2 ! Number in job, where ep (end point) is stored
  integer(kind=i4_kind), parameter  :: JOBS_LEN = SJOB_LEN  ! Length of complete jobs storage
  integer(kind=i4_kind), parameter  :: LOCK_JS = 1 , LOCK_NJ = 2, LOCK_MR = 3, LOCK_TERM = 4
                     !locks names for the threads (via mutex)
  integer(kind=i4_kind), parameter   :: COND_JS_UPDATE = 1, COND_NJ_UPDATE = 2 ! condition numbers conversation
  integer(kind=i4_kind), parameter   :: COND_JS2_UPDATE = 3 ! condition numbers conversation
  integer(kind=i4_kind), parameter  ::  MAILBOX = 1, CONTROL = 2 ! for the new seperate threads
  integer(kind=i4_kind), parameter  ::  MSGTAG = 166 ! message tag for all MPI communication

  integer(kind=i4_kind)            :: my_rank, n_procs ! some synonyms

  integer(kind=i4_kind)             ::  termination_master ! the one who gathers the finished my_resp's
                                         ! and who tells all, when it is complety finished
                                         ! in case of the variant with master as server of jobs, its also the master

  integer(kind=i4_kind)             :: job_storage(jobs_len) ! store all the jobs, belonging to this processor
  integer(kind=i4_kind)             :: new_jobs(SJOB_LEN) ! stores new job, just arrived from other proc
  integer(kind=i4_kind)             :: start_job(SJOB_LEN) ! job_storage is changed a lot, backup for
                                     ! finding out, if someone has stolen something, or how many jobs one
                                     ! has done, when job_storage hold no more jobs
  integer(kind=i4_kind)             :: my_resp ! number of jobs, this processor is responsible for (given
                                              ! in setup
  integer(kind=i4_kind)             :: store_m !keep m for the other threads
  logical                           :: terminated ! for termination algorithm
  integer(kind=i4_kind),allocatable :: requ_m(:), requ_c(:) !requests storages for MAILBOX and CONTROL
  logical, allocatable              :: all_done(:) ! only allocated on termination_master, stores which proc's
                                                   ! jobs are finished: ATTENTION: NOT which proc is finished only if
                                                   ! with master_server
  logical                           :: i_am_waiting ! Thead 0 (main thread) is waiting for CONTROL

  logical                           :: control_waiting ! Thead CONTROL is waiting for change in job_storage !FIXME: it is allowed to
  logical                           :: job_waiting ! Thead CONTROL is waiting for change in new_jobs        ! signal a condition where
                                                                                                            ! noone waits?

  ! some variables are chared between the three threads, there are also locks and conditions to obtain
  ! following a list, which of the three threads (MAIN, CONTROL, MAILBOX) does read or write on them
  ! job_storage: read: ALL             locked by LOCK_JS
  !             write: ALL
  !             initialization (before start of the other threads) is without lock!
  ! my_resp: read: CONTROL, MAILBOX    locked by LOCK_MR
  !          write: CONTROL, MAILBOX
  ! terminated: read: ALL              locked by LOCK_TERM
  !             write: CONTROL, MAILBOX
  ! new_jobs:   read: CONTROL          locked by LOCK_NJ
  !             write: CONTROL, MAILBOX
  ! i_am_waiting: read: CONTROL        also locked by LOCK_JS
  !               write: CONTROL, MAIN
  ! store_m:  read: MAILBOX, CONTROL    also locked by LOCK_JS
  !           write: MAIN
!!! control_waiting: read: MAIN, MAILBOX  also locked by LOCK_JS
!!!                  write: ALL
!!! job_waiting:   read: MAILBOX        also locked by LOCK_NJ
!!!                write: CONTROL, MAILBOX
  ! termination_master, my_rank, n_procs and the paramater: they will be only read, no change to them
  !                  thus it should be save not to lock them???
  !
  ! the follwing conditions are used to tell:
  ! COND_JS_UPDATE: CONTROL waits here (if there have been enough jobs in the last entry)
  !                 for the other to took some jobs out of job_storage
  !!!              CONTROL tells by control_waiting that it waits at this border
  ! COND_JS2_UPDATE: if MAIN finds no more jobs in job_storage, it waits here if CONTROL gets
  !                any, CONTROL should not be waiting at that time but busily searching
  !                for more jobs
  !                MAIN tells by i_am_waiting that it has run out of jobs (and got no termination)
  ! COND_NJ_UPDATE: CONTROL waits for answer to a job request, got answer by MAILBOX
  !!!               CONTROL tells by job_waiting of it, this is only needed at termination, as
  !!!               else MAILBOX can assume that CONTROL is waiting if it gets an job offer
  !
  ! For debugging and counting trace
  integer(kind=i4_kind)             :: count_messages, count_requests, count_offers ! how many messages arrived
  integer(kind=i4_kind)             :: my_resp_start, my_resp_self, your_resp !how many jobs doen where
  integer(kind=i4_kind)             :: many_tries, many_searches !how many times asked for jobs
  double precision :: time_offset = -1.0
  !----------------------------------------------------------------
  !------------ Subroutines ---------------------------------------
contains

! ONLY FOR DEBBUGING (WITHOUT PARAGAUSS)
  subroutine show()
    implicit none

    call show1(job_storage)
  end subroutine show
  subroutine timeloc(place,num )
    implicit none
    character(len=*), intent(in) :: place
    integer, optional :: num
    if (present(num)) then
      print *,"GGG", my_rank, "local", place,num,  MPI_Wtime()
    else
      print *,"GGG", my_rank, "local", place,  MPI_Wtime()
    endif
  end subroutine timeloc
  subroutine timepar(place )
    implicit none
    character(len=*), intent(in) :: place

    print *,"GGG", my_rank, "parallel", place,  MPI_Wtime()
  end subroutine timepar

  function time_stamp_prefix(time) result(prefix)
    implicit none
    double precision, intent(in) :: time
    character(len=28) :: prefix
    ! *** end of interface ***
    write(prefix, '(A,"#", I3, G20.10)')"GGG"  , my_rank, time - time_offset
  end function time_stamp_prefix

  subroutine time_stamp(msg)
    implicit none
    character(len=*), intent(in) :: msg
    ! *** end of interface ***

    double precision :: time

    time = MPI_Wtime()
    if ( time_offset < 0.0 ) then
      time_offset = 0.0
      print *, time_stamp_prefix(time), "(BASE TIME)"
      time_offset = time
    endif

    print *, time_stamp_prefix(time), msg
  end subroutine time_stamp

  subroutine show1(storage)
    implicit none
    integer(i4_kind), intent(in) :: storage(:)

    print *, "storage=", storage
  end subroutine show1

  subroutine assert_n(exp,num)
    implicit none
    logical, intent(in) :: exp
    integer, intent(in) :: num
    if (.not. exp) then
       print *, "ASSERT FAILED", num
       call abort
    endif
  end subroutine assert_n

! END ONLY FOR DEBUGGING

  !*************************************************************
  subroutine dlb_init()
    !  Purpose: initalization of needed stuff
    !------------ Modules used ------------------- ---------------
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
    termination_master = n_procs - 1
    if (my_rank == termination_master) then
      allocate(all_done(n_procs), stat = alloc_stat)
      !ASSERT(alloc_stat==0)
      call assert_n(alloc_stat==0, 1)
    endif
    call th_inits()
    chunksize = 10  ! needed if master_server = .true.
  end subroutine dlb_init
  !*************************************************************
  subroutine dlb_finalize()
    !  Purpose: cleaning up everything, after last call
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
  !*************************************************************
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
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: n
    integer(kind=i4_kind), intent(out  ) :: my_job(L_JOB)
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, v
    integer(i4_kind), target             :: jobs(SJOB_LEN)
!   !------------ Executable code --------------------------------
    ! First try to get a job from local storage
    call local_tgetm(n, jobs, .true.)
    !call timeloc("finished")
    !print *, my_rank, "Finished local search"
    ! if local storage only gives empty job: cycle under checking for termination
    ! only try new job from local_storage after J_STP told that there are any
    do while (jobs(J_STP) >= jobs(J_EP) .and. .not. termination())
       call local_tgetm(n, jobs, .false.)
    enddo
    ! here we should have a valid job slice with at least one valid job
    ! or a terminated algorithm
    print *, my_rank,"GOT JOB: its", jobs
    ! only the start and endpoint of job slice is needed external
    my_job = jobs(:L_JOB)
  end subroutine dlb_give_more
  !*************************************************************
  logical function termination()
    !Purpose: make lock around terminated, but have it as one function
    call th_mutex_lock(LOCK_TERM)
      termination = terminated
    call th_mutex_unlock(lOCK_TERM)
  end function termination
  !*************************************************************
  subroutine thread_mailbox1()
    ! Puropse: This routine should contain all that should be done by
    !          the thread MAILBOX
    !          Thus: mainly check for messages and try to finish the
    !                sended messages (as long as not terminated)
    !                as termination will be done by a message send here
    !                there is no fear that MAILBOX will be stuck while
    !                all others already finished
    !               the clean up of the finished messges is also contained
    !------------ Modules used ------------------- ---------------
    implicit none
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: j,i,req, stat(MPI_STATUS_SIZE), len_req, len_new
    integer(kind=i4_kind)                :: alloc_stat, ierr
    integer(kind=i4_kind),allocatable :: requ_int(:)
    logical     :: flag
    logical, allocatable :: finished(:)
!   !------------ Executable code --------------------------------
    do while (.not. termination())
      call check_messages()
      call test_requests(requ_m)
    enddo
    print *,my_rank, "MAILBOX: got ", count_messages, "messages with", count_requests, "requests and", count_offers, "offers"
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
    ! here he checks if CONTROL is waiting somewhere and if yes
    ! wakes it up, so that it can find out about the termination
    ! CONTROL should then wake up MAIN if neccessary
    call th_mutex_lock( LOCK_NJ)
    print *, my_rank, "prepare termination, check for CONTROL"
    !print *, my_rank, "job_waiting=", job_waiting
    if (job_waiting) call th_cond_signal(COND_NJ_UPDATE)
    call th_cond_signal(COND_NJ_UPDATE)
    call th_mutex_unlock( LOCK_NJ)
    call th_mutex_lock( LOCK_JS)
    !print *, my_rank, "control_waiting=", control_waiting
    !if (control_waiting) call th_cond_signal(COND_JS_UPDATE)
    call th_cond_signal(COND_JS_UPDATE)
    call th_mutex_unlock( LOCK_JS)
    print *, my_rank, "exit thread MAILBOX"
    call timepar("exitmailbox")
    call th_exit()
  end subroutine thread_mailbox1
  !*************************************************************
  subroutine test_requests(requ)
    ! Purpose: tests if any of the messages stored in requ have been
    !          received, than remove the corresponding request
    !------------ Modules used ------------------- ---------------
    implicit none
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer, allocatable :: requ(:)
    integer(kind=i4_kind)                :: j,i,req, stat(MPI_STATUS_SIZE), len_req, len_new
    integer(kind=i4_kind)                :: alloc_stat, ierr
    integer(kind=i4_kind),allocatable :: requ_int(:)
    logical     :: flag
    logical, allocatable :: finished(:)
!   !------------ Executable code --------------------------------
    if (allocated(requ)) then
      len_req = size(requ,1)
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
    endif
  end subroutine test_requests
  !*************************************************************
  subroutine thread_control1()
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
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: i, v, ierr, stat(MPI_STATUS_SIZE), alloc_stat, req
    integer(kind=i4_kind)                :: message(1 + SJOB_LEN), requ_wr
    integer(kind=i4_kind)                :: my_jobs(SJOB_LEN), kind_send, world, len1
    logical                              :: first
!   !------------ Executable code --------------------------------
    many_tries = 0
    many_searches = 0
    my_resp_self = 0
    your_resp = 0
    ! message is always the same (WORK_REQUEST)
    message = 0
    message(1) = WORK_REQUEST
    first = .true.

    ! first lock
    call th_mutex_lock(LOCK_JS)
    do while (.not. termination()) ! while active
      !print *, my_rank, "CONTROL before wait job_storage=", job_storage
      if (.not. i_am_waiting) then
        print *, my_rank, "CONTROL: control_waiting=", control_waiting, i_am_waiting
        !call timepar("Cwait")
      ! if (first) then
      !   first = .false.
      ! else
        control_waiting = .true.
        call th_cond_wait(COND_JS_UPDATE, LOCK_JS)  ! unlockes LOCK_JS while waiting for change in it
        control_waiting = .false.
        endif
        !call timepar("Cwoke")
        !print *, my_rank, "CONTROL finshed waiting for cond"
      !endif
      print *, my_rank, "CONTROL job_storage=", job_storage

      ! verify that CONTROL woke up correctly
      if (job_storage(J_STP) >= job_storage(J_EP)) then !point where CONTROL actually has something to do
        call timepar("controlworking")
        !print *, my_rank, "CONTROL find new jobs"
        many_searches = many_searches + 1
        if (.not. master_server) then
          if (report_or_store(job_storage(:SJOB_LEN), req)) call add_request(req, requ_c)
        endif
        my_jobs = job_storage(:SJOB_LEN)
        call th_mutex_unlock(LOCK_JS)

        call th_mutex_lock(LOCK_NJ) ! LOCKED NJ but unlocked LOCK_JS
        do while (.not. termination() .and. (my_jobs(J_STP) >= my_jobs(J_EP))) ! two points to stop: if there are again new jobs
                                                                               ! or if all is finished
          many_tries = many_tries + 1
          v = select_victim()
          print *, my_rank, "CONTROL send message to", v
          call timeloc("sendmessage",v)
          call MPI_ISEND(message, 1+SJOB_LEN, MPI_INTEGER4, v, MSGTAG, comm_world, requ_wr, ierr)
          !ASSERT(ierr==MPI_SUCCESS)
          call assert_n(ierr==MPI_SUCCESS, 4)
          call test_requests(requ_c)
          !print *,my_rank, "CONTROL: job_waiting=", job_waiting
          !call timepar("waitrep")
          job_waiting = .true.
          call th_cond_wait(COND_NJ_UPDATE, LOCK_NJ) !while waiting is unlocked for MAILBOX
          job_waiting = .false.
          !call timepar("gotrep")
            my_jobs = new_jobs
            new_jobs(J_STP) = 0
            new_jobs(J_EP) = 0
          print *, my_rank, "CONTROL got back", my_jobs
          ! at this point message should have been arrived (there is an answer back!)
          ! but it may also be that the answer message is from termination master
          ! thus cancel to get sure, that there is no dead lock in the MPI_WAIT
          call MPI_CANCEL(requ_wr, ierr)
          !ASSERT(ierr==MPI_SUCCESS)
          call assert_n(ierr==MPI_SUCCESS, 4)
          call MPI_WAIT(requ_wr, stat, ierr)
          !ASSERT(ierr==MPI_SUCCESS)
          call assert_n(ierr==MPI_SUCCESS, 4)
        enddo
        call th_mutex_unlock(LOCK_NJ) ! unlock LOCK_NJ

        call th_mutex_lock(LOCK_JS)
        job_storage(:SJOB_LEN) = my_jobs
        start_job = my_jobs

        if (i_am_waiting) then
          ! tell MAIN that there is something in the storage
          ! as it will wait at cond_js2_update if run out if jobs (and not terminated)
          print *, my_rank, "CONTROL: wake up MAIN, there are new jobs"
          i_am_waiting = .false.
          call th_cond_signal(COND_JS2_UPDATE)
         !call th_mutex_unlock(LOCK_JS)
         !call th_mutex_lock(LOCK_JS)
        endif
      endif
      print *, my_rank,"CONTROL: finished getting new jobs"
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
    ! make sure nobody (MAILBOX) thinks that CONTROL is waiting somewhere
    call th_mutex_lock(LOCK_JS)
    if (i_am_waiting) call th_cond_signal(COND_JS2_UPDATE) ! and free MAIN if waiting
    control_waiting = .false.
    call th_mutex_unlock(LOCK_JS)
   call th_mutex_lock(LOCK_NJ)
    job_waiting = .false.
   call th_mutex_unlock(LOCK_NJ)
    print *, my_rank, "CONTROL: tried", many_searches, "times to get new jobs, by trying to steal", many_tries
    print *, my_rank, "CONTROL: done", my_resp_self, "of my own, gave away", my_resp_start - my_resp_self, "and stole", your_resp
    print *, my_rank, "exit thread CONTROL"
    call timepar("exitcontrol")
    call th_exit()
  end subroutine thread_control1
  !*************************************************************
  subroutine check_messages()
    !  Purpose: checks if any message has arrived, checks for messages:
    !          Someone finished stolen job slice
    !          Someone has finished its responsibilty (only termination_master)
    !          There are no more jobs (message from termination_master to finish)
    !          Job Request from another proc
    !          Job sended by another proc (after CONTROL sended a request)
    !          Except the last one, they are all handled only by this thread
    !          only after got job, put it in intermediate storage and wake CONTROL
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, stat(MPI_STATUS_SIZE), alloc_stat, req
    integer(kind=i4_kind)                :: message(1 + SJOB_LEN)
    logical                              :: if_new_req
!   !------------ Executable code --------------------------------
    ! check and wait for any message with messagetag dlb
    call MPI_RECV(message, 1+SJOB_LEN, MPI_INTEGER4, MPI_ANY_SOURCE, MSGTAG, comm_world, stat, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, my_rank)
    print *, my_rank, "got message", message, "from", stat(MPI_SOURCE)
    call timeloc("gotmessage",stat(MPI_SOURCE))
    count_messages = count_messages + 1
    if (message(1) == DONE_JOB) then ! someone finished stolen job slice
       !ASSERT(message>0)
       call assert_n(message(2)>0, 4)
       call th_mutex_lock(LOCK_MR)
       my_resp = my_resp - message(2)
       if (is_my_resp_done(MAILBOX, req)) call add_request(req, requ_m)
       call th_mutex_unlock(LOCK_MR)
    elseif (message(1) == RESP_DONE) then ! finished responsibility
       if (my_rank == termination_master) then
         call check_termination(message(2), MAILBOX)
       else ! this should not happen
         print *, "WARNING: got unexpected message (I'm not termination master",my_rank,"):", message
         !ASSERT(.false.)
         call assert_n(.false.,19)
       endif
    elseif (message(1) == NO_WORK_LEFT) then ! termination message from termination master
       !ASSERT(message(2)==0)
       call assert_n(message(2)==0, 4)
       !ASSERT(stat(MPI_SOURCE)==termination_master)
       call assert_n(stat(MPI_SOURCE)==termination_master, 4)
       call th_mutex_lock(LOCK_TERM)
       terminated = .true.
       call th_mutex_unlock(LOCK_TERM)
    elseif (message(1) == WORK_DONAT) then ! got work from other proc
       call th_mutex_lock( LOCK_NJ)
         count_offers = count_offers + 1
         new_jobs = message(2:)
    !print *, my_rank, "job_waiting=", job_waiting
         !job_waiting = .false.
         call th_cond_signal(COND_NJ_UPDATE)
       call th_mutex_unlock( LOCK_NJ)  
    elseif (message(1) == WORK_REQUEST) then ! other proc wants something from my jobs
       count_requests = count_requests + 1
       !call timeloc("jobdiv1")
       if (divide_jobs(stat(MPI_SOURCE), req)) call add_request(req, requ_m)
       !call timeloc("jobdiv2")
    else
      ! This message makes no sense in this context, thus give warning
      ! and continue (maybe the actual calculation has used it)
      print *, "WARNING:", my_rank," got message with unexpected content:", message
    endif
  end subroutine check_messages
  !************************************************************
  subroutine add_request(req, requ)
    !Purpose: stores unfinished requests
    integer, intent(in) :: req
    integer, allocatable :: requ(:)
    integer, allocatable :: req_int(:)
    integer :: alloc_stat, len_req
    len_req = 0
    if (allocated(requ)) then
      len_req = size(requ_m,1)
      allocate(req_int(len_req), stat = alloc_stat)
      !ASSERT(alloc_stat==0)
      call assert_n(alloc_stat==0, 4)
      req_int = requ
      deallocate(requ_m, stat = alloc_stat)
      !ASSERT(alloc_stat==0)
      call assert_n(alloc_stat==0, 4)
    endif
    allocate(requ(len_req +1), stat = alloc_stat)
    !ASSERT(alloc_stat==0)
    call assert_n(alloc_stat==0, 4)
    if (len_req > 0) requ(:len_req) = req_int
    requ(len_req +1) = req
  end subroutine add_request
  !************************************************************
  subroutine check_termination(proc, thread)
    !  Purpose: only on termination_master, checks if all procs
    !           have reported termination
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in)    :: proc, thread
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, i, alloc_stat, req_self, stat(MPI_STATUS_SIZE)
    logical                              :: finished
    integer(kind=i4_kind), allocatable   :: request(:), stats(:,:)
    integer(kind=i4_kind)                :: receiver, message(1+SJOB_LEN)
!   !------------ Executable code --------------------------------
    ! all_done stores the procs, which have already my_resp = 0
    all_done(proc+1) = .true.
    ! check if there are still some procs not finished
    print *, my_rank, "CHECK termination", all_done
    finished = .true.
    do i = 1, n_procs
      if (.not. all_done(i)) finished = .false.
    enddo

    ! there will be only a send to the other procs, telling them to terminate
    ! thus the termination_master sets its termination here
    if (finished) then
      call th_mutex_lock(LOCK_TERM)
      terminated = .true.
      call th_mutex_unlock(LOCK_TERM)
      if  (n_procs > 1 .and. (.not. master_server)) then
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
          call timeloc("term", receiver)
          call MPI_ISEND(message, 1+SJOB_LEN, MPI_INTEGER4, receiver, MSGTAG,comm_world ,request(i+1), ierr)
          !ASSERT(ierr==MPI_SUCCESS)
          call assert_n(ierr==MPI_SUCCESS, 4)
          print *, "Send termination to", receiver
        enddo
        call MPI_WAITALL(size(request), request, stats, ierr)
        !ASSERT(ierr==MPI_SUCCESS)
        call assert_n(ierr==MPI_SUCCESS, 4)
         if (thread == CONTROL) then ! in this (seldom) case shut also down my own mailbox (should be
                ! waiting for any message
          print *, "Send termination to myself", termination_master
          call timeloc("term", termination_master)
           call MPI_ISEND(message, 1+SJOB_LEN, MPI_INTEGER4, termination_master, MSGTAG,comm_world ,req_self, ierr)
           !ASSERT(ierr==MPI_SUCCESS)
           call assert_n(ierr==MPI_SUCCESS, 4)
           call MPI_WAIT(req_self, stat, ierr)
           !ASSERT(ierr==MPI_SUCCESS)
           call assert_n(ierr==MPI_SUCCESS, 4)
         endif
      endif
    endif
  end subroutine check_termination
  !************************************************************
  logical function is_my_resp_done(thread, requ)
    !  Purpose: my_resp holds the number of jobs, assigned at the start
    !           to this proc, so this proc is responsible that they will
    !           be finished, if my_resp == 0, I should tell termination_master
    !           that, termination master will collect all the finished resp's
    !           untill it gots all of them back
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
!   !** End of interface *****************************************
    integer(kind=i4_kind), intent(out)   :: requ
    integer(kind=i4_kind), intent(in)   :: thread
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr
    logical                              :: flag
    integer(kind=i4_kind)                :: message(1+SJOB_LEN)
!   !------------ Executable code --------------------------------
    print *,my_rank, "Left of my responsibility:", my_resp
    is_my_resp_done = .false.
    if (my_resp == 0) then
      if (my_rank == termination_master) then
        call check_termination(my_rank,thread)
      else
        message(1) = RESP_DONE
        message(2) = my_rank
        message(3:) = 0
        call MPI_ISEND(message, 1+SJOB_LEN, MPI_INTEGER4, termination_master, MSGTAG,comm_world, requ, ierr)
        !ASSERT(ierr==MPI_SUCCESS)
        call assert_n(ierr==MPI_SUCCESS, 4)
        is_my_resp_done = .true.
      endif
    endif
  end function is_my_resp_done
  !************************************************************
  integer(kind=i4_kind) function select_victim()
     ! Purpose: decide of who to try next to get any jobs
     if (master_server) then
       select_victim = termination_master
     else
       select_victim = select_victim_random2()
       !select_victim = select_victim_r()
     endif
  end function select_victim
  !*************************************************************
  integer(kind=i4_kind) function select_victim_r()
     ! Purpose: decide of who to try next to get any jobs
     ! Each in a row
     integer(kind=i4_kind), save :: count = 1
     select_victim_r = mod(my_rank + count, n_procs)
     count = count + 1
     if (select_victim_r == my_rank) then
        select_victim_r = mod(my_rank + count, n_procs)
        count = count + 1
     endif
     !print *, "RRRRRRRRRRRRRR selcet victim round", select_victim_r, my_rank
  end function select_victim_r
  !*************************************************************
  integer(kind=i4_kind) function select_victim_random2()
     ! Purpose: decide of who to try next to get any jobs
     ! Uses primitive pseudorandom code X_n+1 = (aX_n + b)mod m
     !integer, save, dimension(1) :: random
     integer, allocatable, save :: random(:)
     real(kind=r8_kind), allocatable     ::  harv(:)
     integer(kind=i4_kind)  :: i, s, alloc_stat
     !f (random(1)==-1) random(1) = my_rank
     call random_seed()
     call random_seed(size=s)
     if (.not. allocated(random)) then
       allocate(random(s), stat=alloc_stat)
       !ASSERT(alloc_stat==0)
       call assert_n(alloc_stat==0, 4)
       call random_seed(get=random)
       do i = 1, s
          random(i) = my_rank + i
       enddo
       !print *, my_rank, s, random
     endif
     allocate(harv(s), stat=alloc_stat)
     !ASSERT(alloc_stat==0)
     !all assert_n(alloc_stat==0, 4)

     !print *,my_rank, "random=", random
     call random_seed(put=random)
     call random_number(harv)
     !print *,my_rank, "harv=",harv
     select_victim_random2 = int(harv(1)*n_procs)
     i = 0
     do while (select_victim_random2 == my_rank .and. i < 10)
       call random_number(harv)
       select_victim_random2 = int(harv(1)*n_procs)
       i = i + 1
     enddo
     if ((select_victim_random2 == n_procs) .or. (select_victim_random2 &
        ==my_rank)) select_victim_random2 = select_victim_r()
     call random_number(harv)
     call random_seed(get=random)
     !print *,my_rank, "random=", random
     !print *, "RRRRRRRRRRR random victim", select_victim_random2, my_rank
  end function select_victim_random2
  !*************************************************************
  subroutine local_tgetm(m, my_jobs, first)
    !  Purpose: takes m jobs from the left from object job_storage
    !           in the first try there is no need to wait for
    !           something going on in the jobs
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: m
    logical, intent(in)                  :: first
    integer(kind=i4_kind), intent(out  ) :: my_jobs(SJOB_LEN)
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, sap, w
!   !------------ Executable code --------------------------------
    call th_mutex_lock(LOCK_JS)
    if (.not. first) then
      ! found no job in first try, now wait for change before doing anything
      ! CONTROL will make a wake up
      if(control_waiting) then
        call th_cond_signal(COND_JS_UPDATE)
        control_waiting = .false.
      endif
      !call timepar("wakeC")
      i_am_waiting = .true.
      call th_cond_wait(COND_JS2_UPDATE, LOCK_JS)
      i_am_waiting = .false.
    endif
    w = reserve_workm(m, job_storage) ! how many jobs to get
    my_jobs = job_storage(:SJOB_LEN) ! first SJOB_LEN hold the job
    my_jobs(J_EP)  = my_jobs(J_STP) + w
    job_storage(J_STP) = my_jobs(J_EP)
!   print *, "my_jobs=", my_jobs
!   print *, "job_storage=", job_storage
!   print *, "local_tgetm: end locked", my_rank
    store_m = m
    !print *, my_rank,"control_waiting(MAIN)=", control_waiting, i_am_waiting
    if (job_storage(J_STP) >= job_storage(J_EP) .and. control_waiting) then
      !call timepar("wakeC2")
      print *, my_rank,"update control_waiting(MAIN)=", control_waiting, i_am_waiting
      print *, my_rank, "MAIN wakes CONTROL"
      control_waiting = .false.
      call th_cond_signal(COND_JS_UPDATE)
    endif
    !endif
    call th_mutex_unlock(LOCK_JS)
    !print *, my_rank, "MAIN unlocked mutex"
  end subroutine local_tgetm
  !*************************************************************
  logical function report_or_store(my_jobs, req)
    !  Purpose: If a job is finished, this cleans up afterwards
    !           Needed for termination algorithm, there are two
    !           cases, it was a job of the own responsibilty or
    !           one from another, first case just change my number
    !            second case, send to victim, how many of his jobs
    !            were finished
    !------------ Modules used ------------------- ---------------
!   use
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in  ) :: my_jobs(SJOB_LEN)
    integer(kind=i4_kind), intent(out ) :: req
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, alloc_stat
    logical                              :: flag
    integer(kind=i4_kind)                :: num_jobs_done, message(1 + SJOB_LEN)
    integer(kind=i4_kind),allocatable    :: intermed(:)
!   !------------ Executable code --------------------------------
    print *, my_rank, "FINISHED a job, now report or store", my_jobs
    report_or_store = .false.
    ! my_jobs hold recent last point, as proc started from beginning and
    ! steal from the back, this means that from the initial starting point on
    ! (stored in start_job) to this one, all jobs were done
    ! if my_jobs(J_EP)/= start_job(J_EP) someone has stolen jobs
    num_jobs_done = my_jobs(J_EP) - start_job(J_STP)
    !if (num_jobs_done == 0) return ! there is non job, thus why care
    if (start_job(NRANK) == my_rank) then
      my_resp_self = my_resp_self + num_jobs_done
      call th_mutex_lock(LOCK_MR)
      !print *, my_rank, "my_resp=", my_resp, "-", num_jobs_done
      my_resp = my_resp - num_jobs_done
      report_or_store = is_my_resp_done(CONTROL, req) ! check if all my jobs are done
      call th_mutex_unlock(LOCK_MR)
    else
      your_resp = your_resp + num_jobs_done
      ! As all isends have to be closed sometimes, storage of
      ! the request handlers is needed
      report_or_store = .true.
      print *, my_rank, "send to", start_job(NRANK),"finished", num_jobs_done, "of his jobs"
      message(1) = DONE_JOB
      message(2) = num_jobs_done
      message(3:) = 0
      call timeloc("done_job",start_job(NRANK))
      call MPI_ISEND(message,1 + SJOB_LEN, MPI_INTEGER4, start_job(NRANK),&
                                  MSGTAG,comm_world, req, ierr)
      !ASSERT(ierr==MPI_SUCCESS)
      call assert_n(ierr==MPI_SUCCESS, 4)
    endif
  end function report_or_store
  !*************************************************************
   logical function divide_jobs(partner, requ)
    !  Purpose: chare jobs from job_storage with partner, tell
    !           partner what he got
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: partner
    integer(kind=i4_kind), intent(out  ) :: requ
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, w
    integer(kind=i4_kind)                :: g_jobs(SJOB_LEN), message(1 + SJOB_LEN)
!   !------------ Executable code --------------------------------
    divide_jobs = .true.
    call th_mutex_lock(LOCK_JS)
    w = reserve_workh(store_m,job_storage)
    g_jobs = job_storage
    message(1) = WORK_DONAT
    if (w == 0) then ! nothing to give, set empty
    if (master_server) then
      message(1) = NO_WORK_LEFT 
      call check_termination(partner, MAILBOX)
      if (partner == termination_master) then
        divide_jobs = .false.
        call th_mutex_unlock(LOCK_JS)
        return
      endif
    endif
      g_jobs(J_EP)  = 0
      g_jobs(J_STP) = 0
      g_jobs(NRANK) = -1
    else ! take the last w jobs of the job-storage
      g_jobs(J_STP)  = g_jobs(J_EP) - w
      job_storage(J_EP) = g_jobs(J_STP)
    endif
    message(2:) = g_jobs
    call timeloc("divjob",partner)
    print *, my_rank,"divide jobs: keep", job_storage, "give to", partner, g_jobs
    call MPI_ISEND(message, 1+SJOB_LEN, MPI_INTEGER4, partner, MSGTAG, comm_world, requ, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    !print *, my_rank, "control_waiting(MAILBOX)=", control_waiting, i_am_waiting
      !print *, my_rank, "update control_waiting(MAILBOX)=", control_waiting, i_am_waiting
    if (job_storage(J_STP) >= job_storage(J_EP) .and. control_waiting) then
      print *, my_rank, "MAILBOX wakes CONTROL"
      !call timepar("wakeCm")
      call th_cond_signal(COND_JS_UPDATE)
    endif
    !endif
    call th_mutex_unlock(LOCK_JS)
  end function divide_jobs
  !*************************************************************
  integer(kind=i4_kind) function reserve_workm(m, jobs)
    ! PURPOSE: give back number of jobs to take, should be up to m, but
    ! less if there are not so many available
    integer(kind=i4_kind), intent(in   ) :: m
    integer(kind=i4_kind), intent(in   ) :: jobs(:)
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    !------------ Executable code --------------------------------
    reserve_workm = min(jobs(J_EP) - jobs(J_STP), m)
    reserve_workm = max(reserve_workm, 0)
  end function reserve_workm
  !*************************************************************
  integer(kind=i4_kind) function reserve_workh(m,jobs)
    ! Purpose: give back number of jobs to take, half what is there
!   !** End of interface *****************************************
    integer(kind=i4_kind), intent(in   ) :: m
    integer(kind=i4_kind), intent(in   ) :: jobs(:)
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: many_jobs
    !------------ Executable code --------------------------------
    many_jobs = (jobs(J_EP) - jobs(J_STP))
    ! if the number of job batches (of size m) is not odd, then
    ! the stealing proc should get more, as it starts working on them
    ! immediatelly

    ! code piece for master_server = true : if all left jobs are stored
    ! on one processor, it should give less than half of it back
    if (master_server) then
      if(chunk_m == 0) then !just a jobslice the proc will do at once (too frequently?)
       reserve_workh = min(many_jobs, m)
      elseif(chunk_m ==1) then !serveral jobs if possible but still a fixed block
        reserve_workh = min(many_jobs, chunksize*m)
      else ! exponential decrease, but not less than m (a job bunch at a time)
        reserve_workh = many_jobs/n_procs
        if(reserve_workh < m) reserve_workh = m
        if (reserve_workh > many_jobs) reserve_workh = 0
      endif
    else ! with not master_server all procs may chare, thus this one can give half of it
      !reserve_workh =  (many_jobs /(2* m))* m
      reserve_workh =  many_jobs /(2)
    endif
    reserve_workh = max(reserve_workh, 0)
  end function reserve_workh
  !*************************************************************
  subroutine dlb_setup(job)
    !  Purpose: initialization of a dlb run, each proc should call
    !           it with inital jobs. The inital jobs should be a
    !           static distribution of all available jobs, each job
    !           should be given to one and only one of the procs
    !           jobs should be given as a job range (STP, EP), where
    !           all jobs should be the numbers from START to END, with
    !           START <= STP <= EP <= END
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: job(L_JOB)
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: jobs(SJOB_LEN), ierr
    !------------ Executable code --------------------------------
    ! these variables are for the termination algorithm
    terminated = .false.
   i_am_waiting = .false.
   control_waiting = .false.
   job_waiting = .false.
    count_messages = 0
    count_offers = 0
    count_requests = 0
    store_m = 1 !only for the case, when a proc is starting without any jobs
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
    call th_create_control(CONTROL)
    if (n_procs > 1) call th_create_mail(MAILBOX)
    call timepar("endsetup")
  end subroutine dlb_setup
  !*************************************************************
  !--------------- End of module ----------------------------------

! make routines for CONTROL and MAILBOX extern, for usage in c-wrapper
end module dlb

subroutine thread_control()
  use dlb
  call thread_control1()
end subroutine thread_control

subroutine thread_mailbox()
  use dlb
  call thread_mailbox1()
end subroutine thread_mailbox
