!===============================================================
! Public interface of module
!===============================================================
module dlb_common
  !---------------------------------------------------------------
  !
  !  Purpose: code common for all DLB implementations
  !
  !  Module called by: ...
  !
  !
  !  References:
  !
  !  Author: AN
  !  Date: 08/10->09/10
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
  use mpi
  implicit none
  save            ! save all variables defined in this module
  private         ! by default, all names are private
  !== Interrupt end of public interface of module =================
  !------------ public functions and subroutines ------------------

  public :: select_victim!(rank, np) -> integer victim

  public :: reserve_workm!(m, jobs) -> integer n
  public :: divide_work!(jobs) -> integer n
  public :: divide_work_master!(m, jobs) -> integer n
  public :: steal_work_for_rma!(m, jobs) -> integer n

  public :: time_stamp_prefix
  public :: time_stamp
  public :: assert_n
  public :: dlb_common_init, dlb_common_finalize, dlb_common_setup
  public :: add_request, test_requests, end_requests
  public :: send_resp_done, report_job_done, send_termination, has_last_done
  public :: set_start_job, set_empty_job
  public :: decrease_resp

  ! integer with 4 bytes, range 9 decimal digits
  integer, parameter, public :: i4_kind = selected_int_kind(9)

  ! integer with 8 bytes, range 9 decimal digits
  integer, parameter :: i8_kind = 8

  ! real with 8 bytes, precision 15 decimal digits
  integer, parameter, public :: r8_kind = selected_real_kind(15)

  integer,  public :: comm_world
  integer(kind=i4_kind), parameter, public  :: DONE_JOB = 1, NO_WORK_LEFT = 2, RESP_DONE = 3 !for distingishuing the messages
  integer(kind=i4_kind), parameter, public  :: SJOB_LEN = 3 ! Length of a single job in interface
  integer(kind=i4_kind), parameter, public  :: L_JOB = 2  ! Length of job to give back from interface
  integer(kind=i4_kind), parameter, public  :: NRANK = 3 ! Number in job, where rank of origin proc is stored
  integer(kind=i4_kind), parameter, public  :: J_STP = 1 ! Number in job, where stp (start point) is stored
  integer(kind=i4_kind), parameter, public  :: J_EP = 2 ! Number in job, where ep (end point) is stored
  integer(kind=i4_kind), parameter, public  ::  MSGTAG = 166 ! message tag for all MPI communication
    logical, parameter, public :: masterserver = .false. ! changes to different variant (master slave concept for comparision)
  integer(kind=i4_kind), public       :: my_rank, n_procs ! some synonyms, They will be initialized once and afterwards
                                        ! be read only
  integer(kind=i4_kind), public             ::  termination_master ! the one who gathers the finished my_resp's
                                         ! and who tells all, when it is complety finished
                                         ! in case of the variant with master as server of jobs, its also the master
  integer(kind=i4_kind), allocatable :: my_resp(:)
  integer(kind=i4_kind), allocatable :: messages(:,:), req_dj(:) !need to store the messages for DONE_JOB,
                     ! as they may still be not finished, when the subroutine for generating them is finshed.
                     ! There may be a lot of them, message_on_way keeps track, to whom they are already on their way
                     ! the requests are handeled also separately, as it is needed to know, which one has finsished:
  logical, allocatable :: message_on_way(:) ! which messages are already sended
  !================================================================
  ! End of public interface of module
  !================================================================

  !------------ Declaration of types ------------------------------

  !------------ Declaration of constants and variables ----

  integer(i4_kind), parameter :: output_border = 2

  double precision :: time_offset = -1.0
  logical, allocatable              :: all_done(:) ! only allocated on termination_master, stores which proc's
                                                   ! jobs are finished. If with masterserver: which proc has terminated
  !----------------------------------------------------------------
  !------------ Subroutines ---------------------------------------
contains

! ONLY FOR DEBBUGING (WITHOUT PARAGAUSS)
  function time_stamp_prefix(time) result(prefix)
    implicit none
    double precision, intent(in) :: time
    character(len=28) :: prefix
    ! *** end of interface ***

    write(prefix, '("#", I3, G20.10)') my_rank, time - time_offset
  end function time_stamp_prefix

  subroutine time_stamp(msg, output_level)
    implicit none
    character(len=*), intent(in) :: msg
    integer(i4_kind), intent(in) :: output_level
    ! *** end of interface ***

    double precision :: time

    time = MPI_Wtime()
    if ( time_offset < 0.0 ) then
      time_offset = 0.0
      print *, time_stamp_prefix(time), "(BASE TIME)"
      time_offset = time
    endif

   if(output_level < output_border) print *, time_stamp_prefix(time), msg
  end subroutine time_stamp

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

  subroutine dlb_common_init()
    ! Intialization of common stuff, needed by all routines
    implicit none
    integer :: ierr, alloc_stat
    call MPI_COMM_DUP(MPI_COMM_WORLD, comm_world, ierr)
    !ASSERT(ierr==0)
    call assert_n(ierr==0,4)
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
  end subroutine

  subroutine dlb_common_finalize()
    ! shut down the common stuff, as needed
    implicit none
    integer :: ierr, alloc_stat
    call MPI_COMM_FREE( comm_world, ierr)
    !ASSERT(ierr==0)
    call assert_n(ierr==0,4)
    if (allocated(all_done)) then
      deallocate(all_done, stat=alloc_stat)
      !ASSERT(alloc_stat==0)
      call assert_n(alloc_stat==0, 1)
    endif
  end subroutine

  subroutine dlb_common_setup(resp)
    ! Termination master start of a new dlb run
    implicit none
    integer(kind=i4_kind), intent(in   ) :: resp
    integer ::  alloc_stat
    if (allocated(all_done)) all_done  = .false.
    ! if there is any exchange of jobs, the following things are needed
    ! (they will help to get the DONE_JOB messages on their way)
    allocate(messages(n_procs, SJOB_LEN + 1), message_on_way(n_procs), stat = alloc_stat)
    !ASSERT(alloc_stat==0)
    call assert_n(alloc_stat==0, 1)
    allocate(my_resp(n_procs), req_dj(n_procs), stat = alloc_stat)
    !ASSERT(alloc_stat==0)
    call assert_n(alloc_stat==0, 1)
    ! Here they are initalizied, at the beginning none message has been
    ! put on its way, from the messages we know everything except the
    ! second entry which will be the number of jobs done
    messages(1,:) = DONE_JOB
    messages(3:,:) = 0
    message_on_way = .false.
    my_resp = 0
    my_resp(my_rank) = resp
  end subroutine dlb_common_setup

  ! To make it easier to add for example new job informations
  ! The direct setting of a comlete job is always done here
  ! There are two functions needed: setting of the inital job
  ! for the own rank and setting an empty, easy as that to
  ! recognise job
  ! it is supposed, that all the informations needed so far, will be
  ! still there, but that new informations will be handled on too
  ! (without this two functions, the informations will just copied
  ! and special ones will be changed)

  function set_start_job(job)
    !Purpose: gives a complete starting job description
    !         after gotten just the job range
    integer(kind=i4_kind), intent(in) :: job(L_JOB)
    integer(kind=i4_kind) :: set_start_job(SJOB_LEN)
    set_start_job(:L_JOB) = job
    set_start_job(NRANK) = my_rank
  end function set_start_job

  function set_empty_job()
    !Purpose: gives a complete starting job description
    !         after gotten just the job range
    integer(kind=i4_kind) :: set_empty_job(SJOB_LEN)
    set_empty_job(J_EP) = 0
    set_empty_job(J_STP) = 0
    set_empty_job(NRANK) = -1
  end function set_empty_job

  function select_victim(rank, np) result(victim)
     ! Purpose: decide of who to try next to get any jobs
     ! Context: for 3 threads: control thread.
     !          for 2 threads: secretary.
     implicit none
     integer(i4_kind), intent(in) :: rank, np
     integer(i4_kind)             :: victim
     ! *** end of interface ***

     victim = select_victim_random(rank, np)
     ! victim = select_victim_r(rank, np)
  end function select_victim

  function select_victim_r(rank, np) result(victim)
     ! Purpose: decide of who to try next to get any jobs
     ! Each in a row
     !
     ! Context: 3 threads: control thread.
     !          2 threads: secretary thread.
     !
     ! Not thread safe! Beware of "save :: count" without rwlock!
     !
     implicit none
     integer(i4_kind), intent(in) :: rank, np
     integer(i4_kind)             :: victim
     ! *** end of interface ***

     integer(kind=i4_kind), save :: count = 1

     victim = mod(rank + count, np)
     count = count + 1
     if (victim == rank) then
        victim = mod(rank + count, np)
        count = count + 1
     endif
  end function select_victim_r

  function select_victim_random(rank, np) result(victim)
     ! Purpose: decide of who to try next to get any jobs
     ! Uses pseudorandom sequence:
     !
     ! X    = (a * X + b) mod m
     !  n+1         n
     !
     ! Context: 3 thread: control thread
     !          2 thread: secretary thread
     !
     ! Not thread safe! Beware of "save :: seed" without rwlock!
     !
     implicit none
     integer(i4_kind), intent(in) :: rank, np
     integer(i4_kind)             :: victim
     ! *** end of interface ***

     integer(i8_kind), parameter :: a = 134775813, b = 1 ! see Virtual Pascal/Borland Delphi
     integer(i8_kind), parameter :: m = 2_i8_kind**32
     integer(i8_kind), save :: seed = -1

     if (seed == -1) then
        seed = rank
     endif

     ! PRNG step:
     seed = mod(a * seed + b, m)

     ! np - 1 outcomes in the range [0, np-1] excluding victim == rank:
     if( np > 1) then
       victim = mod(seed, np - 1)
       if (victim >= rank) victim = victim + 1
     else
       victim = 0
     endif
  end function select_victim_random

  pure function reserve_workm(m, jobs) result(n)
    ! PURPOSE: give back number of jobs to take, should be up to m, but
    ! less if there are not so many available
    implicit none
    integer(i4_kind), intent(in) :: m
    integer(i4_kind), intent(in) :: jobs(2)
    integer(i4_kind)             :: n ! result
    !** End of interface *****************************************

    n = min(jobs(2) - jobs(1), m)
    n = max(n, 0)
  end function reserve_workm

  pure function divide_work(jobs) result(n)
    ! Purpose: give back number of jobs to take, half what is there
    !          take less than half if could not equally divided
    implicit none
    integer(i4_kind), intent(in) :: jobs(2)
    integer(i4_kind)             :: n ! result
    !** End of interface *****************************************

    ! give half of all jobs:
    n =  (jobs(2) - jobs(1)) / 2
    n = max(n, 0)
  end function divide_work

  pure function divide_work_master(jobs, np) result(n)
    ! Purpose: give back number of jobs to take,
    !          as all jobs to share are located at master give back
    !          portion of division by the number of jobs
    implicit none
    integer(i4_kind), intent(in) :: jobs(2)
    integer(i4_kind), intent(in) :: np
    integer(i4_kind)             :: n ! result
    !** End of interface *****************************************

    ! exponential decrease, but not less than m (a job bunch at a time)
    n =  (jobs(2) - jobs(1)) / np
    if (n > (jobs(2) - jobs(1))) n = 0
  end function divide_work_master

  pure function steal_work_for_rma(m, jobs) result(n)
    ! Purpose: give back number of jobs to take, half what is there
    !          but gives more if could not divided equally
    !          this should ensure the progress of the whole program
    implicit none
    integer(i4_kind), intent(in) :: m
    integer(i4_kind), intent(in) :: jobs(2)
    integer(i4_kind)             :: n ! result
    !** End of interface *****************************************

    ! give half of all jobs:
    n =  (jobs(2) - jobs(1)) / (2 * m) * m
    n = max(n, 0)
    ! but give more, if job numbers not odd
    n =  (jobs(2) - jobs(1)) - n
  end function steal_work_for_rma

  subroutine add_request(req, requ)
    ! Purpose: stores unfinished requests
    !          there is a storage requ, here keep all that is
    !          inside but add also req
    !
    ! Context: control thread, mailbox thread.
    !          2 threads: secretary thread
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

  subroutine test_requests(requ)
    ! Purpose: tests if any of the messages stored in requ have been
    !          received, than remove the corresponding request
    !          requ is local request of the corresponding thread
    !
    ! Context for 3Threads: mailbox thread, control thread.
    !             2 Threads: secretary thread
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
    if (.not. allocated(requ)) RETURN

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
    do i = 0, size(message_on_way) ! messages for DONE_JOBS
          ! have to be handled separatly, as the messages have
          ! to be kept and may not be changed till the request
          ! has been sended, but here also some messages
          ! may be finished, message_on_way stores informations
          ! to whom there are still some messages of DONE_JOBS
          ! on their way
      if (message_on_way(i)) then
        call MPI_TEST(req_dj(i), flag, stat, ierr)
        !ASSERT(ierr==MPI_SUCCESS)
        call assert_n(ierr==MPI_SUCCESS, 4)
        if (flag) then
           message_on_way(i) = .false.
        endif
      endif
    enddo
  end subroutine test_requests

  subroutine end_requests(requ)
    ! Purpose: end all of the stored requests in requ
    !          no matter if the corresponding message has arived
    !
    ! Context for 3Threads: mailbox thread, control thread.
    !             2 Threads: secretary thread
    !------------ Modules used ------------------- ---------------
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer, allocatable :: requ(:)
    integer(kind=i4_kind)                :: i, ierr
    integer(kind=i4_kind)                :: alloc_stat
    integer(kind=i4_kind)                :: stat(MPI_STATUS_SIZE)
    !------------ Executable code --------------------------------
    if (allocated(requ)) then
      do i = 1, size(requ,1)
        call MPI_CANCEL(requ(i), ierr)
        !ASSERT(ierr==MPI_SUCCESS)
        call assert_n(ierr==MPI_SUCCESS, 4)
        call MPI_WAIT(requ(i),stat, ierr)
        !ASSERT(ierr==MPI_SUCCESS)
        call assert_n(ierr==MPI_SUCCESS, 4)
      enddo
      deallocate(requ, stat=alloc_stat)
      !ASSERT(alloc_stat==0)
      call assert_n(alloc_stat==0, 4)
    endif
    do i = 1, size(req_dj)
      if (message_on_way(i)) then
        ! the messages DONE_JOB should be all finshed already
        ! so using MPI_CANCEL is just for being completly certain
        call MPI_CANCEL(req_dj(i), ierr)
        !ASSERT(ierr==MPI_SUCCESS)
        call assert_n(ierr==MPI_SUCCESS, 4)
        call MPI_WAIT(req_dj(i),stat, ierr)
        !ASSERT(ierr==MPI_SUCCESS)
        call assert_n(ierr==MPI_SUCCESS, 4)
      endif
    enddo
    ! these variables will only be needed after the next dlb-setup
    deallocate(message_on_way, req_dj, messages, my_resp, stat=alloc_stat)
    !ASSERT(alloc_stat==0)
    call assert_n(alloc_stat==0, 4)
  end subroutine end_requests

! algorithm for termination (is in principle the same for all dynamical variants)
  integer(i4_kind) function decrease_resp(n, source)
    ! Purpose:  decrease my_resp  by done jobs, for itself increamental
    !          other procs give sum of their changes
    !          return sum, thats all jobs which have done
    !
    !          there is need to differenciate between the resp of the proc itself
    !          and those he gets from others, the other will give always the sum of all
    !          they have done for this proc, as this proc wants to store only the ones
    !          of the current job batch
    implicit none
    integer(i4_kind), intent(in)  :: n, source
    ! *** end of interface ***

    if (source == my_rank) then
    my_resp(source) = my_resp(source) - n
    else
    my_resp(source) = -n
    endif
    decrease_resp = sum(my_resp)
  end function decrease_resp

  subroutine send_resp_done(requ)
    !  Purpose: my_resp holds the number of jobs, assigned at the start
    !           to this proc, so this proc is responsible that they will
    !           be finished, if my_resp == 0, I should tell termination_master
    !           that, termination master will collect all the finished resp's
    !           untill it gots all of them back
    !
    ! Context 3Thread: mailbox  and control thread.
    !             2 Threads: secretary thread
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
    integer(kind=i4_kind), save          :: message(1+SJOB_LEN) ! message may only be
     ! changed or rewritten after communication finished, thus it is saved here in order
     ! to still be present when the subroutine finishes
     ! as this routine is only called once each process in each dlb call
     ! it will not be overwritten too sone
    !------------ Executable code --------------------------------

    message(1) = RESP_DONE
    message(2) = my_rank
    message(3:) = 0
    call MPI_ISEND(message, 1+SJOB_LEN, MPI_INTEGER4, termination_master, MSGTAG,comm_world, req, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    call add_request(req, requ)
  end subroutine send_resp_done

  subroutine report_job_done(num_jobs_done, source, requ)
    !  Purpose: If a job is finished, this cleans up afterwards
    !           Needed for termination algorithm, there are two
    !           cases, it was a job of the own responsibilty or
    !           one from another, first case just change my number
    !            second case, send to victim, how many of his jobs
    !            were finished
    !
    ! Context: control thread.
    !             2 Threads: secretary thread
    !
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in  ) :: num_jobs_done, source
    integer(kind=i4_kind), allocatable  :: requ(:)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, stat(MPI_STATUS_SIZE)
    integer(kind=i4_kind)                :: i
    !------------ Executable code --------------------------------
    ! We need the messages and req_dj entry of source in any case
    ! thus if there is still a message pending delete it
    ! by sending the sum of all jobs done so far, it is not
    ! of interest any more if the previous message has arrived
    if (message_on_way(source)) then
        call MPI_CANCEL(req_dj(source), ierr)
        !ASSERT(ierr==MPI_SUCCESS)
        call assert_n(ierr==MPI_SUCCESS, 4)
        call MPI_WAIT(req_dj(source),stat, ierr)
        !ASSERT(ierr==MPI_SUCCESS)
        call assert_n(ierr==MPI_SUCCESS, 4)
    endif
    messages(source, 2) = messages(source, 2) +  num_jobs_done
    call time_stamp("send message to source", 2)
    call MPI_ISEND(messages(source,:),1 + SJOB_LEN, MPI_INTEGER4, source,&
                                MSGTAG,comm_world, req_dj, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    message_on_way(source) = .true.
  end subroutine report_job_done

  logical function has_last_done(proc)
    !  Purpose: only on termination_master, checks if reported one
    !           is the last proc to report finished
    !
    !  Context:   3 Threads: mailbox thread
    !             2 Threads: secretary thread
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in)    :: proc
    all_done(proc+1) = .true.
    has_last_done = all(all_done)
  end function has_last_done

  subroutine send_termination()
    !  Purpose: only on termination_master, send to all that termination
    !           has been reached
    !
    ! Context 3Thread: mailbox thread. (termination master only)
    !             2 Threads: secretary thread
    !
    ! FIXME: is it okay to use the same message buffer for all the (exactly same)
    !        messages?
    !      At least here it is okay to have the message only during the routine
    !      as it will be wait for finishing before returning to the calling
    !      code
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, i, alloc_stat
    integer(kind=i4_kind), allocatable   :: request(:), stats(:,:)
    integer(kind=i4_kind)                :: receiver, message(1+SJOB_LEN)
    !------------ Executable code --------------------------------

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
    call MPI_ISEND(message, 1+SJOB_LEN, MPI_INTEGER4, receiver, MSGTAG, comm_world ,request(i+1), ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    enddo
    call MPI_WAITALL(size(request), request, stats, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
  end subroutine send_termination

end module dlb_common
