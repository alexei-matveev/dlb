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
# include "dlb.h"
  USE_MPI
  implicit none
  save            ! save all variables defined in this module
  private         ! by default, all names are private
  !== Interrupt end of public interface of module =================
  !------------ public functions and subroutines ------------------

  public :: distribute_jobs
  public :: select_victim!(rank, np) -> integer victim

  public :: steal_local!(m, local, remaining, stolen) result(ok)
  public :: steal_remote!(m, remote, remaining, stolen) result(ok)

  public :: length!(jobs) -> integer
  public :: empty!(jobs) -> logical

  public :: divide_work!(jobs, np) -> integer n
  public :: split_at! (C, AB, AC, CB)

  public :: irand!(long int) -> long int

  public :: time_stamp_prefix
  public :: time_stamp
  public :: dlb_common_init, dlb_common_finalize, dlb_common_setup
  public :: add_request, test_requests, end_requests
  public :: end_communication
  public :: send_resp_done, send_termination, has_last_done
  public :: set_start_job, set_empty_job
  public :: output_border

  public :: report_to!(owner, n), with n being an incremental report
  public :: report_by!(source, n), with n being a cumulative report
  public :: reports_pending!() -> integer

  public :: isend!(buf, rank, tag, req)

  public :: clear_up

  public :: print_statistics
  ! integer with 4 bytes, range 9 decimal digits
  integer, parameter, public :: i4_kind = selected_int_kind(9)

  ! integer with 8 bytes, range 9 decimal digits
  integer, parameter :: i8_kind = 8

  ! real with 8 bytes, precision 15 decimal digits
  integer, parameter, public :: r8_kind = selected_real_kind(15)

  integer,  public, protected :: comm_world

  integer(kind=i4_kind), parameter, public  :: DONE_JOB = 1, NO_WORK_LEFT = 2, RESP_DONE = 3 !for distingishuing the messages
  integer(kind=i4_kind), parameter, public  :: WORK_REQUEST = 4, WORK_DONAT = 5 ! messages for work request
  integer(kind=i4_kind), parameter, public  :: JLENGTH = 3 ! Length of a single job in interface
  integer(kind=i4_kind), parameter, public  :: L_JOB = 2  ! Length of job to give back from interface
  integer(kind=i4_kind), parameter, public  :: JOWNER = 3 ! Number in job, where rank of origin proc is stored
  integer(kind=i4_kind), parameter, public  :: JLEFT = 1 ! Number in job, where stp (start point) is stored
  integer(kind=i4_kind), parameter, public  :: JRIGHT = 2 ! Number in job, where ep (end point) is stored
  integer(kind=i4_kind), parameter, public  ::  MSGTAG = 166 ! message tag for all MPI communication

#ifdef DLB_MASTER_SERVER
    logical, parameter, public :: masterserver = .true. ! changes to different variant (master slave concept for comparision)
#else
    logical, parameter, public :: masterserver = .false. ! changes to different variant (master slave concept for comparision)
#endif

  integer(kind=i4_kind), public, protected :: my_rank, n_procs ! some synonyms, They will be initialized once and afterwards
                                        ! be read only

  !
  ! Termination master is the process that gathers completion reports
  ! and tells everyone to terminate. In case of the variant with master
  ! as server of jobs, its also the master.
  !
  integer(i4_kind), public, protected :: termination_master

  !================================================================
  ! End of public interface of module
  !================================================================

  !
  ! This scalar holds the number of jobs initially assigned to the process
  ! owning this variable. FIXME: make it an array (1:n_procs) to keep full
  ! info about initial assignment. Currently not doable as the setup procedure
  ! is provided only one assignment (to myself).
  !
  integer(i4_kind) :: responsibility = -1

  !
  ! This array holds the counts of jobs that were initially assigned to the
  ! process owning this structure and later reported as done by one of the
  ! workers. Note that sum(reported_by(:)) is to be compared with
  ! with the total number of jobs from the initial assignment kept in
  ! the variable "responsibility".
  !
  integer(i4_kind), allocatable :: reported_by(:) ! (0:n_procs-1)

  !
  ! The next array holds the counts of jobs that were delivered to "userspace"
  ! of the process owning this structure (aka jobs that were "done") and reported
  ! to the respective job owner according to initial assignment.
  ! Note that sum(reported_to(1:n_procs)) is the number of jobs "done" by the
  ! process owning this structure. We dont abuse message buffers to keep this data
  ! anymore.
  !
  integer(i4_kind), allocatable :: reported_to(:) ! (0:n_procs-1)

  integer(kind=i4_kind), allocatable :: messages(:,:), req_dj(:) !need to store the messages for DONE_JOB,
                     ! as they may still be not finished, when the subroutine for generating them is finshed.
                     ! There may be a lot of them, message_on_way keeps track, to whom they are already on their way
                     ! the requests are handeled also separately, as it is needed to know, which one has finsished:
  logical, allocatable :: message_on_way(:) ! which messages are already sended

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
! END ONLY FOR DEBUGGING

  subroutine dlb_common_init()
    ! Intialization of common stuff, needed by all routines
    implicit none
    ! *** end of interface ***

    integer :: ierr, alloc_stat

    call MPI_COMM_DUP(MPI_COMM_WORLD, comm_world, ierr)
    ASSERT(ierr==0)

    call MPI_COMM_RANK( comm_world, my_rank, ierr )
    ASSERT(ierr==MPI_SUCCESS)

    call MPI_COMM_SIZE(comm_world, n_procs, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    !
    ! FIXME: This is just a choice, not a requirement, right?
    !
    termination_master = n_procs - 1

    !
    ! Only on termination master:
    !
    if (my_rank == termination_master) then
      allocate(all_done(n_procs), stat = alloc_stat)
      ASSERT(alloc_stat==0)
    endif
  end subroutine

  subroutine dlb_common_finalize()
    ! shut down the common stuff, as needed
    implicit none
    ! *** end of interface ***

    integer :: ierr, alloc_stat

    !
    ! Only on termination master:
    !
    if (allocated(all_done)) then
      deallocate(all_done, stat=alloc_stat)
      ASSERT(alloc_stat==0)
    endif

    call MPI_COMM_FREE(comm_world, ierr)
    ASSERT(ierr==0)

    if (comm_world .ne. MPI_COMM_NULL) then
      print *, my_rank, "MPI communicator was not freed correctly"
      call abort()
    endif
  end subroutine

  subroutine dlb_common_setup(resp)
    ! Termination master start of a new dlb run
    implicit none
    integer(kind=i4_kind), intent(in   ) :: resp
    ! *** end of interface ***

    integer ::  alloc_stat

    !
    ! Only on termination master:
    !
    if (allocated(all_done)) all_done  = .false.

    ! if there is any exchange of jobs, the following things are needed
    ! (they will help to get the DONE_JOB messages on their way)
    allocate(messages(JLENGTH + 1, n_procs), stat = alloc_stat)
    ASSERT(alloc_stat==0)

    allocate(message_on_way(n_procs), stat = alloc_stat)
    ASSERT(alloc_stat==0)

    allocate(req_dj(n_procs), stat = alloc_stat)
    ASSERT(alloc_stat==0)

    !
    ! These arrays are indexed by MPI ranks, make them base-0:
    !
    allocate(reported_by(0:n_procs-1), stat = alloc_stat)
    ASSERT(alloc_stat==0)

    allocate(reported_to(0:n_procs-1), stat = alloc_stat)
    ASSERT(alloc_stat==0)

    !
    ! Nothing reported yet:
    !
    reported_by(:) = 0
    reported_to(:) = 0
    responsibility = resp


    ! Here they are initalizied, at the beginning none message has been
    ! put on its way, from the messages we know everything except the
    ! second entry which will be the number of jobs done
    messages = 0
    messages(1,:) = DONE_JOB
    message_on_way = .false.
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

  function set_start_job(job) result(job_data)
    !Purpose: gives a complete starting job description
    !         after gotten just the job range
    implicit none
    integer(i4_kind), intent(in) :: job(L_JOB)
    integer(i4_kind)             :: job_data(JLENGTH)
    ! *** end of interface ***

    job_data(:L_JOB) = job
    job_data(JOWNER) = my_rank
  end function set_start_job

  function set_empty_job() result(job_data)
    !Purpose: gives a complete starting job description
    !         after gotten just the job range
    implicit none
    integer(i4_kind) :: job_data(JLENGTH)
    ! *** end of interface ***

    job_data(JRIGHT) = 0
    job_data(JLEFT) = 0
    job_data(JOWNER) = -1
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

  pure function irand(i) result(p)
     ! Pseudorandom sequence:
     !
     ! X    = (a * X + b) mod m
     !  n+1         n
     !
     implicit none
     integer(i8_kind), intent(in) :: i
     integer(i8_kind)             :: p
     ! *** end of interface ***

     ! see Virtual Pascal/Borland Delphi
     integer(i8_kind), parameter :: A = 134775813, B = 1
     integer(i8_kind), parameter :: RANGE = 2_i8_kind**32

     p = mod(A * i + B, RANGE)
  end function irand

  function select_victim_random(rank, np) result(victim)
     ! Purpose: decide of who to try next to get any jobs
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

     integer(i8_kind), save :: seed = -1
     integer(i8_kind) :: np8

     if (seed == -1) then
        seed = rank
     endif

     ! PRNG step:
     seed = irand(seed)

     ! np - 1 outcomes in the range [0, np-1] excluding victim == rank:
     if( np > 1) then
       np8 = np ! convert to long int
       victim = mod(seed, np8 - 1)
       if (victim >= rank) victim = victim + 1
     else
       victim = 0
     endif
  end function select_victim_random

  function steal_remote(m, remote, remaining, stolen) result(ok)
    !
    ! Stealing is implemented as splitting the interval
    !
    !     (A, B] = remote(1:2)
    !
    ! into
    !
    !     remaining(1:2) = (A, C] and stolen(1:2) = (C, B]
    !
    ! at the point C computed with the help of the legacy procedure as
    !
    !     C = B - length(remote) / (2m) * m
    !
    ! NOTE: This function has to adhere to the interface of modify(...)
    ! argument in try_read_modify_write(...)
    !
    ! FIXME: the role of parameter "m" is not clear!
    !
    ! use dlb_common, only: i4_kind, JLENGTH, split_at
    implicit none
    integer(i4_kind), intent(in)  :: m
    integer(i4_kind), intent(in)  :: remote(:) ! (JLENGTH)
    integer(i4_kind), intent(out) :: remaining(:) ! (JLENGTH)
    integer(i4_kind), intent(out) :: stolen(:) ! (JLENGTH)
    logical                       :: ok ! result
    ! *** end of interface ***

    integer(i4_kind) :: work, c

    ASSERT(size(remote)==JLENGTH)
    ASSERT(size(remaining)==JLENGTH)
    ASSERT(size(stolen)==JLENGTH)
    ASSERT(m>=0)

    ok = .false. ! failure
    remaining = -1 ! junk
    stolen = -1 ! junk

    !
    ! Now compute how much of the work should be stolen
    ! try to avoid breaking in job intervals smaller than m
    !

    !
    ! This much is is available:
    !
    work = length(remote)

    !
    ! FIXME: Is this logic still necessary?
    !
    ! Leave a multiple of "m", steal the rest.
    ! Note that for m==1 a (bigger) half will be stolen:
    !
    work = work - work / (2 * m) * m
    ASSERT(work>=0)

    !
    ! Split the interval here:
    !
    c = remote(JLEFT) + work

    call split_at(c, remote, stolen, remaining)

    !
    ! Stolen interval will be empty if work == 0:
    !
    ok = .not. empty(stolen)
  end function steal_remote

  function steal_local(m, local, remaining, stolen) result(ok)
    !
    ! "Stealing" from myself is implemented as splitting the interval
    !
    !     (A, B] = local(1:2)
    !
    ! into
    !
    !     stolen(1:2) = (A, C] and remaining(1:2) = (C, B]
    !
    ! at the point
    !
    !     C = A + min(B - A, m)
    !
    ! NOTE: This function has to adhere to the interface of modify(...)
    ! argument in try_read_modify_write(...)
    !
    ! use dlb_common, only: i4_kind, JLENGTH, split_at
    implicit none
    integer(i4_kind), intent(in)  :: m
    integer(i4_kind), intent(in)  :: local(:) ! (JLENGTH)
    integer(i4_kind), intent(out) :: remaining(:) ! (JLENGTH)
    integer(i4_kind), intent(out) :: stolen(:) ! (JLENGTH)
    logical                       :: ok ! result
    ! *** end of interface ***

    integer(i4_kind) :: work, c

    ASSERT(size(local)==JLENGTH)
    ASSERT(size(remaining)==JLENGTH)
    ASSERT(size(stolen)==JLENGTH)
    ASSERT(m>=0)

    ! The give_grid function needs only up to m jobs at once:
    work = min(length(local), m)
    ! work = max(work, 0)
    ASSERT(work>=0)

    ! split here:
    c = local(JLEFT) + work

    !
    ! Note the order of (stolen, remaining) --- left interval is stolen, right
    ! interval is remaining:
    !
    call split_at(c, local, stolen, remaining)

    ok = .not. empty(stolen)
  end function steal_local

  logical function empty(jobs)
    implicit none
    integer(i4_kind), intent(in) :: jobs(:)
    ! *** end of interface ***

    empty = length(jobs) == 0
  end function empty

  function length(jobs) result(n)
    implicit none
    integer(i4_kind), intent(in) :: jobs(:)
    integer(i4_kind)             :: n ! result
    ! *** end of interface ***

    ASSERT(size(jobs)==JLENGTH)

    n = max(jobs(JRIGHT) - jobs(JLEFT), 0)
  end function length

  subroutine split_at(C, AB, AC, CB)
    !
    ! Split (A, B] into (A, C] and (C, B]
    !
    implicit none
    integer(i4_kind), intent(in)  :: C, AB(:)
    integer(i4_kind), intent(out) :: AC(:), CB(:)
    ! *** end of interface ***

    ASSERT(size(AB)==JLENGTH)
    ASSERT(size(AC)==JLENGTH)
    ASSERT(size(CB)==JLENGTH)

    ASSERT(C>=AB(JLEFT))
    ASSERT(C<=AB(JRIGHT))

    ! copy trailing posiitons, if any:
    AC(:) = AB(:)
    CB(:) = AB(:)

    AC(JLEFT)  = AB(JLEFT)
    AC(JRIGHT) = C

    CB(JLEFT)  = C
    CB(JRIGHT) = AB(JRIGHT)
  end subroutine split_at

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
      len_req = size(requ)
      allocate(req_int(len_req), stat = alloc_stat)
      ASSERT(alloc_stat==0)
      req_int = requ
      deallocate(requ, stat = alloc_stat)
      ASSERT(alloc_stat==0)
    endif
    allocate(requ(len_req +1), stat = alloc_stat)
    ASSERT(alloc_stat==0)
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
    ASSERT(alloc_stat==0)
    finished = .false.
    len_new = len_req
    do i = 1, len_req
      req = requ(i)
      call MPI_TEST(req, flag, stat, ierr)
      ASSERT(ierr==MPI_SUCCESS)
      if (flag) then
        finished(i) = .true.
        len_new = len_new - 1
      endif
    enddo
    requ_int(:) = requ(:)
    deallocate(requ, stat = alloc_stat)
    ASSERT(alloc_stat==0)
    if (len_new > 0) then
      allocate(requ(len_new), stat = alloc_stat)
      ASSERT(alloc_stat==0)
      j = 0
      do i = 1, len_req
        if (.not. finished(i)) then
          j = j + 1
          requ(j) = requ_int(i)
        endif
      enddo
    endif
    deallocate(requ_int, finished, stat = alloc_stat)
    ASSERT(alloc_stat==0)
    do i = 1, size(message_on_way) ! messages for DONE_JOBS
          ! have to be handled separatly, as the messages have
          ! to be kept and may not be changed till the request
          ! has been sended, but here also some messages
          ! may be finished, message_on_way stores informations
          ! to whom there are still some messages of DONE_JOBS
          ! on their way
      if (message_on_way(i)) then
        call MPI_TEST(req_dj(i), flag, stat, ierr)
        ASSERT(ierr==MPI_SUCCESS)
        if (flag) then
           message_on_way(i) = .false.
        endif
      endif
    enddo
  end subroutine test_requests

  subroutine end_requests(requ)
    ! Purpose: end all of the stored requests in requ
    !          at this point all the communication has to be
    !          ended already! After terminated = .True. for
    !          rma variant, and after clear_up of threads
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
        call MPI_WAIT(requ(i),stat, ierr)
        ASSERT(ierr==MPI_SUCCESS)
      enddo
      deallocate(requ, stat=alloc_stat)
      ASSERT(alloc_stat==0)
    endif
  end subroutine end_requests

  subroutine end_communication()
    ! Purpose: end all of the stored requests in requ
    !          no matter if the corresponding message has arived
    !
    ! Context for 3Threads: mailbox thread, control thread.
    !             2 Threads: secretary thread
    !------------ Modules used ------------------- ---------------
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: i, ierr
    integer(kind=i4_kind)                :: alloc_stat
    integer(kind=i4_kind)                :: stat(MPI_STATUS_SIZE)
    !------------ Executable code --------------------------------
    do i = 1, size(message_on_way)
      if (message_on_way(i)) then
        ! the messages DONE_JOB should be all finshed already
        call MPI_WAIT(req_dj(i),stat, ierr)
        ASSERT(ierr==MPI_SUCCESS)
      endif
    enddo
    ! these variables will only be needed after the next dlb-setup
    deallocate(message_on_way, messages, stat=alloc_stat)
    ASSERT(alloc_stat==0)
    deallocate(req_dj, reported_by, reported_to, stat=alloc_stat)
    ASSERT(alloc_stat==0)
  end subroutine end_communication

  subroutine clear_up(my_last, last_proc, arrived, requ)
    ! Purpose: clear_up will finish the still outstanding communications of the
    !          thread variants, after the flag termination was set. The most computational
    !          costy part of this is a MPI_ALLGATHER.
    !          Each proc tells threre from which proc he waits for an answer and gives the
    !          number of his message counter to this proc. This number tells the other proc,
    !          if he has already answered, but the answer has not yet arrived, or if he has
    !          to really wait for the message. If every proc now on how many messages he has to
    !          wait, he does so (responds if neccessary) and finishes.
    !          A MPI_barrier in the main code ensures, that he won't get messges belonging to the
    !          next mpi cycle.
    ! Context for 3Threads: mailbox thread.
    !             2 Threads: secretary thread
    !------------ Modules used ------------------- ---------------
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer, allocatable  :: requ(:)
    integer(kind=i4_kind), intent(in) :: my_last(:), arrived(:), last_proc
    integer(kind=i4_kind) :: ierr, i, count_req, req
    integer(kind=i4_kind) :: rec_buff(n_procs * 2)
    integer(kind=i4_kind)                :: stat(MPI_STATUS_SIZE)
    integer(kind=i4_kind)                :: message_s(1 + JLENGTH), message_r(1 + JLENGTH)
    !------------ Executable code --------------------------------

    rec_buff = 0
    rec_buff(2 * my_rank + 1) = last_proc ! Last I sended to or -1
    rec_buff(2 * my_rank+2) = my_last(last_proc + 1) ! the request number I sended to him
    call MPI_ALLGATHER(MPI_IN_PLACE, 0, MPI_DATATYPE_NULL, rec_buff, 2, MPI_INTEGER4, comm_world, ierr)
    ASSERT(ierr==MPI_SUCCESS)
    count_req = 0

    ! If I'm waiting for a job donation also
    if (last_proc > -1) count_req = 1

    do i = 1, n_procs
      if (rec_buff(2*i-1) == my_rank) then
         ! This one sended it last message to me
         ! now check if it has already arrived (answerded)
         if (rec_buff(2*i) .NE. arrived(i)) count_req = count_req + 1
      endif
    enddo

    ! Cycle over all left messages, blocking MPI_RECV, as there is nothing else to do
    if (count_req > 0) then
      do i =1, count_req
        call MPI_RECV(message_r, 1+JLENGTH, MPI_INTEGER4, MPI_ANY_SOURCE, MSGTAG, comm_world, stat, ierr)
        select case(message_r(1))
        case (WORK_DONAT)
          cycle
        case (WORK_REQUEST)
           ! FIXME: tag is the only info we send:
           message_s(:) = 0
           call isend(message_s, stat(MPI_SOURCE), WORK_DONAT, req)
           call add_request(req, requ)
        case default
          print *, my_rank, "got Message", message_r, ", which I wasn't waiting for"
          stop "got unexpected message"
        end select
      enddo
    endif
    ! Now all request are finished and could be ended
    call end_requests(requ)
  end subroutine clear_up

  subroutine print_statistics()
    ! Purpose: end all of the stored requests in requ
    !          no matter if the corresponding message has arived
    !
    ! Context for 3Threads: mailbox thread, control thread.
    !             2 Threads: secretary thread
    implicit none
    !** End of interface *****************************************

    if (1 < output_border) then
        ! executed by me, sorted by owner:
        write(*, '("[", I3, "] REPORTED_TO =", 128I4)")') my_rank, reported_to

        ! assigned to me, sorted by execution host:
        write(*, '("[", I3, "] REPORTED_BY =", 128I4)")') my_rank, reported_by
    endif

    !
    ! FIXME: why cannot I use collective communications here?
    !        Tried MPI_BARRIER for pretty printing.
    !
  end subroutine print_statistics

  subroutine report_by(source, n)
    !
    ! Handles arriving scheduling reports.
    ! NOTE: "n" is NOT an increment, but a cumulative
    ! report of the job count from my initial assignment
    ! scheduled by the source.
    !
    implicit none
    integer(i4_kind), intent(in) :: n, source
    ! *** end of interface ***

    reported_by(source) = n
    ! ... yes, this array was allocated as base-0.
  end subroutine report_by

  function reports_pending() result(pending)
    !
    ! How many jobs from my initial assignment
    ! have not yet been reported as scheduled.
    !
    implicit none
    integer(i4_kind) :: pending
    ! *** end of interface ***

    pending = responsibility - sum(reported_by)
    ASSERT(pending>=0)
  end function reports_pending

  subroutine send_resp_done(requ)
    ! Purpose: when condition
    !
    !   sum(reported_by(:)) == responsibility
    !
    ! is met, I should tell termination_master that jobs initially
    ! assigned to be have been scheduled by calling this sub.
    ! Termination master will collect all the finished resp's
    ! untill it gots all of them back
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
    integer(kind=i4_kind), allocatable   :: requ(:)
    !** End of interface *****************************************

    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: req
    integer(kind=i4_kind), save          :: message(1+JLENGTH) ! message may only be
     ! changed or rewritten after communication finished, thus it is saved here in order
     ! to still be present when the subroutine finishes
     ! as this routine is only called once each process in each dlb call
     ! it will not be overwritten too soon
    !------------ Executable code --------------------------------

    ! FIXME: maybe use MPI_SOURCE status field on receiving side?
    message(:) = 0
    message(2) = my_rank

    call isend(message, termination_master, RESP_DONE, req)

    call add_request(req, requ)
  end subroutine send_resp_done

  subroutine report_to(owner, num_jobs_done)
    !
    ! Handles reporting scheduled jobs, expects an increment
    ! and an owner rank.
    !
    ! Needed for termination algorithm, there are two
    ! cases, it was a job of the own responsibilty or
    ! one from another, first case just change my number
    ! second case, send a cumulative report on how many of
    ! his jobs were scheduled.
    !
    ! Context: control thread.
    !             2 Threads: secretary thread
    !
    implicit none
    integer(i4_kind), intent(in) :: owner, num_jobs_done
    !** End of interface *****************************************

    integer(i4_kind) :: ierr, stat(MPI_STATUS_SIZE)
    integer(i4_kind) :: owner1 ! == owner + 1

    ! FIXME: for compatibility reasons:
    if ( num_jobs_done == 0 ) then
        print *, "report_to: why reporting nothing?"
        RETURN
    endif
    ASSERT(num_jobs_done>=0)

    !
    ! Increment the local counters keeping track of all reports:
    !
    reported_to(owner) = reported_to(owner) + num_jobs_done
    ! ... yes, this array was allocated as base-0.

    if ( owner == my_rank ) then
        !
        ! We intentionally avoid sending messages to myself.
        ! FIXME: schould we really?
        !
        call report_by(owner, reported_to(owner))
    else
        ! base-1 rank of the owner for indexing into fortran arrays:
        owner1 = owner + 1

        ! We need the messages and req_dj entry of source in any case
        ! thus if there is still a message pending delete it
        ! by sending the sum of all jobs done so far, it is not
        ! of interest any more if the previous message has arrived
        if (message_on_way(owner1)) then
            call MPI_CANCEL(req_dj(owner1), ierr)
            ASSERT(ierr==MPI_SUCCESS)
            call MPI_WAIT(req_dj(owner1), stat, ierr)
            ASSERT(ierr==MPI_SUCCESS)
        endif

        call time_stamp("send message to owner1", 2)

        !
        ! Cumulative report to remote owner:
        ! (hopefully after an MPI_CANCEL we can reuse the buffer)
        !
        messages(:, owner1) = 0
        messages(2, owner1) = reported_to(owner)

        call isend(messages(:, owner1), owner, DONE_JOB, req_dj(owner1))

        message_on_way(owner1) = .true.
    endif
  end subroutine report_to

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
    integer(kind=i4_kind)                :: receiver, message(1+JLENGTH)
    !------------ Executable code --------------------------------

    allocate(request(n_procs -1), stats(n_procs -1, MPI_STATUS_SIZE),&
    stat = alloc_stat)
    ASSERT(alloc_stat==0)

    ! FIXME: reusing the same buffer for non-blocking sends:
    message(:) = 0
    do i = 0, n_procs-2
        receiver = i
        ! skip the termination master itself:
        if (i >= termination_master) receiver = i+1
        call time_stamp("send termination", 5)

        ! FIXME: the tag is the only useful info sent:
        call isend(message, receiver, NO_WORK_LEFT, request(i+1))
    enddo
    call MPI_WAITALL(size(request), request, stats, ierr)
    ASSERT(ierr==MPI_SUCCESS)
  end subroutine send_termination

  subroutine isend(buf, rank, tag, req)
    !
    ! Here buf(1) is set to "tag" and an ISEND request is posted
    ! with a fixed MPI MSGTAG
    !
    ! FIXME: dont abuse buf(1) use MPI tags instead
    !
    implicit none
    integer(i4_kind), intent(inout), target :: buf(:) ! (1+JLENGTH)
    integer(i4_kind), intent(in)            :: rank, tag
    integer(i4_kind), intent(out)           :: req
    ! *** end of interface ***

    integer(i4_kind) :: ierr

    ASSERT(size(buf)==1+JLENGTH)

    ! FIXME: use MPI tags instead:
    buf(1) = tag
    call MPI_ISEND(buf, size(buf), MPI_INTEGER4, rank, MSGTAG, comm_world, req, ierr)
    ASSERT(ierr==MPI_SUCCESS)
  end subroutine isend

#ifdef DLB_MASTER_SERVER
  pure function divide_work(jobs, np) result(n)
    ! Purpose: give back number of jobs to take,
    !          as all jobs to share are located at master give back
    !          portion of division by the number of jobs
    implicit none
    integer(i4_kind), intent(in) :: jobs(2)
    integer(i4_kind), intent(in) :: np
    integer(i4_kind)             :: n ! result
    !** End of interface *****************************************

    ! exponential decrease (?)
    n =  (jobs(2) - jobs(1)) / np
    if (n > (jobs(2) - jobs(1))) n = 0
  end function divide_work

  function distribute_jobs(N, n_procs, my_rank) result(my_jobs)
    !  Purpose: given the number of jobs alltogether, decides how many
    !           will be done on each proc and where is start and endpoint
    !           in the global job range, this will be fed directly in
    !           the dlb_setup of the dlb routine
    !           masterserver variant, 70% are distributed beforehand
    !           the rest is on the master
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: N, n_procs, my_rank
    integer(kind=i4_kind)                :: my_jobs(L_JOB)
    !** End of interface *****************************************

    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind) :: jobs_per_proc, rest

    jobs_per_proc = N / n_procs * 7 / 10
    my_jobs(JLEFT) = jobs_per_proc * my_rank
    ! if it is not dividable, distribute the rest
    ! this will shift the start point of each (but the first) proc
    my_jobs(JRIGHT) = my_jobs(JLEFT) + jobs_per_proc
    if (my_rank == termination_master) then
      my_jobs(JRIGHT) = N
    endif
  end function distribute_jobs
#else
  pure function divide_work(jobs, np) result(n)
    ! Purpose: give back number of jobs to take, half what is there
    !          take less than half if could not equally divided
    implicit none
    integer(i4_kind), intent(in) :: jobs(2)
    integer(i4_kind), intent(in) :: np ! unused
    integer(i4_kind)             :: n ! result
    !** End of interface *****************************************

    ! give half of all jobs:
    n =  (jobs(2) - jobs(1)) / 2
    n = max(n, 0)
  end function divide_work

  function distribute_jobs(N, n_procs, my_rank) result(my_jobs)
    !  Purpose: given the number of jobs alltogether, decides how many
    !           will be done on each proc and where is start and endpoint
    !           in the global job range, this will be fed directly in
    !           the dlb_setup of the dlb routine
    !
    !           The last processes will get fewer jobs, if it is not
    !           dividable equally.
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: N, n_procs, my_rank
    integer(kind=i4_kind)                :: my_jobs(L_JOB)
    !** End of interface *****************************************

    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind) :: jobs_per_proc
    integer(kind=i4_kind) :: rest

    ! jobs_per_proc = minimum number of jobs per processes
    ! first rest jobs will get one more job
    jobs_per_proc = N / n_procs
    ! N = jobs_per_proc * n_procs + rest; rest < n_procs
    rest = N - jobs_per_proc * n_procs

    if (my_rank == termination_master .and. 1 < output_border) then
        print *, "Distributing of jobs on the processors"
        print *, "There are ", N, " jobs altogether"
        print *, "each processor will get approximatly", jobs_per_proc
    endif

    ! Starting point of own interval:
    my_jobs(JLEFT) = jobs_per_proc * my_rank + min(rest, my_rank)

    ! if this proc has to do one more job tell him so
    ! changes job_per_proc from minimum value
    !to jobs_per_proc for me
    if (my_rank < rest) jobs_per_proc = jobs_per_proc + 1

    my_jobs(JRIGHT) = my_jobs(JLEFT) + jobs_per_proc
  end function distribute_jobs
#endif

end module dlb_common
