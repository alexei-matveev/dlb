!
! Copyright (c) 2010-2014 Astrid Nikodem, Alexei Matveev
!
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
  USE_MPI!, only: MPI_STATUS_SIZE, MPI_SUCCESS, MPI_COMM_NULL, &
       !MPI_DOUBLE_PRECISION, MPI_WTIME, MPI_DATATYPE_NULL
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

  public :: irand ! (long int) -> long int, FIXME: used in test.f90

  public :: time_stamp
  public :: dlb_common_init, dlb_common_finalize, dlb_common_setup
  public :: add_request, test_requests, end_requests
  public :: end_communication
  public :: send_resp_done, send_termination, has_last_done
  public :: set_start_job, set_empty_job

  public :: report_to!(owner, n), with n being an incremental report
  public :: report_by!(source, n), with n being a cumulative report
  public :: reports_pending!() -> integer

  public :: isend!(buf, rank, tag, req)
  public :: iprobe!(src, tag, stat) -> ok
  public :: recv!(buf, rank, tag, stat)

  public :: print_statistics
  public :: dlb_timers

  ! For those integers which will  stay 4 bytes no matter what changes
  ! around  (for example  for statistics).   One is  better  off using
  ! default  integers here as  this is  what MPI  likely uses  for its
  ! object handlers and counts. FIXME: maybe use kind(0) instead?
  integer, parameter, public :: ik = selected_int_kind (9)

  ! Integer with 8 bytes, or rather with range of 18 decimal digits:
  integer, parameter, public :: i8_kind = selected_int_kind(18)

  ! For job IDs, the actual data that DLB serves, use integers with at
  ! least that many decimal digits:
  integer, parameter, private :: kind_of_lk = 18
  integer, parameter, public :: lk = selected_int_kind (kind_of_lk)

  ! MPI has its own convention for  types, it needs its own version of
  ! the  integer  to  send.  This   will  be  set  to  something  more
  ! meaningfull in dlb_common_init():
  integer, public, protected :: lk_mpi = MPI_DATATYPE_NULL


  integer,  public, protected :: comm_world

  integer (ik), parameter, public  :: OUTPUT_BORDER = FPP_OUTPUT_BORDER

  integer (ik), parameter, public  :: DONE_JOB = 1, NO_WORK_LEFT = 2, RESP_DONE = 3 ! message tags
  integer (ik), parameter, public  :: WORK_REQUEST = 4, WORK_DONAT = 5 ! messages for work request
  integer (ik), parameter, public  :: JLENGTH = 3 ! Length of a single job in interface
  integer (ik), parameter, public  :: L_JOB = 3  ! Length of job to give back from inner interface (dlb_imp)
  integer (ik), parameter, public  :: JOWNER = 3 ! Number in job, where rank of origin proc is stored
  integer (ik), parameter, public  :: JLEFT = 1 ! Number in job, where stp (start point) is stored
  integer (ik), parameter, public  :: JRIGHT = 2 ! Number in job, where ep (end point) is stored

  ! Variables need for debug and efficiency testing
  ! They are the timers, needed in all variants, in multithreaded variants only
  ! MAIN is allowed to acces them
  double precision, public  :: main_wait_all, main_wait_max, main_wait_last
  double precision, public  :: max_work, last_work, average_work, num_jobs
  double precision, public  :: dlb_time, min_work, second_last_work
  double precision, public  :: timer_give_more, timer_give_more_last

  integer (ik), public, protected :: my_rank, n_procs ! some synonyms, They will be initialized
                                        !once and afterwards be read only

  !
  ! Termination master is the  process that gathers completion reports
  ! and  tells everyone to terminate.
  !
  integer (ik), public, protected :: termination_master

  !================================================================
  ! End of public interface of module
  !================================================================

  !
  ! This scalar  holds the  number of jobs  initially assigned  to the
  ! process owning this variable.  FIXME: make it an array (1:n_procs)
  ! to keep  full info about initial assignment.  Currently not doable
  ! as  the  setup  procedure  is  provided only  one  assignment  (to
  ! myself).
  !
  integer (lk) :: responsibility = -1

  !
  ! This array holds  the counts of jobs that  were initially assigned
  ! to the process owning this structure and later reported as done by
  ! one  of  the workers.   Note  that  sum(reported_by(:))  is to  be
  ! compared  with with  the total  number  of jobs  from the  initial
  ! assignment kept in the variable "responsibility".
  !
  integer (lk), allocatable :: reported_by(:) ! (0:n_procs-1)

  !
  ! The next  array holds  the counts of  jobs that were  delivered to
  ! "userspace" of  the process owning  this structure (aka  jobs that
  ! were "done") and reported to the respective job owner according to
  ! initial assignment.   Note that sum(reported_to(:))  is the number
  ! of jobs "done" by the process owning this structure. We dont abuse
  ! message buffers to keep this data anymore.
  !
  integer (lk), allocatable :: reported_to(:) ! (0:n_procs-1)

  integer (ik), allocatable :: req_dj(:) ! need to store the
                                               ! messages for
                                               ! DONE_JOB,

  ! Need to store the messages for  DONE_JOB, as they may still be not
  ! finished, when the subroutine for generating them is finshed.
  integer (lk), allocatable :: messages(:,:)

  ! There may  be a lot of  them, message_on_way keeps  track, to whom
  ! they  are already  on their  way  the requests  are handeled  also
  ! separately, as it is needed to know, which one has finsished:
  logical, allocatable :: message_on_way(:) ! which messages are
                                            ! already sent

  !------------ Declaration of types ------------------------------

  !------------ Declaration of constants and variables ----

  double precision :: time_offset = -1.0
  logical, allocatable              :: all_done(:)
    ! only allocated on termination_master, stores which procs
    ! jobs are finished. If with masterserver: which proc has terminated
  !----------------------------------------------------------------
  !------------ Subroutines ---------------------------------------
contains

! ONLY FOR DEBBUGING (WITHOUT PARAGAUSS)
  subroutine time_stamp(msg, output_level)
    implicit none
    character(len=*), intent(in) :: msg
    integer (ik), intent(in) :: output_level
    ! *** end of interface ***

    double precision :: time
    character(len=28) :: prefix
    ! ATTENTION: write into character and printing might be not thread save
    ! for all compilers. Thus keep write and print in the if-clause allowing
    ! to get rid of them by setting OUTPUT_BORDER=0.

    time = MPI_Wtime()
    if ( time_offset < 0.0 ) then
      time_offset = time
      if(output_level < OUTPUT_BORDER) then
         write(prefix, '("#", I3, G20.10)') my_rank, time_offset
         print *, prefix, "(BASE TIME)"
      endif
    endif

    if(output_level < OUTPUT_BORDER) then
      write(prefix, '("#", I3, G20.10)') my_rank, time - time_offset

      print *, prefix, msg
    endif
  end subroutine time_stamp
! END ONLY FOR DEBUGGING

  subroutine dlb_timers(output_level)
    ! Purpose: Collects and prints some statistics of how the current dlb
    !          run has been performed
    !          For output_level = 0 exits without doing anything
    !          For output_level = 1 time spend in dlb_give_more (split)
    !          For output_level = 2 + wait for new tasks
    !          For output_level = 3 + last work slice
    !          For output_level = 4 + work length/task sizes + complete program
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer (ik), intent(in)  :: output_level
    !------------ Declaration of local variables -----------------
    double precision, allocatable :: times(:,:), help_arr(:)
    double precision              :: time_singles(12)
    integer (ik)              :: ierr
    ! *** end of interface ***
    ! Return if none output is wanted, then there is also no need to
    ! send the output to the master
    if (output_level == 0) RETURN

    ! Master needs some place to store all results
    if (my_rank == 0) then
        allocate(times(12, n_procs), help_arr(n_procs), stat = ierr )
        ASSERT (ierr == 0)
        times = 0
    else
        ! will not contain any information, but better have it also on the
        ! slaves
        allocate(times(1, 1), stat = ierr )
        ASSERT (ierr == 0)
        times = 0
    endif

    time_singles(1) = timer_give_more
    time_singles(2) = timer_give_more_last
    time_singles(3) = main_wait_all
    time_singles(4) = main_wait_max
    time_singles(5) = main_wait_last
    time_singles(6) = dlb_time
    time_singles(7) = max_work
    time_singles(8) = min_work
    time_singles(9) = average_work
    time_singles(10) = last_work
    time_singles(11) = second_last_work
    time_singles(12) = num_jobs
    ASSERT (size(time_singles) == 12)

    call MPI_GATHER(time_singles, size(time_singles), MPI_DOUBLE_PRECISION,  &
          times, size(time_singles), MPI_DOUBLE_PRECISION, 0, comm_world, ierr)
    ASSERT (ierr == 0)

    if (my_rank == 0) then
        print *, "--DLB-statistics--DLB-statistics--DLB-statistics--DLB-statistics--"
        print *, "Statistics for last DLB loop (times in seconds):"
        print *, "1. Time spend in DLB"
        print *, "  DLB loop time (without terminating) = ", sum(times(1,:))
        print *, "  DLB termination                     = ", minval(times(2,:)) * n_procs
        print *, "  DLB last call - termination         = ", sum(times(2,:)) - n_procs * minval(times(2,:))
        if (output_level > 1) then
            print *, "2. Waiting times for new task"
            print *, "  Maximum wait  min/max               =", minval(times(4,:)), maxval(times(4,:))
            print *, "  Total wait                          =", sum(times(3,:))
            print *, "  Total wait times without last       =", sum(times(3,:)) - sum(times(5,:))
            print *, "  Number of tasks on proc.  min/max   =", minval(times(12,:)) , maxval(times(12,:))
        endif
        if (output_level > 2) then
           print *, "3. Times of last work slices (time between 2 dlb_give_more calls)"
           print *, "  Last work slice min/max             =", minval(times(10,:)) , maxval(times(10,:))
           print *, "  Second last work slice min/max      =", minval(times(11,:)) , maxval(times(11,:))
        endif
        if (output_level > 3) then
           print *, "  Maximum task on proc min/max        =", minval(times(7,:)) , maxval(times(7,:))
           print *, "  Minimal task on proc min/max        =", minval(times(8,:)) , maxval(times(8,:))
           help_arr = times(9,:) / times(12,:)
           print *, "4. Average work load (time between the dlb calls / Number of tasks)"
           print *, "  Average work load all/min/max       =", sum(times(9,:)) / sum(times(12,:)), minval(help_arr) &
                                                           , maxval(help_arr)
           print *, "5. Complete Time (between dlb_setup and termination)"
           print *, "  Complete Time all                   =", sum(times(6,:))
           print *, "  Time for single proc min/max        =", minval(times(6,:)), maxval(times(6,:))
        endif
        print *, "----DLB-statistics-end--DLB-statistics-end--DLB-statistics-end----"
        print *, ""
        if (allocated(times)) then
            deallocate(times, stat = ierr)
            ASSERT (ierr == 0)
        endif
    endif
  end subroutine dlb_timers


  subroutine dlb_common_init(world)
    ! Intialization of common stuff, needed by all routines
    implicit none
    integer (ik) ,  intent(in) :: world
    ! *** end of interface ***

    integer (ik) :: ierr, alloc_stat

    !
    ! Set global communicator as a DUP of the world:
    !
    call MPI_COMM_DUP(world, comm_world, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    call MPI_COMM_RANK(comm_world, my_rank, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    call MPI_COMM_SIZE(comm_world, n_procs, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    !
    ! Create a  new MPI_Datatype, here  lk_mpi. It is not  a kind
    ! (like  lk), it  is not  a  number of  decimal digits  (like
    ! kind_of_lk) but a real  MPI type, represented as an integer
    ! as everything else in Fortran bindings.
    !
    ! FIXME: even with the guard, valgrind reports a memory leak here.
    ! But at least one MPI interpretation says:
    !
    !   "An  MPI_Datatype  returned  by  this  subroutine  is  already
    !    committed.  It cannot be freed with MPI_TYPE_FREE()."
    !
    ! Here kind_of_lk is a  PARAMETER and does not change between
    ! invocations. So do it just once:
    !
    if (lk_mpi == MPI_DATATYPE_NULL) then
       call MPI_TYPE_CREATE_F90_INTEGER (kind_of_lk, lk_mpi, ierr)
       ASSERT(ierr==MPI_SUCCESS)
    endif

    !
    ! Anyone  can  be  the  termination  master. So  choose  the  last
    ! processor  to  be  the  termination  master.   It  should  exist
    ! irrespective  what  the  number  of  processors  is.  The  first
    ! processor is most likely playing the role of master for the real
    ! calculation,  this  way the  additional  load  should be  better
    ! balanced.
    !
    termination_master = n_procs - 1

    !
    ! Only on termination master:
    !
    if (my_rank == termination_master) then
      allocate(all_done(n_procs), stat = alloc_stat)
      ASSERT(alloc_stat==0)
    endif
  end subroutine dlb_common_init

  subroutine dlb_common_finalize()
    ! shut down the common stuff, as needed
    implicit none
    ! *** end of interface ***

    integer (ik) :: ierr, alloc_stat

    !
    ! Only on termination master:
    !
    if (allocated(all_done)) then
      deallocate(all_done, stat=alloc_stat)
      ASSERT(alloc_stat==0)
    endif

    call MPI_COMM_FREE(comm_world, ierr)
    ASSERT(ierr==MPI_SUCCESS)
    ASSERT(comm_world==MPI_COMM_NULL)
  end subroutine dlb_common_finalize

  subroutine dlb_common_setup(resp)
    ! Termination master start of a new dlb run
    implicit none
    integer (lk), intent(in   ) :: resp
    ! *** end of interface ***

    integer ::  alloc_stat

    !
    ! Only on termination master:
    !
    if (allocated(all_done)) all_done  = .false.

    ! if there is any exchange of jobs, the following things are needed
    ! (they will help to get the DONE_JOB messages on their way)
    allocate(messages(JLENGTH, n_procs), stat = alloc_stat)
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
    messages(:, :) = 0
    message_on_way = .false.

    !initalize some timers for debug output, can be accessed via output level
    ! directly from the processors or via dlb_print_statistics
    main_wait_all = 0
    timer_give_more = 0
    timer_give_more_last = 0
    main_wait_max = 0
    main_wait_last = 0
    max_work = 0
    min_work = huge(min_work)
    last_work = 0
    second_last_work = 0
    average_work = 0
    num_jobs = 0
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
    integer (lk), intent(in) :: job(L_JOB)
    integer (lk)             :: job_data(JLENGTH)
    ! *** end of interface ***

    job_data(:L_JOB) = job
    ASSERT (job_data(JOWNER) == my_rank)
  end function set_start_job

  function set_empty_job() result(job_data)
    !Purpose: gives a complete starting job description
    !         after gotten just the job range
    implicit none
    integer (lk) :: job_data(JLENGTH)
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
     integer (ik), intent(in) :: rank, np
     integer (ik)             :: victim
     ! *** end of interface ***

     victim = select_victim_random(rank, np)
  end function select_victim

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
     integer (ik), intent(in) :: rank, np
     integer (ik)             :: victim
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
    ! use dlb_common, only: lk, JLENGTH, split_at
    implicit none
    integer (lk), intent(in)  :: m
    integer (lk), intent(in)  :: remote(:) ! (JLENGTH)
    integer (lk), intent(out) :: remaining(:) ! (JLENGTH)
    integer (lk), intent(out) :: stolen(:) ! (JLENGTH)
    logical                       :: ok ! result
    ! *** end of interface ***

    integer (lk) :: work, c

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
    ! use dlb_common, only: lk, JLENGTH, split_at
    implicit none
    integer (lk), intent(in)  :: m
    integer (lk), intent(in)  :: local(:) ! (JLENGTH)
    integer (lk), intent(out) :: remaining(:) ! (JLENGTH)
    integer (lk), intent(out) :: stolen(:) ! (JLENGTH)
    logical                       :: ok ! result
    ! *** end of interface ***

    integer (lk) :: work, c

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
    integer (lk), intent(in) :: jobs(:)
    ! *** end of interface ***

    empty = length(jobs) == 0
  end function empty

  function length(jobs) result(n)
    implicit none
    integer (lk), intent(in) :: jobs(:)
    integer (lk)             :: n ! result
    ! *** end of interface ***

    ASSERT(size(jobs)==JLENGTH)

    n = max(jobs(JRIGHT) - jobs(JLEFT), 0)
  end function length

  subroutine split_at(C, AB, AC, CB)
    !
    ! Split (A, B] into (A, C] and (C, B]
    !
    implicit none
    integer (lk), intent(in)  :: C, AB(:)
    integer (lk), intent(out) :: AC(:), CB(:)
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

  subroutine add_request (req, requ)
    !
    ! Stores  unfinished   requests.   Grow  requ(:)   by  one  entry,
    ! allocating if it is not allocated, and append req.
    !
    ! Context: control thread, mailbox thread.
    !          2 threads: secretary thread
    !
    ! Locks: none.
    !
    implicit none
    integer (ik), intent (in) :: req
    integer (ik), allocatable, intent (inout) :: requ(:)
    ! *** end of interface ***

    integer (ik), allocatable :: req_int(:)
    integer (lk) :: len_req
    integer (ik) :: alloc_stat

    if (allocated (requ)) then
       ! Can it be zero?
       len_req = size (requ)
       call move_alloc (requ, req_int)
    else
       len_req = 0
    endif

    allocate (requ(len_req + 1), stat=alloc_stat)
    ASSERT (alloc_stat==0)

    if (allocated (req_int)) then
       requ(:len_req) = req_int
    endif
    requ(len_req + 1) = req
  end subroutine add_request

  subroutine test_requests (requ)
    !
    ! Tests if any of the  messages stored in requ have been received,
    ! than remove  the corresponding request requ is  local request of
    ! the corresponding thread.
    !
    ! Context for 3 Threads: mailbox thread, control thread.
    !             2 Threads: secretary thread
    !
    !
    implicit none
    integer (ik), allocatable, intent (inout) :: requ(:)
    !** End of interface *****************************************

    integer (lk) :: i, len_new
    integer (ik) :: alloc_stat, ierr, stat(MPI_STATUS_SIZE)
    integer (ik), allocatable :: requ_int(:)
    logical :: flag
    logical, allocatable :: finished(:)

    ! FIXME: no  requests should rather correspod to  0-size! Also, at
    ! the bottom we  have a check that is unrelated  to requ(:). It is
    ! not obvious why  that check depends on the  allocation status of
    ! requ(:).
    if (.not. allocated (requ)) RETURN

    allocate (finished(size (requ)), stat=alloc_stat)
    ASSERT (alloc_stat==0)

    do i = 1, size (requ)
      call MPI_TEST (requ(i), finished(i), stat, ierr)
      ASSERT (ierr==MPI_SUCCESS)
    enddo

    call move_alloc (requ, requ_int)

    len_new = count (.not. finished)

    ! FIXME: zero is a valid array size:
    if (len_new > 0) then
      allocate (requ(len_new), stat=alloc_stat)
      ASSERT (alloc_stat==0)

      requ = pack (requ_int, mask=(.not. finished))
    endif

    deallocate (requ_int, finished, stat=alloc_stat)
    ASSERT (alloc_stat==0)

    !
    ! This  is unrelated to  requ(:) at  all. Use  the chance  to test
    ! requests in the module global variable req_dj(:)
    !
    do i = 1, size (message_on_way) ! messages for DONE_JOBS
       ! Have to be handled separatly, as the messages have to be kept
       ! and may  not be changed till  the request has  been sent. But
       ! here  also  some  messages  may be  finished,  message_on_way
       ! stores informations to whom  there are still some messages of
       ! DONE_JOBS on their way.
       if (message_on_way(i)) then
          call MPI_TEST (req_dj(i), flag, stat, ierr)
          ASSERT (ierr==MPI_SUCCESS)
          message_on_way(i) = .not. flag
       endif
    enddo
  end subroutine test_requests

  subroutine end_requests (requ)
    !
    ! End all of  the stored requests in requ.  At  this point all the
    ! communication  has  to be  ended  already!   After terminated  =
    ! .True. for rma variant, and after clear_up of threads
    !
    ! Context for 3Threads: mailbox thread, control thread.
    !             2 Threads: secretary thread
    !
    implicit none
    integer (ik), allocatable, intent (inout) :: requ(:)
    !** End of interface *****************************************

    integer (lk) :: i
    integer (ik) :: alloc_stat, ierr
    integer (ik) :: stat(MPI_STATUS_SIZE)

    ! FIXME: no requests should rather correspod to 0-size!
    if (.not. allocated (requ)) RETURN

    ! FIXME: use MPI_WAITALL() instead:
    do i = 1, size (requ)
       call MPI_WAIT (requ(i), stat, ierr)
       ASSERT (ierr==MPI_SUCCESS)
    enddo
    deallocate (requ, stat=alloc_stat)
    ASSERT (alloc_stat==0)
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
    integer (lk)                :: i
    integer (ik)              :: alloc_stat, ierr
    integer (ik)              :: stat(MPI_STATUS_SIZE)
    !------------ Executable code --------------------------------
    do i = 1, size(message_on_way)
      if (message_on_way(i)) then
        ! the messages DONE_JOB should be all finshed already
        call MPI_WAIT(req_dj(i),stat, ierr)
        ASSERT(ierr==MPI_SUCCESS)
      endif
    enddo
    ! these variables will only be needed after the next dlb-setup
    if (allocated(messages)) then
        deallocate(messages, stat=alloc_stat)
        ASSERT(alloc_stat==0)
    endif
    if (allocated(message_on_way)) then
        deallocate(message_on_way, stat=alloc_stat)
        ASSERT(alloc_stat==0)
    endif
    if (allocated(req_dj)) then
        deallocate(req_dj, stat=alloc_stat)
        ASSERT(alloc_stat==0)
    endif
    if (allocated(reported_by)) then
        deallocate( reported_by, stat=alloc_stat)
        ASSERT(alloc_stat==0)
    endif
    if (allocated(reported_to)) then
        deallocate( reported_to, stat=alloc_stat)
        ASSERT(alloc_stat==0)
    endif
  end subroutine end_communication

  subroutine print_statistics()
    ! Purpose: end all of the stored requests in requ
    !          no matter if the corresponding message has arived
    !
    ! Context for 3Threads: mailbox thread, control thread.
    !             2 Threads: secretary thread
    implicit none
    !** End of interface *****************************************

    if (1 < OUTPUT_BORDER) then
        ! executed by me, sorted by owner:
        write(*, '("[", I3, "] REPORTED_TO =", 128I4)') my_rank, reported_to

        ! assigned to me, sorted by execution host:
        write(*, '("[", I3, "] REPORTED_BY =", 128I4)') my_rank, reported_by
    endif

    !
    ! FIXME: why cannot I use collective communications here?
    !        Tried MPI_BARRIER for pretty printing.
    !
  end subroutine print_statistics

  subroutine report_by(source, n)
    !
    ! Handles  arriving  scheduling  reports.   NOTE: "n"  is  NOT  an
    ! increment,  but a  cumulative report  of the  job count  from my
    ! initial assignment scheduled by the source.
    !
    implicit none
    integer (ik), intent(in) :: source
    integer (lk), intent(in)   :: n
    ! *** end of interface ***

    reported_by(source) = n
    ! ... yes, this array was allocated as base-0.
  end subroutine report_by

  function reports_pending() result(pending)
    !
    ! How  many jobs  from my  initial  assignment have  not yet  been
    ! reported as scheduled.
    !
    implicit none
    integer (lk) :: pending
    ! *** end of interface ***

    ASSERT(allocated(reported_by))
    ! I am not  sure if sum works for interger  (and not only "normal"
    ! integer). Of course it does:
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
    ! Termination master will collect all the finished resps
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
    integer (ik), allocatable :: requ(:)
    !** End of interface *****************************************

    !------------ Declaration of local variables -----------------
    integer (ik)              :: req
    integer (lk), save          :: message(JLENGTH) ! message may only be
     ! changed or rewritten after communication finished, thus it is saved here in order
     ! to still be present when the subroutine finishes
     ! as this routine is only called once each process in each dlb call
     ! it will not be overwritten too soon
    !------------ Executable code --------------------------------

    ! FIXME: maybe use MPI_SOURCE status field on receiving side?
    message(:) = 0
    message(1) = my_rank

    call isend(message, termination_master, RESP_DONE, req)

    call add_request(req, requ)
  end subroutine send_resp_done

  subroutine report_to(owner, num_jobs_done)
    !
    ! Handles reporting scheduled jobs, expects an increment and an
    ! owner rank.
    !
    ! Needed for termination algorithm, there are two cases, it was a
    ! job of the own responsibilty or one from another, first case
    ! just change my number second case, send a cumulative report on
    ! how many of his jobs were scheduled.
    !
    ! Context: control thread.
    !             2 Threads: secretary thread
    !
    implicit none
    integer (lk), intent(in) :: num_jobs_done
    integer (ik), intent(in) :: owner
    !** End of interface *****************************************

    integer (ik) :: ierr, stat(MPI_STATUS_SIZE)
    integer (ik) :: owner1 ! == owner + 1

    ! FIXME: for compatibility reasons:
    if ( num_jobs_done == 0 ) then
        ! print *, "report_to: why reporting nothing?"
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
        messages(1, owner1) = reported_to(owner)

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
    integer (ik), intent(in)    :: proc
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
    !
    implicit none
    !** End of interface *****************************************

    integer (ik) :: ierr
    integer (ik)   :: i
    integer (ik)   :: receiver
    integer (lk)   :: message(JLENGTH)
    integer (ik) :: request(n_procs - 1)
    integer (ik) :: stat(MPI_STATUS_SIZE)
    ASSERT(my_rank==termination_master)

    call time_stamp("send termination", 5)

    ! FIXME: reusing the same buffer for non-blocking sends:
    message(:) = 0
    do i = 0, n_procs-2
        receiver = i
        ! skip the termination master itself:
        if (i >= my_rank) receiver = i+1

        ! FIXME: the tag is the only useful info sent:
        call isend(message, receiver, NO_WORK_LEFT, request(i+1))
    enddo
    !
    ! FIXME: the routine MPI_WAITALL()  will have problems if used for
    ! requests  other than  normal integer.   Therefore the  call call
    ! MPI_WAITALL(size(request),  request,  MPI_STATUSES_IGNORE, ierr)
    ! does not work for other than lk = integer(4). One is better
    ! off using default  integers as this is what  MPI likely uses for
    ! its object handlers and counts!
    !
    ASSERT(kind(request)==kind(1))
    do i = 1, n_procs-1
        call MPI_WAIT (request(i), stat, ierr)
        ASSERT(ierr==MPI_SUCCESS)
    enddo
  end subroutine send_termination

  subroutine isend(buf, rank, tag, req)
    !
    ! Convenience wrapper around MPI_ISEND
    !
    implicit none
    integer (lk), intent(inout), target :: buf(:) ! (JLENGTH)
    integer (ik), intent(in)          :: rank, tag
    integer (ik), intent(out)         :: req
    ! *** end of interface ***

    integer (ik) :: ierr

    ASSERT(size(buf)==JLENGTH)

    call MPI_ISEND(buf, size(buf), lk_mpi, rank, tag, comm_world, req, ierr)
    ASSERT(ierr==MPI_SUCCESS)
  end subroutine isend

  function iprobe(src, tag, stat) result(ok)
    !
    ! Convenience wrapper around MPI_IPROBE
    !
    implicit none
    integer (ik), intent(in)  :: src, tag
    integer (ik), intent(out) :: stat(MPI_STATUS_SIZE)
    logical                       :: ok
    ! *** end of interface ***

    integer (ik) :: ierr

    call MPI_IPROBE(src, tag, comm_world, ok, stat, ierr)
    ASSERT(ierr==MPI_SUCCESS)
  end function iprobe

  subroutine recv(buf, rank, tag, stat)
    !
    ! Convenience wrapper around MPI_RECV
    !
    implicit none
    integer (lk), intent(out) :: buf(:) ! (1+JLENGTH)
    integer (ik), intent(in)  :: rank, tag
    integer (ik), intent(out) :: stat(MPI_STATUS_SIZE)
    ! *** end of interface ***

    integer (ik) :: ierr

    ASSERT(size(buf)==JLENGTH)

    call MPI_RECV(buf, size(buf), lk_mpi, rank, tag, comm_world, stat, ierr)
    ASSERT(ierr==MPI_SUCCESS)
  end subroutine recv

  pure function divide_work(jobs, np) result(n)
    !
    ! Returns number of jobs to  take (steal?).  Take the half of what
    ! is there.  Take less than half if could not equally divided.
    !
    implicit none
    integer (lk), intent(in) :: jobs(2)
    integer (ik), intent(in) :: np ! unused
    integer (lk) :: n ! result
    !** End of interface *****************************************

    ! give half of all jobs:
    n =  (jobs(2) - jobs(1)) / 2
    n = max(n, 0)
  end function divide_work

  function distribute_jobs(N, procs, rank) result(my_jobs)
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
    integer (lk), intent(in   ) :: N
    integer (ik), intent(in ) :: procs, rank
    integer (lk)                :: my_jobs(L_JOB)
    !** End of interface *****************************************

    !------------ Declaration of local variables -----------------
    integer (lk) :: n_procs, my_rank
    integer (lk) :: jobs_per_proc
    integer (lk) :: rest
    n_procs = procs
    my_rank = rank

    ! jobs_per_proc = minimum number of jobs per processes
    ! first rest jobs will get one more job
    jobs_per_proc = N / n_procs
    ! N = jobs_per_proc * n_procs + rest; rest < n_procs
    rest = N - jobs_per_proc * n_procs

    if (my_rank == termination_master .and. 1 < OUTPUT_BORDER) then
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
    my_jobs(JOWNER) = my_rank
  end function distribute_jobs

end module dlb_common
