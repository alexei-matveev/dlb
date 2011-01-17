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

  public :: reserve_workm!(m, jobs) -> integer n
  public :: divide_work!(jobs) -> integer n
  public :: divide_work_master!(m, jobs) -> integer n
  public :: steal_work_for_rma!(m, jobs) -> integer n
  public :: split_at! (C, AB, AC, CB)

  public :: irand!(long int) -> long int

  public :: time_stamp_prefix
  public :: time_stamp
  public :: dlb_common_init, dlb_common_finalize, dlb_common_setup
  public :: add_request, test_requests, end_requests
  public :: end_communication
  public :: send_resp_done, report_job_done, send_termination, has_last_done
  public :: set_start_job, set_empty_job
  public :: decrease_resp
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
  integer(kind=i4_kind), public, protected ::  termination_master ! the one who gathers the finished my_resp's
                                         ! and who tells all, when it is complety finished
                                         ! in case of the variant with master as server of jobs, its also the master

  !================================================================
  ! End of public interface of module
  !================================================================

  integer(kind=i4_kind), allocatable :: my_resp(:)
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

    if (allocated(all_done)) all_done  = .false.

    ! if there is any exchange of jobs, the following things are needed
    ! (they will help to get the DONE_JOB messages on their way)
    allocate(messages(JLENGTH + 1, n_procs), stat = alloc_stat)
    ASSERT(alloc_stat==0)

    allocate(message_on_way(n_procs), stat = alloc_stat)
    ASSERT(alloc_stat==0)

    allocate(req_dj(n_procs), stat = alloc_stat)
    ASSERT(alloc_stat==0)

    allocate(my_resp(n_procs), stat = alloc_stat)
    ASSERT(alloc_stat==0)

    ! Here they are initalizied, at the beginning none message has been
    ! put on its way, from the messages we know everything except the
    ! second entry which will be the number of jobs done
    messages = 0
    messages(1,:) = DONE_JOB
    message_on_way = .false.
    my_resp = 0
    my_resp(my_rank + 1) = resp
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
      len_req = size(requ,1)
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
    deallocate(req_dj, my_resp, stat=alloc_stat)
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
    message_s = 0
    message_s(1) = WORK_DONAT

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
           call MPI_ISEND(message_s, 1+JLENGTH, MPI_INTEGER4, stat(MPI_SOURCE), MSGTAG, comm_world, req, ierr)
           ASSERT(ierr==MPI_SUCCESS)
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
    !------------ Modules used ------------------- ---------------
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: i, j
    integer(kind=i4_kind)                :: stolen_jobs(n_procs-1)
    !------------ Executable code --------------------------------
    j = 1
    do i = 1, n_procs
        if ( i /= my_rank+1) then
            stolen_jobs(j) = messages(2, i)
            j = j + 1
        endif
    enddo
    print *,  my_rank, "stole jobs from the others", stolen_jobs
    print *, my_rank, "got jobs stolen:", my_resp(my_rank + 1)
  end subroutine print_statistics

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
    my_resp(source+1) = my_resp(source+1) - n
    else
    my_resp(source+1) = -n
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
    integer(kind=i4_kind), save          :: message(1+JLENGTH) ! message may only be
     ! changed or rewritten after communication finished, thus it is saved here in order
     ! to still be present when the subroutine finishes
     ! as this routine is only called once each process in each dlb call
     ! it will not be overwritten too sone
    !------------ Executable code --------------------------------

    message(1) = RESP_DONE
    message(2) = my_rank
    message(3:) = 0
    call MPI_ISEND(message, 1+JLENGTH, MPI_INTEGER4, termination_master, MSGTAG,comm_world, req, ierr)
    ASSERT(ierr==MPI_SUCCESS)
    call add_request(req, requ)
  end subroutine send_resp_done

  subroutine report_job_done(num_jobs_done, source2)
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
    integer(kind=i4_kind), intent(in  ) :: num_jobs_done, source2
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, stat(MPI_STATUS_SIZE)
    integer(kind=i4_kind)                :: source
    !------------ Executable code --------------------------------
    if (num_jobs_done < 1) RETURN

    source = source2 + 1
    ! We need the messages and req_dj entry of source in any case
    ! thus if there is still a message pending delete it
    ! by sending the sum of all jobs done so far, it is not
    ! of interest any more if the previous message has arrived
    if (message_on_way(source)) then
        call MPI_CANCEL(req_dj(source), ierr)
        ASSERT(ierr==MPI_SUCCESS)
        call MPI_WAIT(req_dj(source),stat, ierr)
        ASSERT(ierr==MPI_SUCCESS)
    endif
    messages(1, source) = DONE_JOB
    messages(3:, source) = 0
    messages(2, source) = messages(2, source) +  num_jobs_done
    call time_stamp("send message to source", 2)
    call MPI_ISEND(messages(:,source),1 + JLENGTH, MPI_INTEGER4, source2,&
                                MSGTAG,comm_world, req_dj(source), ierr)
    ASSERT(ierr==MPI_SUCCESS)
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
    integer(kind=i4_kind)                :: receiver, message(1+JLENGTH)
    !------------ Executable code --------------------------------

    allocate(request(n_procs -1), stats(n_procs -1, MPI_STATUS_SIZE),&
    stat = alloc_stat)
    ASSERT(alloc_stat==0)
    message(1) = NO_WORK_LEFT
    message(2:) = 0
    do i = 0, n_procs-2
    receiver = i
    ! skip the termination master (itsself)`
    if (i >= termination_master) receiver = i+1
    call time_stamp("send termination", 5)
    call MPI_ISEND(message, 1+JLENGTH, MPI_INTEGER4, receiver, MSGTAG, comm_world ,request(i+1), ierr)
    ASSERT(ierr==MPI_SUCCESS)
    enddo
    call MPI_WAITALL(size(request), request, stats, ierr)
    ASSERT(ierr==MPI_SUCCESS)
  end subroutine send_termination

#ifdef DLB_MASTER_SERVER
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

    if (my_rank == termination_master) then
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
