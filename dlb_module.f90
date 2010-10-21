!===============================================================
! Public interface of module
!===============================================================
module dlb_module
  !---------------------------------------------------------------
  !
  !  Purpose: takes care about dynamical load balancing,
  !           uses an RMA (MPI) object to store the job informations
  !           (only the number of the job) and allows other routines 
  !           to steal them, if they have no own left
  !           unfortunatelly the MPI one-sided works different on different
  !           systems, this routine is only useful if the RMA can be accessed
  !           while the target proc is not in MPI-context
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
  !          the algorithm: it follows in principle the Algorithm 2 from the
  !                         first reference, each proc has an local RMA memory containing
  !                         job informations, it takes jobs form the left (stp values
  !                         are increased, till stp == ep) then it searches the
  !                         job storages of the other procs for jobs left, this is done
  !                         by a code similar to the one descriebed in the second reference,
  !                         but here on each proc and without specifieng the ranges to work on
  !                         stolen work is put in the own storage, after taking the current job
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
  !  Module called by: ...
  !
  !
  !  References: "Scalable Work Stealing", James Dinan, D. Brian Larkins,
  !              Sriram Krishnamoorthy, Jarek Nieplocha, P. Sadayappan, 
  !             Proc. of 21st intl. Conference on Supercomputing (SC). 
  !             Portland, OR, Nov. 14-20, 2009  (for work stealing algorithm)
  !             "Implementing Byte-Range Locks Using MPI One-Sided Communication",
  !             Rajeev Thakur, Robert Ross, and Robert Latham
  !             (in EuroPVM/MPI)  (read modify write algorithm with the same ideas)
  ! 
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
  use iso_c_binding
  !use type_module ! type specification parameters
  implicit none
  save            ! save all variables defined in this module
  private         ! by default, all names are private
  !== Interrupt end of public interface of module =================

  !------------ public functions and subroutines ------------------
  public dlb_init, dlb_finalize, dlb_setup, dlb_give_more

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

  !------------ Declaration of constants and variables ----
  integer(kind=i4_kind), parameter  :: DONE_JOB = 1, NO_WORK_LEFT = 2, RESP_DONE = 3 !for distingishuing the messages
  integer(kind=i4_kind), parameter  :: ON = 1, OFF = 0 ! For read-modify-write
  integer(kind=i4_kind), parameter  :: SJOB_LEN = 3 ! Length of a single job in interface
  integer(kind=i4_kind), parameter  :: L_JOB = 2  ! Length of job to give back from interface
  integer(kind=i4_kind), parameter  :: NRANK = 3 ! Number in job, where rank of origin proc is stored
  integer(kind=i4_kind), parameter  :: J_STP = 1 ! Number in job, where stp (start point) is stored
  integer(kind=i4_kind), parameter  :: J_EP = 2 ! Number in job, where ep (end point) is stored
  integer(kind=i4_kind)             :: jobs_len  ! Length of complete jobs storage
  integer(kind=i4_kind)            :: my_rank, n_procs ! some synonyms
  integer(kind=i4_kind)             ::  termination_master ! the one who gathers the finished my_resp's
                                         ! and who tells all, when it is complety finished
  integer(kind=i4_kind)            :: win ! for the RMA object
  type(c_ptr)  :: c_job_pointer     ! needed for MPI RMA handling (for c pseudo pointer)
  integer(kind=i4_kind), pointer    :: job_storage(:) ! store all the jobs, belonging to this processor
  integer(kind=i4_kind)             :: start_job(SJOB_LEN) ! job_storage is changed a lot, backup for
                                     ! finding out, if someone has stolen something, or how many jobs one
                                     ! has done, when job_storage hold no more jobs
  integer(kind=i4_kind)             :: my_resp ! number of jobs, this processor is responsible for (given
                                              ! in setup
  logical                           :: had_thief ! only check for messages if there is realy a change that there are some
  logical                           :: terminated!, finishedjob ! for termination algorithm
  integer(kind=i4_kind),allocatable :: requ2(:) ! requests of send are stored till relaese
  integer(kind=i4_kind)             :: requ1 ! this request will be used in any case
  logical, allocatable              :: all_done(:) ! only valid on termination_master, stores which proc's
                                                   ! jobs are finished: ATTENTION: NOT which proc is finished

  double precision :: time_offset = -1.0
  !----------------------------------------------------------------
  !------------ Subroutines ---------------------------------------
contains

! ONLY FOR DEBBUGING (WITHOUT PARAGAUSS)
  subroutine show()
    implicit none

    call show1(job_storage)
  end subroutine show

  subroutine time_stamp(msg)
    implicit none
    character(len=*), intent(in) :: msg
    ! *** end of interface ***

    double precision :: time

    time = MPI_Wtime()
    if( time_offset < 0.0 ) time_offset = time
    time = time - time_offset

    print *, "TIMESTAMP:", my_rank, time, msg
  end subroutine time_stamp

  subroutine show1(storage)
    implicit none
    integer(i4_kind), intent(in) :: storage(:)

    print *, "storage=", storage
  end subroutine show1

  logical function assert_n(exp,num)
    implicit none
    logical, intent(in) :: exp
    integer, intent(in) :: num
    if (.not. exp) then
       print *, "ASSERT FAILED", num
       call abort
    endif
    assert_n = .false.
  end function assert_n

! END ONLY FOR DEBUGGING

  !*************************************************************
  subroutine dlb_init()
    !  Purpose: Initialization of the objects needed for running the dlb
    !           job scheduling, the most part is of setting up an RMA
    !           object with MPI to have it ready to use for the rest of the
    !           dlb model. It may be set up only once, even if there are
    !           several dlb's wanted, the initalizing of a specific run
    !           should be done with dlb_setup
    !           It is also recommended to call this subroutine only once
    !           as it needs parallelization of all processes
    !------------ Modules used ------------------- ---------------
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, sizeofint, v, i
    integer(kind=MPI_ADDRESS_KIND)       :: size_alloc, alloc_stat
    !------------ Executable code --------------------------------
!   print *, "dlb_init: entered"
    ! some aliases, highly in use during the whole module
    call MPI_COMM_RANK( comm_world, my_rank, ierr )
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 1)
    call MPI_COMM_SIZE(comm_world, n_procs, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 2)
    allocate(all_done(n_procs), stat = alloc_stat)
    !ASSERT(alloc_stat==0)
    call assert_n(alloc_stat==0, 1)

    termination_master = n_procs - 1
    ! find out, how much there is to store and allocate (with MPI) the memory
    call MPI_TYPE_EXTENT(MPI_INTEGER4, sizeofint, ierr)
    jobs_len = SJOB_LEN + n_procs
    size_alloc = jobs_len * sizeofint
!   print *, "sizeofint=", sizeofint
    call MPI_ALLOC_MEM(size_alloc, MPI_INFO_NULL, c_job_pointer, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 3)
    ! this connects a c'like pointer with our job_storage, as MPI can not handle our
    ! pointers as they are
    call c_f_pointer(c_job_pointer, job_storage, [jobs_len])
!   call show()
    ! win (an integer) is set up, so that the RMA-processes can call on it
    ! they will then get acces to the local stored job_storage of the corresponding proc
    call MPI_WIN_CREATE(job_storage, jobs_len * sizeofint, sizeofint, MPI_INFO_NULL, &
                        comm_world, win, ierr)
!   print *, "win=", win
    call assert_n(ierr==MPI_SUCCESS, 4)
    !ASSERT(ierr==MPI_SUCCESS)
    ! needed to make certain, that in the next steps, all the procs have all the informations
    call MPI_WIN_FENCE(0, win, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    ! put the complete storage content to 0
    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, my_rank, 0, win, ierr)
    job_storage = 0
    call MPI_WIN_UNLOCK(my_rank, win, ierr)
    call show()
!   print *, "dlb_init: exit"
  end subroutine dlb_init
  !*************************************************************
  subroutine dlb_finalize()
    !  Purpose: shuts down the dlb objects (especially job_storage)
    !  when it is not further needed, should be called, after ALL
    !  dlb runs have complete
    !------------ Modules used ------------------- ---------------
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, alloc_stat
    !------------ Executable code --------------------------------

    call MPI_WIN_FENCE(0, win, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    CALL MPI_FREE_MEM(job_storage, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 5)
    call MPI_WIN_FENCE(0, win, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    CALL MPI_WIN_FREE(win, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 5)
    deallocate(all_done, stat=alloc_stat)
    !ASSERT(alloc_stat==0)
    call assert_n(alloc_stat==0, 1)
  end subroutine dlb_finalize
  !*************************************************************
  subroutine dlb_give_more(n, my_job)
    !  Purpose: Returns next bunch of up to n jobs, if jobs(J_EP)<=
    !  jobs(J_STP) there are no more jobs there, else returns the jobs
    !  done by the procs should be jobs(J_STP) + 1 to jobs(J_EP) in
    !  the related job list
    !  first the jobs are tried to get from the local storage of the
    !  current proc, if there are no more, it will try to steal some
    !  more work from others, till the separate termination algorithm
    !  states, that everything is done
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: n
    integer(kind=i4_kind), intent(out  ) :: my_job(L_JOB)
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, v
    logical                              :: term
    integer(i4_kind), target             :: jobs(SJOB_LEN)
!   !------------ Executable code --------------------------------
    ! check for (termination) messages, if: have to gather the my_resp = 0
    ! messages (termination Master) or if someone has stolen
    ! and my report back
    term = .false.
    if(had_thief .or. (my_rank==termination_master)) term = check_messages()
    if (term) then
      print *, my_rank,"RETURNED after gotten message for termination"
      my_job = start_job(:L_JOB)
      my_job(J_STP) = my_job(J_EP)
      return
    endif
    ! First try to get job from local storage (loop because some one lese may
    ! try to read from there (then local_tgetm = false)
    do while (.not. local_tgetm(n, jobs))
       print *, "Waiting", my_rank, "for time to acces my own memory"
       call time_stamp("waiting")
    enddo
    call time_stamp("finished")
    print *, my_rank, "Finished local search"
    ! if local storage only gives empty job:
    do while ((jobs(J_STP) >= jobs(J_EP)) .and. .not. check_messages())
      ! check like above but also for termination message from termination master
      print *, my_rank, "Started search others"
      v = select_victim()
      print *, my_rank, "Victim", v
      ! try to get job from v, if v's memory occupied by another or contains
      ! nothing to steal, job is still empty
      term = rmw_tgetm(n, v, jobs)
       call time_stamp("waiting")
    enddo
    call time_stamp("finished")
    print *, my_rank,"GOT JOB: its", jobs
    ! only the start and endpoint of job slice is needed external
    my_job = jobs(:L_JOB)
    ! this would not work: no ensurance that the others already know that
    ! they should be terminated, they could steal jobs setup by already terminated
    ! processors
    !if (terminated) call dlb_setup(setup_jobs)
    print *, "dlb_give_more: exit", my_rank
  end subroutine dlb_give_more
  !*************************************************************
  logical function check_messages()
    !  Purpose: checks if any message has arrived, checks for messages:
    !          Someone finished stolen job slice
    !          Someone has finished its responsibilty (only termination_master)
    !          There are no more jobs (message from termination_master to finish)
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, stat(MPI_STATUS_SIZE), alloc_stat
    logical                              :: flag
    integer(kind=i4_kind)                :: message(2)
    integer(kind=i4_kind), allocatable  :: statusse(:,:)
!   !------------ Executable code --------------------------------
    ! check for any message
    call MPI_IPROBE(MPI_ANY_SOURCE, 185, comm_world,flag, stat, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    if (flag) then !got a message
      print *,"MMMMMMM", my_rank, "got message from", stat(MPI_SOURCE), "with tag", stat(MPI_TAG)
      call MPI_RECV(message, 2, MPI_INTEGER4, MPI_ANY_SOURCE, 185,comm_world, stat,ierr) 
      !ASSERT(ierr==MPI_SUCCESS)
      call assert_n(ierr==MPI_SUCCESS, 4)
      if (message(1) == DONE_JOB) then ! someone finished stolen job slice
         !ASSERT(message>0)
         call assert_n(message(2)>0, 4)
         my_resp = my_resp - message(2)
         call is_my_resp_done()
      elseif (message(1) == RESP_DONE) then ! finished responsibility
         if (my_rank == termination_master) then
           call check_termination(message(2))
         else ! give only warning, some other part of the code my use this message (but SHOULD NOT)
           ! This message makes no sense in this context, thus give warning
           ! and continue (maybe the actual calculation has used it)
           print *, "WARNING: got unexpected message (I'm no termination master",my_rank,"):", message
         endif
      elseif (message(1) == NO_WORK_LEFT) then ! termination message from termination master
         !ASSERT(message(2)==0)
         call assert_n(message(2)==0, 4)
         !ASSERT(stat(MPI_SOURCE)==termination_master)
         call assert_n(stat(MPI_SOURCE)==termination_master, 4)
         terminated = .true.
         ! NOW all my messages HAVE to be complete, so close (without delay)
         call MPI_WAIT(requ1, stat, ierr)
         !ASSERT(ierr==MPI_SUCCESS)
         call assert_n(ierr==MPI_SUCCESS, 4)
         if (allocated(requ2)) then
           allocate(statusse(size(requ2),MPI_STATUS_SIZE), stat = alloc_stat)
           !ASSERT(alloc_stat==0)
           call assert_n(alloc_stat==0, 4)
           call MPI_WAITALL(size(requ2),requ2, statusse, ierr)
           !ASSERT(ierr==MPI_SUCCESS)
           call assert_n(ierr==MPI_SUCCESS, 4)
         endif
      else
        ! This message makes no sense in this context, thus give warning
        ! and continue (maybe the actual calculation has used it)
        print *, "WARNING:", my_rank," got message with unexpected content:", message
      endif

    endif
    check_messages = terminated
    print *, my_rank,"CHeck if terminated", terminated
  end function check_messages
  
  !************************************************************
  subroutine check_termination(proc)
    !  Purpose: only on termination_master, checks if all procs
    !           have reported termination
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in)    :: proc
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, i, alloc_stat
    logical                              :: finished
    integer(kind=i4_kind), allocatable   :: request(:), stats(:,:)
    integer(kind=i4_kind)                :: receiver, message(2)
!   !------------ Executable code --------------------------------
    ! all_done stores the procs, which have already my_resp = 0
    all_done(proc+1) = .true.
    ! check if there are still some procs not finished
    finished = .true.
    do i = 1, n_procs
      if (.not. all_done(i)) finished = .false.
    enddo

    ! there will be only a send to the other procs, telling them to terminate
    ! thus the termination_master sets its termination here
    if (finished) terminated = .true.
    if (finished .and. n_procs > 1) then
      allocate(request(n_procs-1), stats(n_procs-1,MPI_STATUS_SIZE),&
                     stat = alloc_stat)
      !ASSERT(alloc_stat==0)
      call assert_n(alloc_stat==0,9)
      message(1) = NO_WORK_LEFT
      message(2) = 0
      do i = 0, n_procs-2
        receiver = i
        ! skip the termination master (itsself)`
        if (i >= termination_master) receiver = i+1
        call MPI_ISEND(message, 2, MPI_INTEGER4, receiver, 185,comm_world ,request(i+1), ierr)
        !ASSERT(ierr==MPI_SUCCESS)
        call assert_n(ierr==MPI_SUCCESS, 4)
        print *, "Send termination to", receiver
      enddo
      call MPI_WAITALL(size(request), request, stats, ierr)
      !ASSERT(ierr==MPI_SUCCESS)
      call assert_n(ierr==MPI_SUCCESS, 4)
    endif
  end subroutine check_termination
  !************************************************************
  subroutine is_my_resp_done()
    !  Purpose: my_resp holds the number of jobs, assigned at the start
    !           to this proc, so this proc is responsible that they will
    !           be finished, if my_resp == 0, I should tell termination_master
    !           that, termination master will collect all the finished resp's
    !           untill it gots all of them back
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr
    logical                              :: flag
    integer(kind=i4_kind)                :: message(2)
!   !------------ Executable code --------------------------------
    print *,my_rank, "Left of my responsibility:", my_resp
    if (my_resp == 0) then
      if (my_rank == termination_master) then
        call check_termination(my_rank)
      else
        message(1) = RESP_DONE
        message(2) = my_rank
        call MPI_ISEND(message, 2, MPI_INTEGER4, termination_master, 185,comm_world, requ1, ierr) 
        !ASSERT(ierr==MPI_SUCCESS)
        call assert_n(ierr==MPI_SUCCESS, 4)
      endif
    endif
  end subroutine is_my_resp_done
  !************************************************************
  integer(kind=i4_kind) function select_victim()
     ! Purpose: decide of who to try next to get any jobs
     select_victim = select_victim_random2()
     !select_victim = select_victim_r()
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
     print *, "RRRRRRRRRRRRRR selcet victim round", select_victim_r, my_rank
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
       print *, my_rank, s, random
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
     print *, "RRRRRRRRRRR random victim", select_victim_random2, my_rank
  end function select_victim_random2
  !*************************************************************
  integer(kind=i4_kind) function select_victim_random()
     ! Purpose: decide of who to try next to get any jobs
     ! Uses primitive pseudorandom code X_n+1 = (aX_n + b)mod m
     integer(kind=i4_kind),parameter  :: a =134775813, b = 1 ! see Virtual Pascal/Borland Delphi
     integer(kind=i4_kind),parameter  :: m = 1000 ! FIXME: shoud we use other integer for 2**32 (fit to rest)
     integer(kind=i4_kind), save :: seed = -1
     integer(kind=i4_kind)  :: i, seedold
     ! ASSERT(.false.)
     ! Does not work yet, due to too much overflow (negative procs)
     if (seed == -1) then
        seed = my_rank
     endif
     seedold = seed
     seed = mod( (a*seed+b),m)
     select_victim_random = mod(seed, n_procs)
     i = 0
     do while (select_victim_random == my_rank .and. i < 10)
       seed = mod( (a*seed+b),m)
       select_victim_random = mod(seed, n_procs)
       i = i + 1
     enddo
     print *, my_rank, seedold,a*seedold+b , seed, select_victim_random, i
     if (select_victim_random == my_rank) select_victim_random = select_victim_r()
     print *, "RRRRRRRRRRR random victim", select_victim_random, my_rank
  end function select_victim_random
  !*************************************************************
   logical function local_tgetm(m, my_jobs)
    !  Purpose: returns true if could acces the global memory with
    !            the local jobs of the machine, false if there is already
    !            someone else working,
    !           if true also:
    !           local try for get m jobs, m is number of requested jobs
    !           if there are enough my_jobs will give their number back
    !           (fewer if there are not m left), if there is 0 jobs
    !           given back, there is no more in the storage
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: m
    integer(kind=i4_kind), intent(out  ) :: my_jobs(SJOB_LEN)
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, sap, w
!   !------------ Executable code --------------------------------
    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, my_rank, 0, win, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    ! each proc which ones to acces the memory, sets his point to 1
    ! (else they are all 0), so if the sum is more than 0, at least one
    ! proc tries to get the memory, the one, who got sap = 0 has the right
    ! to work
    sap = sum(job_storage(SJOB_LEN+1:))
    if (sap > 0) then
      print *, "There is already something going on in this storage"
      print *, "Because there is at least one proc active:", sap
      call time_stamp("blocked")
      local_tgetm = .false.
    else ! nobody is on the memory right now
      call time_stamp("free")
      local_tgetm = .true.
      ! check for thieves (to know when to wait for back reports)
      if (.not. job_storage(J_EP)== start_job(J_EP)) had_thief = .true.
      w = reserve_workm(m, job_storage) ! how many jobs to get
!     print *, "w=", w
      my_jobs = job_storage(:SJOB_LEN) ! first SJOB_LEN hold the job (furter the sap
      ! information)
!     print *, "my_jobs=", my_jobs
      if (w == 0) then ! no more jobs
        call report_or_store(my_jobs)
      else ! take my part of the jobs (divide jobs of storage)
            ! take from start beginning
        my_jobs(J_EP)  = my_jobs(J_STP) + w
        job_storage(J_STP) = my_jobs(J_EP)
      endif
      job_storage(SJOB_LEN+1:) = 0
    endif
!   print *, "my_jobs=", my_jobs
!   print *, "job_storage=", job_storage
!   print *, "local_tgetm: end locked", my_rank
    call MPI_WIN_UNLOCK(my_rank, win, ierr)
    call time_stamp("release")
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
!   print *, "local_tgetm: exit", my_rank
  end function local_tgetm
  !*************************************************************
  subroutine report_or_store(my_jobs)
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
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, alloc_stat
    logical                              :: flag
    integer(kind=i4_kind)                :: num_jobs_done, message(2)
    integer(kind=i4_kind),allocatable    :: intermed(:)
!   !------------ Executable code --------------------------------
    print *, my_rank, "FINISHED a job, now report or store", my_jobs
    ! my_jobs hold recent last point, as proc started from beginning and
    ! steal from the back, this means that from the initial starting point on
    ! (stored in start_job) to this one, all jobs were done
    ! if my_jobs(J_EP)/= start-job(J_EP) someone has stolen jobs
    num_jobs_done = my_jobs(J_EP) - start_job(J_STP)
    !if (num_jobs_done == 0) return ! there is non job, thus why care

    if (start_job(NRANK) == my_rank) then
      print *, my_rank, "my_resp=", my_resp, "-", num_jobs_done
      my_resp = my_resp - num_jobs_done
      call is_my_resp_done() ! check if all my jobs are done
    else
      ! As all isends have to be closed sometimes, storage of
      ! the request handlers is needed
      print *, my_rank, "send to", start_job(NRANK),"finished", num_jobs_done, "of his jobs"
      if (allocated(requ2)) then
        allocate(intermed(size(requ2,1)),stat = alloc_stat)
        !ASSERT(alloc_stat==0)
        call assert_n(alloc_stat==0, 4)
        intermed = requ2
        deallocate(requ2, stat = alloc_stat)
        !ASSERT(alloc_stat==0)
        call assert_n(alloc_stat==0, 4)
      endif
      if (allocated(intermed)) then
        allocate(requ2(size(intermed,1)+1), stat = alloc_stat)
      else
        allocate(requ2(1), stat = alloc_stat)
      endif
      !ASSERT(alloc_stat==0)
      call assert_n(alloc_stat==0, 4)
      if (size(requ2,1) > 1) requ2(2:) = intermed
      message(1) = DONE_JOB
      message(2) = num_jobs_done
      call MPI_ISEND(message,2, MPI_INTEGER4, start_job(NRANK),&
                                   185,comm_world, requ2(1), ierr)
      !ASSERT(ierr==MPI_SUCCESS)
      call assert_n(ierr==MPI_SUCCESS, 4)
    endif
  end subroutine report_or_store
  !*************************************************************
   logical function rmw_tgetm(m,source, my_jobs)
    !  Purpose: read-modify-write variant fo getting m from another proc
    !           source, returns true if the other proc has its memory free
    !           false if it is not
    !           if the memory can be accessed, take some jobs from him,
    !           store those, which are more than m in own local storage
    !           read-modify-write is implemented with an integer for each
    !           proc, which is 0, if this proc does not wants to access the
    !           memory and 1 else; before changing or using the informations
    !           gotten with MPI_GET, each proc tests if the sum fo this integer
    !           is 0, if it is, this proc is the first to access the memory,
    !           it may change and rewrite it, else only the integer belonging to
    !           the proc may be reset to 0
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: m, source
    integer(kind=i4_kind), intent(out  ) :: my_jobs(SJOB_LEN)
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, i, sap, w
    integer(i4_kind), target             :: jobs_infom(jobs_len)
!   !------------ Executable code --------------------------------
    my_jobs(J_EP)  = 0
    my_jobs(J_STP) = 0
    my_jobs(NRANK) = -1
    ! First GET-PUT round, MPI only ensures taht after MPI_UNLOCK the
    ! MPI-RMA accesses are finished, thus modify has to be done out of this lock
    ! There are two function calls inside: one to get the data and check if it may be accessed
    ! second one to show to other procs, that this one is interested in modifing the data
    print *, my_rank, "RMW, first lock start"
    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, source, 0, win, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    call MPI_GET(jobs_infom, jobs_len, MPI_INTEGER4, source, 0, jobs_len, MPI_INTEGER4, win, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    call MPI_PUT(ON,1,MPI_INTEGER4, source, my_rank + SJOB_LEN, 1, MPI_INTEGER4, win, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    call MPI_WIN_UNLOCK(source, win, ierr)
    print *, my_rank, "RMW, first lock exit"
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    jobs_infom(my_rank+1+SJOB_LEN) = 0
    print *,my_rank,"Available jobs",source,"are", jobs_infom(:SJOB_LEN)
    print *,my_rank, "Currently working on proc", source,"are", jobs_infom(SJOB_LEN+1:)
    call time_stamp("inform")
    ! check if there are any procs, saying that they want to acces the memory
    sap = sum(jobs_infom(SJOB_LEN+1:))

    if (sap > 0) then ! this one is not the first, thus leave the memory to the others
      print *, "There is already something going on in this storage", my_rank
      print *, "Because there is at least one proc active:", sap, my_rank 
      call time_stamp("blocked")
      rmw_tgetm = .false.
      ! Just set back the want of access (there is anyhow no method to store another order, than 
      ! first and rest)
!   print *, my_rank, "RMW, second no acces lock start"
!     call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, source, 0, win, ierr)
!     !ASSERT(ierr==MPI_SUCCESS)
!     call assert_n(ierr==MPI_SUCCESS, 4)
!     call MPI_PUT(OFF,1,MPI_INTEGER4, source, my_rank + SJOB_LEN, 1, MPI_INTEGER4, win, ierr)
!     !ASSERT(ierr==MPI_SUCCESS)
!     call assert_n(ierr==MPI_SUCCESS, 4)
!     call MPI_WIN_UNLOCK(source, win, ierr)
!   print *, my_rank, "RMW, second no acces lock exit"
      !ASSERT(ierr==MPI_SUCCESS)
      call assert_n(ierr==MPI_SUCCESS, 4)
    else ! is the first one, therefor do what you want with it
      rmw_tgetm = .true.
      ! how much of the work should be stolen
      w = reserve_workh(m,jobs_infom)
      print *, "Nobody out here, take for me", my_rank, w
      call time_stamp("free")
      my_jobs = jobs_infom(:SJOB_LEN)
      if (w == 0) then ! nothing to steal, set default
        my_jobs(J_EP)  = 0
        my_jobs(J_STP) = 0
        my_jobs(NRANK) = -1
      else ! take the last w jobs of the job-storage
        my_jobs(J_STP)  = my_jobs(J_EP) - w
        jobs_infom(J_EP) = my_jobs(J_STP)
        ! the rest is for reseting the single job run, needed for termination
        start_job = my_jobs
      endif
      ! after setting back, there is a new chance to access the memory for the others
      jobs_infom(SJOB_LEN+1:) = 0
      print *,my_rank ,"JOBS to give back", jobs_infom
      ! Final lock of this memory, to give back unstolen jobs and to free the 
      ! user setted outer lock for the others
    print *, my_rank, "RMW, second with acces lock start"
      call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, source, 0, win, ierr)
      !ASSERT(ierr==MPI_SUCCESS)
      call assert_n(ierr==MPI_SUCCESS, 4)
      call MPI_PUT(jobs_infom, jobs_len, MPI_INTEGER4, source, 0, jobs_len, MPI_INTEGER4, win, ierr)
      !ASSERT(ierr==MPI_SUCCESS)
      call assert_n(ierr==MPI_SUCCESS, 4)
      call MPI_WIN_UNLOCK(source, win, ierr)
    print *, my_rank, "RMW, second with acces lock exit"
      call time_stamp("released")
      !ASSERT(ierr==MPI_SUCCESS)
      call assert_n(ierr==MPI_SUCCESS, 4)
      ! The give_grid function needs only up to m' jobs at once, thus
      ! divide the jobs
      if (w /= 0) then
        jobs_infom(:SJOB_LEN) = my_jobs
        w = reserve_workm(m,jobs_infom)
        ! these are for direct use
        my_jobs(J_EP)  = my_jobs(J_STP) + w
        jobs_infom(J_STP) = my_jobs(J_EP)
        print *, my_rank, "Store my new jobs"
        ! this stores the rest for later in the own job-storage
        call store_new_work(jobs_infom)
      endif
    endif
  end function rmw_tgetm
  !*************************************************************
  integer(kind=i4_kind) function reserve_workm(m, jobs)
    integer(kind=i4_kind), intent(in   ) :: m
    integer(kind=i4_kind), intent(in   ) :: jobs(:)
    ! PURPOSE: give back number of jobs to take, should be up to m, but
    ! less if there are not so many available
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    !------------ Executable code --------------------------------
    reserve_workm = min(jobs(J_EP) - jobs(J_STP), m)
    reserve_workm = max(reserve_workm, 0)
  end function reserve_workm
  !*************************************************************
   subroutine store_new_work(my_jobs)
    !  Purpose: stores the new jobs (minus the ones for direct use)
    !           in the storage, as soon as there a no more other procs
    !           trying to do something
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in  ) :: my_jobs(jobs_len)
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, sap
!   !------------ Executable code --------------------------------
    sap = 1
    !ASSERT(sum(my_jobs(SJOB_LEN+1:))==0)
    call assert_n(sum(my_jobs(SJOB_LEN+1:))==0, 4)
    do while (sap > 0 )
      call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, my_rank, 0, win, ierr)
      !ASSERT(ierr==MPI_SUCCESS)
      call assert_n(ierr==MPI_SUCCESS, 4)
      ! Test if there are more procs doing something on the storage
      ! (They may give back something wrong) if yes cycle, else store
      sap = sum(job_storage(SJOB_LEN+1:))
      if (sap > 0) then
        print *, "There is already something going on in my storage"
        print *, "Because there is at least one proc active:", sap
      else
        job_storage = my_jobs
      endif
      call MPI_WIN_UNLOCK(my_rank, win, ierr)
      !ASSERT(ierr==MPI_SUCCESS)
      call assert_n(ierr==MPI_SUCCESS, 4)
!     print *, "local_tgetm: exit", my_rank
    enddo
  end subroutine store_new_work
  !*************************************************************
  integer(kind=i4_kind) function reserve_workh(m,jobs)
    integer(kind=i4_kind), intent(in   ) :: m
    integer(kind=i4_kind), intent(in   ) :: jobs(:)
    ! Purpose: give back number of jobs to take, half what is there
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: many_jobs
    !------------ Executable code --------------------------------
    many_jobs = (jobs(J_EP) - jobs(J_STP))
    ! if the number of job batches (of size m) is not odd, then
    ! the stealing proc should get more, as it starts working on them
    ! immediatelly
    reserve_workh =  (many_jobs /(2* m))* m
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
    had_thief = .false.
    terminated = .false.
    all_done  = .false.
    print *, my_rank, "start my_rest=", my_resp
    ! set starting values for the jobs, needed also in this way for
    ! termination, as they will be stored also in the job_storage
    ! start_job should only be changed if all current jobs are finished
    ! and there is a try to steal new ones
    start_job(:L_JOB) = job
    start_job(NRANK) = my_rank
    ! needed for termination
    my_resp = start_job(J_EP) - start_job(J_STP)
    ! Job storage holds all the jobs currently in use
    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, my_rank, 0, win, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    job_storage(:SJOB_LEN) = start_job
    job_storage(SJOB_LEN+1:) = 0
    call MPI_WIN_UNLOCK(my_rank, win, ierr)
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
    call MPI_WIN_FENCE(0, win, ierr) ! FIXME: Is this really needed? We should
                                    ! Have lot's of time, till the first non local
                                    ! job search
    !ASSERT(ierr==MPI_SUCCESS)
    call assert_n(ierr==MPI_SUCCESS, 4)
  end subroutine dlb_setup
  !*************************************************************


  !--------------- End of module ----------------------------------
end module dlb_module
