!===============================================================
! Public interface of module
!===============================================================
module dlb_impl
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
# include "dlb.h"
  USE_MPI
  use iso_c_binding
  use dlb_common, only: i4_kind, r8_kind, comm_world
  use dlb_common, only: time_stamp, time_stamp_prefix ! for debug only
  use dlb_common, only: DONE_JOB, NO_WORK_LEFT, RESP_DONE, SJOB_LEN, L_JOB, JOWNER, JLEFT, JRIGHT, MSGTAG
  use dlb_common, only: add_request, test_requests, end_requests, send_resp_done, report_job_done
  use dlb_common, only: dlb_common_setup, has_last_done, send_termination
  use dlb_common, only: my_rank, n_procs, termination_master, set_start_job, set_empty_job
  use dlb_common, only: decrease_resp
  use dlb_common, only: end_communication
  implicit none
  save            ! save all variables defined in this module
  private         ! by default, all names are private
  !== Interrupt end of public interface of module =================

  !------------ public functions and subroutines ------------------
  public :: dlb_init, dlb_finalize, dlb_setup, dlb_give_more

  !================================================================
  ! End of public interface of module
  !================================================================

  !------------ Declaration of types ------------------------------

  !------------ Declaration of constants and variables ----
  integer(kind=i4_kind), parameter  :: ON = 1, OFF = 0 ! For read-modify-write
  integer(kind=i4_kind)             :: jobs_len  ! Length of complete jobs storage
  integer(kind=i4_kind)            :: win ! for the RMA object
  type(c_ptr)  :: c_job_pointer     ! needed for MPI RMA handling (for c pseudo pointer)
  integer(kind=i4_kind), pointer    :: job_storage(:) ! store all the jobs, belonging to this processor
  integer(kind=i4_kind)             :: start_job(SJOB_LEN) ! job_storage is changed a lot, backup for
                                     ! finding out, if someone has stolen something, or how many jobs one
                                     ! has done, when job_storage hold no more jobs
                                              ! in setup
  logical                           :: had_thief ! only check for messages if there is realy a change that there are some
  logical                           :: terminated!, finishedjob ! for termination algorithm
  integer(kind=i4_kind),allocatable :: requ2(:) ! requests of send are stored till relaese
  integer(kind=i4_kind)             :: requ1 ! this request will be used in any case
  integer(kind=i4_kind)             :: my_resp_start, my_resp_self, your_resp !how many jobs doen where
  integer(kind=i4_kind)             :: many_tries, many_searches !how many times asked for jobs
  integer(kind=i4_kind)             :: many_locked, many_zeros
  !----------------------------------------------------------------
  !------------ Subroutines ---------------------------------------
contains

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
    use dlb_common, only: dlb_common_init
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, sizeofint
    integer(kind=MPI_ADDRESS_KIND)       :: size_alloc, size_all
    !------------ Executable code --------------------------------

    ! some aliases, highly in use during the whole module
    call dlb_common_init()

    ! find out, how much there is to store and allocate (with MPI) the memory
    call MPI_TYPE_EXTENT(MPI_INTEGER4, sizeofint, ierr)

    jobs_len = SJOB_LEN + n_procs
    size_alloc = jobs_len * sizeofint

    call MPI_ALLOC_MEM(size_alloc, MPI_INFO_NULL, c_job_pointer, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    ! this connects a c'like pointer with our job_storage, as MPI can not handle our
    ! pointers as they are
    call c_f_pointer(c_job_pointer, job_storage, [jobs_len])

    size_all = jobs_len * sizeofint ! for having it in the correct kind

    ! win (an integer) is set up, so that the RMA-processes can call on it
    ! they will then get acces to the local stored job_storage of the corresponding proc
    call MPI_WIN_CREATE(job_storage, size_all, sizeofint, MPI_INFO_NULL, &
                        comm_world, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    ! needed to make certain, that in the next steps, all the procs have all the informations
    call MPI_WIN_FENCE(0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    ! put the complete storage content to 0
    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, my_rank, 0, win, ierr)

    job_storage = 0

    call MPI_WIN_UNLOCK(my_rank, win, ierr)
  end subroutine dlb_init

  subroutine dlb_finalize()
    !  Purpose: shuts down the dlb objects (especially job_storage)
    !  when it is not further needed, should be called, after ALL
    !  dlb runs have complete
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: dlb_common_finalize
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr
    !------------ Executable code --------------------------------

    call MPI_WIN_FENCE(0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    CALL MPI_FREE_MEM(job_storage, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    !
    ! FIXME: why second time?
    !
    call MPI_WIN_FENCE(0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    CALL MPI_WIN_FREE(win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    call dlb_common_finalize()
  end subroutine dlb_finalize

  subroutine dlb_give_more(n, my_job)
    !  Purpose: Returns next bunch of up to n jobs, if jobs(JRIGHT)<=
    !  jobs(JLEFT) there are no more jobs there, else returns the jobs
    !  done by the procs should be jobs(JLEFT) + 1 to jobs(JRIGHT) in
    !  the related job list
    !  first the jobs are tried to get from the local storage of the
    !  current proc, if there are no more, it will try to steal some
    !  more work from others, till the separate termination algorithm
    !  states, that everything is done
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: select_victim
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: n
    integer(kind=i4_kind), intent(out  ) :: my_job(L_JOB)
    !** End of interface *****************************************

    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: v, ierr
    logical                              :: term
    integer(i4_kind), target             :: jobs(SJOB_LEN)
    !------------ Executable code --------------------------------

    ! check for (termination) messages, if: have to gather the my_resp = 0
    ! messages (termination Master) or if someone has stolen
    ! and my report back
    term = .false.
    if(had_thief .or. (my_rank==termination_master)) term = check_messages()

! This seems to be there with our local network, trying to avoid as much lock/unlocks as possible
!   if (term) then
!     my_job = start_job(:L_JOB)
!     my_job(JLEFT) = my_job(JRIGHT)
!     call time_stamp("dlb_give_more: exit on TERMINATION flag", 1)
!     return
!   endif

    ! First try to get job from local storage (loop because some one lese may
    ! try to read from there (then local_tgetm = false)
    do while (.not. local_tgetm(n, jobs))
       call time_stamp("waiting for time to acces my own memory",2)
    enddo
    call time_stamp("finished local search",3)

    if (jobs(JLEFT) >= jobs(JRIGHT)) many_searches = many_searches + 1 ! just for debugging

    ! if local storage only gives empty job:
    do while ((jobs(JLEFT) >= jobs(JRIGHT)) .and. .not. check_messages())

      ! check like above but also for termination message from termination master
      v = select_victim(my_rank, n_procs)

      ! try to get job from v, if v's memory occupied by another or contains
      ! nothing to steal, job is st
      many_tries = many_tries + 1 ! just for debugging ill empty

      call time_stamp("about to call rmw_tgetm()",4)
      term = rmw_tgetm(n, v, jobs)
      call time_stamp("returned from rmw_tgetm()",4)
    enddo
    call time_stamp("finished stealing",3)

    ! only the start and endpoint of job slice is needed external
    my_job = jobs(:L_JOB)

    ! this would not work: no ensurance that the others already know that
    ! they should be terminated, they could steal jobs setup by already terminated
    ! processors
    !if (terminated) call dlb_setup(setup_jobs)
    if (jobs(JLEFT) >= jobs(JRIGHT)) then
       print *, my_rank, "tried", many_searches, "for new jobs and stealing", many_tries
       print *, my_rank, "was locked", many_locked, "got zero", many_zeros
       call MPI_BARRIER(comm_world, ierr)
       ASSERT(ierr==MPI_SUCCESS)
    endif
    call time_stamp("dlb_give_more: exit",3)
  end subroutine dlb_give_more

  logical function check_messages()
    !  Purpose: checks if any message has arrived, checks for messages:
    !          Someone finished stolen job slice
    !          Someone has finished its responsibilty (only termination_master)
    !          There are no more jobs (message from termination_master to finish)
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, stat(MPI_STATUS_SIZE)
    logical                              :: flag
    integer(kind=i4_kind)                :: message(1 + SJOB_LEN)
    !------------ Executable code --------------------------------

    ! check for any message
    call MPI_IPROBE(MPI_ANY_SOURCE, MSGTAG, comm_world,flag, stat, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    do while (flag) !got a message

      call MPI_RECV(message, 1+SJOB_LEN, MPI_INTEGER4, MPI_ANY_SOURCE, MSGTAG,comm_world, stat,ierr)
      !print *, time_stamp_prefix(MPI_Wtime()), "got message from", stat(MPI_SOURCE), "with", message
      ASSERT(ierr==MPI_SUCCESS)

      select case ( message(1) )

      case ( DONE_JOB )
         ! someone finished stolen job slice
         ASSERT(message(2)>0)

         if (decrease_resp(message(2), stat(MPI_SOURCE)) == 0) then
           if (my_rank == termination_master) then
             call check_termination(my_rank)
           else
             call send_resp_done(requ2)
           endif
           call time_stamp("send my_resp done", 2)
         endif

      case ( RESP_DONE )
         ! finished responsibility

         if (my_rank == termination_master) then
           call check_termination(message(2))
         else ! give only warning, some other part of the code my use this message (but SHOULD NOT)
           ! This message makes no sense in this context, thus give warning
           ! and continue (maybe the actual calculation has used it)
           print *, time_stamp_prefix(MPI_Wtime()), "ERROR: got unexpected message (I'm no termination master):", message
           print *, "Please make sure, that message tag",MSGTAG, "is not used by the rest of the program"
           print *, "or change parameter MSGTAG in this module to an unused value"
           call abort()
         endif

      case ( NO_WORK_LEFT )
         ! termination message from termination master
         ASSERT(message(2)==0)

         if( stat(MPI_SOURCE) /= termination_master )then
             stop "stat(MPI_SOURCE) /= termination_master"
         endif

         terminated = .true.
         ! NOW all my messages HAVE to be complete, so close (without delay)
         call end_requests(requ2)
         call end_communication()

      case default
        ! This message makes no sense in this context, thus give warning
        ! and continue (maybe the actual calculation has used it)
        print *, time_stamp_prefix(MPI_Wtime()), "ERROR: got message with unexpected content:", message
        print *, "Please make sure, that message tag",MSGTAG, "is not used by the rest of the program"
        print *, "or change parameter MSGTAG in this module to an unused value"
        call abort()
      end select

      call MPI_IPROBE(MPI_ANY_SOURCE, MSGTAG, comm_world, flag, stat, ierr)
      ASSERT(ierr==MPI_SUCCESS)
    enddo
    check_messages = terminated

    if (terminated) then
      call time_stamp("TERMINATING!", 1)
    endif
  end function check_messages

  subroutine check_termination(proc)
    !  Purpose: only on termination_master, checks if all procs
    !           have reported termination
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in)    :: proc
    !** End of interface *****************************************

    if (.not. has_last_done(proc)) RETURN

    ! there will be only a send to the other procs, telling them to terminate
    ! thus the termination_master sets its termination here
    terminated = .true.
    if (n_procs > 1) then
      call send_termination()
    endif
    call end_requests(requ2)
    call end_communication()
  end subroutine check_termination

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
    use dlb_common, only: reserve_workm
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: m
    integer(kind=i4_kind), intent(out  ) :: my_jobs(SJOB_LEN)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, sap, w
    !------------ Executable code --------------------------------

    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, my_rank, 0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    ! each proc which ones to acces the memory, sets his point to 1
    ! (else they are all 0), so if the sum is more than 0, at least one
    ! proc tries to get the memory, the one, who got sap = 0 has the right
    ! to work
    my_jobs = job_storage(:SJOB_LEN) ! first SJOB_LEN hold the job (furter the sap

    sap = sum(job_storage(SJOB_LEN+1:))
    if (sap > 0) then
      call time_stamp("blocked by lock contension",2)
      ! find out if it makes sense to wait:
      if (my_jobs(JLEFT) >= my_jobs(JRIGHT)) then
         local_tgetm = .true.
         call time_stamp("blocked, but empty",2)
         call report_or_store(my_jobs)
      else
         local_tgetm = .false.
         my_jobs(JRIGHT) = my_jobs(JLEFT) ! non actuell informations, make them invalid
      endif
    else ! nobody is on the memory right now
      call time_stamp("free",2)
      local_tgetm = .true.
      ! check for thieves (to know when to wait for back reports)
      if (.not. job_storage(JRIGHT)== start_job(JRIGHT)) had_thief = .true.
      w = reserve_workm(m, job_storage) ! how many jobs to get
      if (w == 0) then ! no more jobs
        call report_or_store(my_jobs)
      else ! take my part of the jobs (divide jobs of storage)
            ! take from start beginning
        my_jobs(JRIGHT)  = my_jobs(JLEFT) + w
        job_storage(JLEFT) = my_jobs(JRIGHT)
      endif
      job_storage(SJOB_LEN+1:) = 0
    endif
    call MPI_WIN_UNLOCK(my_rank, win, ierr)
    call time_stamp("release",3)
    ASSERT(ierr==MPI_SUCCESS)
  end function local_tgetm

  subroutine report_or_store(my_jobs)
    !  Purpose: If a job is finished, this cleans up afterwards
    !           Needed for termination algorithm, there are two
    !           cases, it was a job of the own responsibilty or
    !           one from another, first case just change my number
    !            second case, send to victim, how many of his jobs
    !            were finished
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in  ) :: my_jobs(SJOB_LEN)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: num_jobs_done
    !------------ Executable code --------------------------------
    !print *, time_stamp_prefix(MPI_Wtime()), "finished a job, now report or store", my_jobs
    ! my_jobs hold recent last point, as proc started from beginning and
    ! steal from the back, this means that from the initial starting point on
    ! (stored in start_job) to this one, all jobs were done
    ! if my_jobs(JRIGHT)/= start-job(JRIGHT) someone has stolen jobs
    num_jobs_done = my_jobs(JRIGHT) - start_job(JLEFT)
    !if (num_jobs_done == 0) return ! there is non job, thus why care

    if (start_job(JOWNER) == my_rank) then
      my_resp_self = my_resp_self + num_jobs_done
      if (decrease_resp(num_jobs_done, my_rank)== 0) then
        if (my_rank == termination_master) then
          call check_termination(my_rank)
        else
          call send_resp_done(requ2)
        endif
      endif
    else
      your_resp = your_resp + num_jobs_done
      call report_job_done(num_jobs_done, start_job(JOWNER))
    endif
  end subroutine report_or_store

  function rmw_tgetm(m, rank, jobs) result(locked)
    !  Purpose: read-modify-write variant fo getting m from another proc
    !           rank, returns true if the other proc has its memory free
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
    use dlb_common, only: steal_work_for_rma, reserve_workm
    implicit none
    integer(i4_kind), intent(in)  :: m, rank
    integer(i4_kind), intent(out) :: jobs(SJOB_LEN)
    logical                       :: locked ! result
    ! *** end of interface ***

    integer(i4_kind) :: remote_jobs(SJOB_LEN)
    integer(i4_kind) :: stolen_jobs(SJOB_LEN)
    integer(i4_kind) :: remaining_jobs(SJOB_LEN)
    integer(i4_kind) :: local_jobs(SJOB_LEN)
    integer(i4_kind) :: work

    !
    ! remote_jobs are fetched from rank, split into
    !   1) remaining_jobs --- are returned back to rank
    !   2) stolen_jobs    --- for local processing,
    !      these are further split into
    !      a) jobs        --- are output of this sub.
    !      b) local_jobs  --- are stored in local storage
    !
    ! FIXME: splitting stolen jobs into 2a) and 2b) is beyond the scope
    !        of read-modify-write and doesn not belong here!
    !

    !
    ! NOTE: size(jobs) is SJOB_LEN, (jobs(JLEFT), jobs(JRIGHT)] specify
    ! the interval, the rest is metadata that should be copied
    ! around.
    !
    ASSERT(size(jobs)==SJOB_LEN)

    jobs = set_empty_job()

    !
    ! Try reading job info from "rank", returns false if not successfull:
    !
    locked = try_lock_and_read(rank, remote_jobs)

    ! Return if locking failed:
    if ( .not. locked ) RETURN

    ! how much of the work should be stolen
    work = steal_work_for_rma(m, remote_jobs) ! expects remote_jobs(1:2)
    !
    ! FIXME: stealing could be realized as splitting the interval/slice:
    !
    !        split_job_slice(orig, left, right)
    !
    ! See redundant error-prone code below.
    !

    ! Unlock and return if nothing to steal
    if ( work == 0 ) then
        !
        ! Nothing can be stolen --- skip "modify-write" in "read-modify-write",
        ! only do unlock:
        !
        call unlock(rank)
        RETURN
    endif

    !
    ! Stealing is implemented as splitting the interval
    !
    !     (A, B] = remote_jobs(1:2)
    !
    ! at the point
    !
    !     C = B - work
    !
    ! into
    !
    !     remote_jobs(1:2) = (A, C] and jobs(1:2) = (C, B]
    !
    call split_at(remote_jobs(JRIGHT) - work, remote_jobs, remaining_jobs, stolen_jobs)

    !
    ! The rest is for reseting the single job run, needed for termination
    !
    start_job = jobs
    ! FIXME: Why secretly modifying global data structures?
    !        Does it need to be protected by lock/unlock or why?

    !
    ! Write back remote_jobs(1:2) and release the lock:
    !
    call write_and_unlock(rank, remaining_jobs)

    !
    ! FIXME: why the following code is put here is not quite clear,
    !        maybe it does not belong into "rmw"?
    !

    !
    ! "Stealing" from myself is implemented as splitting the interval
    !
    !     (A, B] = stolen_jobs(1:2)
    !
    ! at the point
    !
    !     C = A + work
    !
    ! into
    !
    !     jobs(1:2) = (A, C] and local_jobs(1:2) = (C, B]
    !

    ! The give_grid function needs only up to m' jobs at once, thus
    ! divide the jobs
    work = reserve_workm(m, jobs) ! expects jobs(1:2)

    call split_at(jobs(JLEFT) + work, stolen_jobs, jobs, local_jobs)

    !
    ! This stores the rest for later in the OWN job-storage
    !
    call write_unsafe(my_rank, local_jobs)
    ! This stores the new jobs (minus the ones for direct use)
    ! in the storage, as soon as there a no more other procs
    ! trying to do something
    !
    ! FIXME: (already addressed?)
    ! The while loop may be go on for ever: because it
    ! is nowere ensured that the current processor will
    ! find its memory unlocked of the user lock by the others
    ! some times. If all other procs have finished and this
    ! one wants to write the additional jobs in its own storage
    ! it may find each time it gets the win_lock a user lock
    ! by someone else. This has already happend if some procs
    ! were trying to get their local_tgetm when all were
    ! finished. In this case a checking for termination stuff
    ! already im the local loop helped.
    ! Here the solution is that the proc writes the additional
    ! information in the storage, that is empty, anyhow.
    ! In this case stealing procs, which find empty storage
    ! are allowed only to reset the lock relevant part of the storage
    ! A better solution is welcome.
  end function rmw_tgetm

  function try_lock_and_read(rank, jobs) result(locked)
    !
    ! Try getting a lock indexed by rank and if that succeeds,
    ! read data into jobs(1:2)
    !
    use dlb_common, only: my_rank, n_procs
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(i4_kind), intent(in)  :: rank
    integer(i4_kind), intent(out) :: jobs(:) ! (SJOB_LEN)
    logical                       :: locked ! result
    ! *** end of interface ***

    integer(i4_kind)          :: ierr
    integer(i4_kind), target  :: win_data(jobs_len) ! FIXME: why target?
    integer(MPI_ADDRESS_KIND) :: displacement
    integer(MPI_ADDRESS_KIND), parameter :: zero = 0

    ASSERT(size(jobs)==SJOB_LEN)

    locked = .false.
    jobs = -1 ! junk

    ! First GET-PUT round, MPI only ensures taht after MPI_UNLOCK the
    ! MPI-RMA accesses are finished, thus modify has to be done out of this lock
    ! There are two function calls inside: one to get the data and check if it may be accessed
    ! second one to show to other procs, that this one is interested in modifing the data

    !
    ! FIXME: GET/PUT of the same data item in a single epoch is illegal:
    !
    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, rank, 0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    call MPI_GET(win_data, size(win_data), MPI_INTEGER4, rank, zero, size(win_data), MPI_INTEGER4, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    displacement = my_rank + SJOB_LEN ! for getting it in the correct kind

    call MPI_PUT(ON, 1, MPI_INTEGER4, rank, displacement, 1, MPI_INTEGER4, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    call MPI_WIN_UNLOCK(rank, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    !
    ! FIXME: this is the data item that was GET/PUT in the same epoch:
    !
    win_data(my_rank + 1 + SJOB_LEN) = 0

    !
    ! Check if there are any procs, saying that they want to acces the memory
    !
    if ( sum(win_data(SJOB_LEN+1:)) == 0 ) then
        locked = .true.
        jobs(:) = win_data(1:SJOB_LEN)
    else
        ! Do nothing. Dont even try to undo our attempt to acquire the lock
        ! as the one that is holding the lock will overwrite the lock
        ! data structure when finished. See unlock/write_and_unlock below.
    endif
  end function try_lock_and_read

  subroutine unlock(rank)
    !
    ! Release previousely acquired lock indexed by rank.
    !
    use dlb_common, only: n_procs
    implicit none
    integer(i4_kind), intent(in) :: rank
    ! *** end of interface ***

    integer(i4_kind) :: ierr
    integer(i4_kind), target :: zeros(n_procs) ! FIXME: why target?
    integer(MPI_ADDRESS_KIND), parameter :: displacement = SJOB_LEN ! long int
    integer(MPI_ADDRESS_KIND), parameter :: zero = 0

    zeros(:) = 0

    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, rank, 0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    call MPI_PUT(zeros, size(zeros), MPI_INTEGER4, rank, displacement, size(zeros), MPI_INTEGER4, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    call MPI_WIN_UNLOCK(rank, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)
  end subroutine unlock

  subroutine write_and_unlock(rank, jobs)
    !
    ! Update job range description and release previousely acquired lock.
    !
    use dlb_common, only: steal_work_for_rma, reserve_workm
    implicit none
    integer(i4_kind), intent(in) :: rank
    integer(i4_kind), intent(in) :: jobs(:)
    ! *** end of interface ***

    integer(i4_kind)          :: ierr
    integer(i4_kind), target  :: win_data(jobs_len) ! FIXME: why target?
    integer(MPI_ADDRESS_KIND), parameter :: zero = 0
    !------------ Executable code --------------------------------

    ASSERT(size(jobs)==SJOB_LEN)
    ASSERT(SJOB_LEN+n_procs==jobs_len)

    !
    ! Real data:
    !
    win_data(1:SJOB_LEN) = jobs(:)

    !
    ! Zero fields responsible for locking:
    !
    win_data(SJOB_LEN+1:) = 0

    !
    ! PUT data and overwrite lock data structure with zeros in one epoch:
    !
    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, rank, 0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    call MPI_PUT(win_data, size(win_data), MPI_INTEGER4, rank, zero, size(win_data), MPI_INTEGER4, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    call MPI_WIN_UNLOCK(rank, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)
  end subroutine write_and_unlock

  subroutine write_unsafe(rank, jobs)
    !
    ! Owerwrite jobs data structure without acquiring user-level lock
    ! as done in try_read_lock()/write_unlock()
    !
    use dlb_common, only: my_rank
    implicit none
    integer(i4_kind), intent(in) :: rank, jobs(:)
    ! *** end of interface ***

    integer(i4_kind) :: ierr

    !
    ! FIXME: so far only used to store into the local datastructure:
    !
    ASSERT(rank==my_rank)
    ASSERT(size(jobs)==SJOB_LEN)

    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, rank, 0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    job_storage(:SJOB_LEN) = jobs(:)

    call MPI_WIN_UNLOCK(rank, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)
  end subroutine write_unsafe

  subroutine split_at(C, AB, AC, CB)
    !
    ! Split (A, B] into (A, C] and (C, B]
    !
    implicit none
    integer(i4_kind), intent(in)  :: C, AB(:)
    integer(i4_kind), intent(out) :: AC(size(AB)), CB(size(AB))
    ! *** end of interface ***

    ASSERT(size(AB)==SJOB_LEN)

    ASSERT(C>AB(JLEFT))
    ASSERT(C<=AB(JRIGHT))

    ! copy trailing posiitons, if any:
    AC(:) = AB(:)
    CB(:) = AB(:)

    AC(JLEFT)  = AB(JLEFT)
    AC(JRIGHT) = C

    CB(JLEFT)  = C
    CB(JRIGHT) = AB(JRIGHT)
  end subroutine split_at

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
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr
    !------------ Executable code --------------------------------

    ! these variables are for the termination algorithm
    had_thief = .false.
    terminated = .false.

    ! set starting values for the jobs, needed also in this way for
    ! termination, as they will be stored also in the job_storage
    ! start_job should only be changed if all current jobs are finished
    ! and there is a try to steal new ones
    start_job = set_start_job(job)

    call dlb_common_setup(start_job(JRIGHT) - start_job(JLEFT))

    many_tries = 0
    many_searches = 0
    many_locked = 0
    many_zeros = 0
    my_resp_self = 0
    your_resp = 0

    ! needed for termination
    my_resp_start = start_job(JRIGHT) - start_job(JLEFT)

    ! Job storage holds all the jobs currently in use
    call write_and_unlock(my_rank, start_job)
    ! FIXME: this abuses MPI_PUT to store to the local storage,
    !        specialize write_and_unlock(...) it desired.

    !
    ! FIXME: this is a collective operation, it will prevent
    !        workers from starting asyncronousely with their
    !        workshares!
    !
    call MPI_WIN_FENCE(0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)
  end subroutine dlb_setup

end module dlb_impl
