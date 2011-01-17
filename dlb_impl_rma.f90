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
  use dlb_common, only: DONE_JOB, NO_WORK_LEFT, RESP_DONE, JLENGTH, L_JOB, JOWNER, JLEFT, JRIGHT, MSGTAG
  use dlb_common, only: add_request, test_requests, end_requests, send_resp_done, report_job_done
  use dlb_common, only: dlb_common_setup, has_last_done, send_termination
  use dlb_common, only: my_rank, n_procs, termination_master, set_start_job, set_empty_job
  use dlb_common, only: decrease_resp
  use dlb_common, only: end_communication
  use dlb_common, only: split_at
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
  integer(kind=i4_kind)             :: already_done ! stores how many jobs the proc has done from
                                      ! the current interval
  integer(kind=i4_kind)             :: remember_last ! to find out if someone has stolen something
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

    jobs_len = JLENGTH + n_procs
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
    if (my_rank ==0) then
        print *, "DLB init: using variant 'rma'"
        print *, "DLB init: This variant uses the remote memory access of MPI implementation"
        print *, "DLB init: it needs real asynchronous RMA actions by MPI for working properly"
    endif
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

  subroutine dlb_give_more(n, slice)
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
    integer(i4_kind), intent(in)  :: n
    integer(i4_kind), intent(out) :: slice(2)
    !** End of interface *****************************************

    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: v, ierr
    logical                              :: term
    integer(i4_kind), target             :: jobs(JLENGTH)
    !------------ Executable code --------------------------------

    ! check for (termination) messages, if: have to gather the my_resp = 0
    ! messages (termination Master) or if someone has stolen
    ! and my report back
    term = .false.
    if(had_thief .or. (my_rank==termination_master)) term = check_messages()

    ! First try to get job from local storage (loop because some one lese may
    ! try to read from there ( local_tgetm false means, have to try again,
    ! true means that this proc has gotten all the jobs it may get from its
    ! local storage in this dlb-loop)
    do while ( .not. local_tgetm(n, jobs, already_done) )
       call time_stamp("waiting for time to acces my own memory",2)
    enddo
    call time_stamp("finished local search",3)
    if (jobs(JLEFT) .NE. remember_last) had_thief = .true.
    remember_last = jobs(JRIGHT)

    if ( empty(jobs) ) many_searches = many_searches + 1 ! just for debugging

    ! if local storage only gives empty job:
    do while ( empty(jobs) .and. .not. check_messages())

      ! check like above but also for termination message from termination master
      v = select_victim(my_rank, n_procs)

      ! try to get job from v, if v's memory occupied by another or contains
      ! nothing to steal, job is still empty
      many_tries = many_tries + 1 ! just for debugging

      call time_stamp("about to call rmw_tgetm()",4)
      term = rmw_tgetm(n, v, jobs, already_done)
      call time_stamp("returned from rmw_tgetm()",4)
    enddo
    call time_stamp("finished stealing",3)

    ! NOTE: named constants are now known outside.
    ! Return only the start and endpoint of the job slice:
    slice(1) = jobs(JLEFT)
    slice(2) = jobs(JRIGHT)

    ! this would not work: no ensurance that the others already know that
    ! they should be terminated, they could steal jobs setup by already terminated
    ! processors
    !if (terminated) call dlb_setup(setup_jobs)
    if ( empty(jobs) ) then
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
    use dlb_common, only: print_statistics
    implicit none
    !------------ Declaration of formal parameters ---------------
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, stat(MPI_STATUS_SIZE)
    logical                              :: flag
    integer(kind=i4_kind)                :: message(1 + JLENGTH)
    !------------ Executable code --------------------------------

    do ! while MPI_IPROBE(..., flag, ...) returns flag=.true.
        ! check for any message
        call MPI_IPROBE(MPI_ANY_SOURCE, MSGTAG, comm_world, flag, stat, ierr)
        ASSERT(ierr==MPI_SUCCESS)
        if ( .not. flag ) exit ! while loop

        call MPI_RECV(message, 1+JLENGTH, MPI_INTEGER4, MPI_ANY_SOURCE, MSGTAG,comm_world, stat,ierr)
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
            call print_statistics()
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
    use dlb_common, only: print_statistics
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in)    :: proc
    !** End of interface *****************************************

    if (.not. has_last_done(proc)) RETURN

    ! there will be only a send to the other procs, telling them to terminate
    ! thus the termination_master sets its termination here
    terminated = .true.
    if (n_procs > 1) then
      call print_statistics()
      call send_termination()
    endif
    call end_requests(requ2)
    call end_communication()
  end subroutine check_termination

  function local_tgetm(m, jobs, already_done) result(ok)
    !  Purpose: tries to get m jobs from the local job_storage.
    !           Returns true if either: it got some jobs
    !                        or: there are no jobs in the storage
    !                            as this proc is the only one to put
    !                            jobs in the storage, there is no use
    !                            in waiting here
    !           returns only false if it has been locked by others while
    !           accessing a non-empty storage.
    !           if true also:
    !           local try for get m jobs, m is number of requested jobs
    !           if there are enough my_jobs will give their number back
    !           (fewer if there are not m left), if there is 0 jobs
    !           given back, there is no more in the storage
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: reserve_workm
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(i4_kind), intent(in)  :: m
    integer(i4_kind), intent(inout)  :: already_done
    integer(i4_kind), intent(out) :: jobs(:) ! (JLENGTH)
    logical                       :: ok ! result
    !** End of interface *****************************************

    integer(i4_kind) :: ierr, locked

    integer(i4_kind) :: local(JLENGTH)
    integer(i4_kind) :: remaining(JLENGTH)

    ASSERT(size(jobs)==JLENGTH)

    jobs = set_empty_job()

    !
    ! Acquire MPI-lock on local storage:
    !
    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, my_rank, 0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    ! each proc which ones to acces the memory, sets his point to 1
    ! (else they are all 0), so if the sum is more than 0, at least one
    ! proc tries to get the memory, the one, who got locked = 0 has the right
    ! to work
    local(:) = job_storage(:JLENGTH) ! first JLENGTH hold the job

    !
    ! See if someone else holding the user-lock:
    !
    locked = sum(job_storage(JLENGTH+1:))

    !
    ! Below are 2x2 = 4 branches roughly corresponding to the
    ! four possible cases of (locked, empty(local)) tuple.
    ! In three of these cases ok = .true. in one ... (guess what).
    !
    ! FIXME: too difficult to reason about this code.
    !
    if ( locked > 0 ) then
        !
        ! Someone else is holding user-lock on local storage:
        !
        call time_stamp("blocked by lock contension",2)

        ! find out if it makes sense to wait:
        if ( empty(local) ) then
            ok = .true.
            call time_stamp("blocked, but empty",2)
            ! True means we got all jobs we can from the storage, there is no need
            ! to wait for an locked access, as we can see this by are mere read,
            ! no need for write, thus no need to cycle around

            !
            ! Finished a job interval, thus someone (local(JOWNER)) has to recrease
            ! its my_resp, local(JRIGHT) tells together with start_jobs how many jobs
            ! have been done
            !
            call report_or_store(local, already_done)
        else
            ok = .false.
            jobs(JRIGHT) = jobs(JLEFT) ! non actuell informations, make them invalid
        endif

    else
        !
        ! Acquired user-lock on local storage:
        !
        ok = .true.
        call time_stamp("free",2)

        !
        ! Try taking a slice from (a copy of) the local storage:
        !
        if ( steal_local(m, local, remaining, jobs) ) then
            !
            ! This shoul be better done at a single place:
            !
            already_done = already_done + length(jobs)
            !
            ! Store remaining jobs:
            !
            job_storage(:JLENGTH) = remaining(:)
        else
            ! no more jobs!
            ASSERT(empty(local))
            !
            ! see above
            !
            call report_or_store(local, already_done)
        endif

        !
        ! Release user-lock:
        !
        job_storage(JLENGTH+1:) = 0
    endif
    !
    ! Release MPI-lock:
    !
    call MPI_WIN_UNLOCK(my_rank, win, ierr)
    call time_stamp("release",3)
    ASSERT(ierr==MPI_SUCCESS)
  end function local_tgetm

  subroutine report_or_store(my_jobs, already_done)
    !  Purpose: If a job is finished, this cleans up afterwards
    !           Needed for termination algorithm, there are two
    !           cases, it was a job of the own responsibilty or
    !           one from another, first case just change my number
    !            second case, send to victim, how many of his jobs
    !            were finished
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in  ) :: my_jobs(JLENGTH)
    integer(kind=i4_kind), intent(inout) :: already_done
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: num_jobs_done
    !------------ Executable code --------------------------------
    !print *, time_stamp_prefix(MPI_Wtime()), "finished a job, now report or store", my_jobs
    ! my_jobs hold recent last point, as proc started from beginning and
    ! steal from the back, this means that from the initial starting point on
    ! (stored in start_job) to this one, all jobs were done
    ! if my_jobs(JRIGHT)/= start-job(JRIGHT) someone has stolen jobs
    num_jobs_done = already_done
    if (my_jobs(JOWNER) == my_rank) then
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
      call report_job_done(num_jobs_done, my_jobs(JOWNER))
    endif
  end subroutine report_or_store

  function rmw_tgetm(m, rank, jobs, already_done) result(ok)
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
    implicit none
    integer(i4_kind), intent(in)  :: m, rank
    integer(i4_kind), intent(inout)  :: already_done
    integer(i4_kind), intent(out) :: jobs(:) ! (JLENGTH)
    logical                       :: ok ! result
    ! *** end of interface ***

    integer(i4_kind) :: stolen_jobs(JLENGTH)
    integer(i4_kind) :: local_jobs(JLENGTH)

    !
    ! NOTE: size(jobs) is JLENGTH, (jobs(JLEFT), jobs(JRIGHT)] specify
    ! the interval, the rest is metadata that should be copied
    ! around.
    !
    ASSERT(size(jobs)==JLENGTH)

    !
    ! remote_jobs are fetched from rank, split into
    !   1) remaining_jobs --- are returned back to rank
    !   2) stolen_jobs    --- for local processing,
    !      these are further split into
    !      a) jobs        --- are output of this sub.
    !      b) local_jobs  --- are stored in local storage
    !

    ok = try_read_modify_write(rank, steal_remote, m, stolen_jobs)

    if ( .not. ok ) then
        jobs = set_empty_job()
        RETURN
    endif

    already_done = 0
    !
    ! "Stealing" from myself:
    !
    ok = steal_local(m, stolen_jobs, local_jobs, jobs)
    ASSERT(ok)

    !
    ! This shoul be better done at a single place:
    !
    already_done = already_done + length(jobs)

    !
    ! This stores the rest for later delivery in the OWN job-storage
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

  function try_read_modify_write(rank, modify, iarg, jobs) result(ok)
    !
    ! Perform read-modify-write sequence
    !
    implicit none
    integer(i4_kind), intent(in)  :: rank, iarg
    integer(i4_kind), intent(out) :: jobs(:) ! (JLENGTH)
    logical                       :: ok ! result
    !
    ! Input argument "modify(...)" is a procedure that takes an integer argument
    ! "iarg", a jobs descriptor "orig" and produces two job descriptors
    ! "left" and "right" of which one is written back in a "write_and_unloc"
    ! step and the other is returned as the result of "try_read_modify_write".
    ! If the (logical) return status of "modify(...)" is false, the
    ! "write" step of "read-modify-write" is aborted:
    !
    interface
        logical function modify(iarg, orig, left, right)
            use dlb_common, only: i4_kind
            implicit none
            integer(i4_kind), intent(in)  :: iarg
            integer(i4_kind), intent(in)  :: orig(:) ! (JLENGTH)
            integer(i4_kind), intent(out) :: left(:) ! (JLENGTH)
            integer(i4_kind), intent(out) :: right(:) ! (JLENGTH)
        end function modify
    end interface
    ! *** end of interface ***

    integer(i4_kind) :: orig(JLENGTH)
    integer(i4_kind) :: left(JLENGTH)

    !
    ! "orig" job descriptor is fetched from "rank", split into
    !   1) "left"  --- which written back to "rank"
    !   2) "right" --- that is to be returned in "jobs"

    !
    ! NOTE: size(jobs) is JLENGTH, (jobs(JLEFT), jobs(JRIGHT)] specify
    ! the interval, the rest is metadata that should be copied
    ! around.
    !
    ASSERT(size(jobs)==JLENGTH)

    jobs = set_empty_job()

    !
    ! Try reading job info from "rank" into "orig" job descriptor, returns false
    ! if not successfull:
    !
    ok = try_lock_and_read(rank, orig)

    ! Return if locking failed:
    if ( .not. ok ) RETURN

    ! Modify step:
    ok = modify(iarg, orig, left, jobs)

    !
    ! If "modify" step failed then skip "modify-write" in "read-modify-write",
    ! only do unlock:
    !
    if ( .not. ok ) then
        call unlock(rank)
        RETURN
    endif

    !
    ! Write back and release the lock:
    !
    call write_and_unlock(rank, left)

    !
    ! ... and return "jobs".
    !
  end function try_read_modify_write

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
    ! at the point C computed with the help of the legacy function
    ! "steal_work_for_rma(...)" as
    !
    !     C = B - steal_work_for_rma(m, remote)
    !
    ! NOTE: This function has to adhere to the interface of modify(...)
    ! argument in try_read_modify_write(...)
    !
    ! FIXME: the role of parameter "m" is not clear!
    !
    use dlb_common, only: i4_kind, JLENGTH, steal_work_for_rma
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

    ok = .false. ! failure
    remaining = -1 ! junk
    stolen = -1 ! junk

    ! how much of the work should be stolen
    ! try to avoid breaking in job intervals smaller than m
    work = steal_work_for_rma(m, remote(:L_JOB))

    !
    ! We were told that nothing can be stolen --- report failure:
    !
    if ( work == 0 ) RETURN

    !
    ! Split the interval here:
    !

    c = remote(JLEFT) + work

    call split_at(c, remote, stolen, remaining)

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
    !     C = A + reserve_workm(m, local)
    !
    ! NOTE: This function has to adhere to the interface of modify(...)
    ! argument in try_read_modify_write(...)
    !
    use dlb_common, only: i4_kind, JLENGTH, reserve_workm
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

    ! The give_grid function needs only up to m jobs at once, thus
    ! divide the jobs
    work = reserve_workm(m, local(:L_JOB))

    ! split here:
    c = local(JLEFT) + work

    !
    ! Note the order of (stolen, remaining) --- left interval is stolen, right
    ! interval is remaining:
    !
    call split_at(c, local, stolen, remaining)

    ok = .not. empty(stolen)
  end function steal_local

  function try_lock_and_read(rank, jobs) result(ok)
    !
    ! Try getting a lock indexed by rank and if that succeeds,
    ! read data into jobs(1:2)
    !
    use dlb_common, only: my_rank, n_procs
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(i4_kind), intent(in)  :: rank
    integer(i4_kind), intent(out) :: jobs(:) ! (JLENGTH)
    logical                       :: ok ! result
    ! *** end of interface ***

    integer(i4_kind)          :: ierr
    integer(i4_kind), target  :: win_data(jobs_len) ! FIXME: why target?
    integer(MPI_ADDRESS_KIND) :: displacement
    integer(MPI_ADDRESS_KIND), parameter :: zero = 0

    ASSERT(size(jobs)==JLENGTH)

    ok = .false.
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

    displacement = my_rank + JLENGTH ! for getting it in the correct kind

    call MPI_PUT(ON, 1, MPI_INTEGER4, rank, displacement, 1, MPI_INTEGER4, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    call MPI_WIN_UNLOCK(rank, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    !
    ! FIXME: this is the data item that was GET/PUT in the same epoch:
    !
    win_data(my_rank + 1 + JLENGTH) = 0

    !
    ! Check if there are any procs, saying that they want to acces the memory
    !
    if ( sum(win_data(JLENGTH+1:)) == 0 ) then
        ok = .true.
        jobs(:) = win_data(1:JLENGTH)
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
    integer(MPI_ADDRESS_KIND), parameter :: displacement = JLENGTH ! long int
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
    implicit none
    integer(i4_kind), intent(in) :: rank
    integer(i4_kind), intent(in) :: jobs(:)
    ! *** end of interface ***

    integer(i4_kind)          :: ierr
    integer(i4_kind), target  :: win_data(jobs_len) ! FIXME: why target?
    integer(MPI_ADDRESS_KIND), parameter :: zero = 0
    !------------ Executable code --------------------------------

    ASSERT(size(jobs)==JLENGTH)
    ASSERT(JLENGTH+n_procs==jobs_len)

    !
    ! Real data:
    !
    win_data(1:JLENGTH) = jobs(:)

    !
    ! Zero fields responsible for locking:
    !
    win_data(JLENGTH+1:) = 0

    !
    ! PUT data and overwrite lock data structure with zeros in one epoch:
    !
    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, rank, 0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    if (rank == my_rank) then
      job_storage = win_data
    else
      call MPI_PUT(win_data, size(win_data), MPI_INTEGER4, rank, zero, size(win_data), MPI_INTEGER4, win, ierr)
      ASSERT(ierr==MPI_SUCCESS)
    endif

    call MPI_WIN_UNLOCK(rank, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)
  end subroutine write_and_unlock

  subroutine read_unsafe(rank, jobs)
    !
    ! Read jobs data structure without acquiring user-level lock
    !
    use dlb_common, only: my_rank
    implicit none
    integer(i4_kind), intent(in)  :: rank
    integer(i4_kind), intent(out) :: jobs(JLENGTH)
    ! *** end of interface ***

    integer(i4_kind) :: ierr

    !
    ! FIXME: so far only used to store into the local datastructure:
    !
    ASSERT(rank==my_rank)

    ! FIXME: less strict locking?
    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, rank, 0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    jobs(:) = job_storage(:JLENGTH)

    call MPI_WIN_UNLOCK(rank, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)
  end subroutine read_unsafe

  subroutine write_unsafe(rank, jobs)
    !
    ! Owerwrite jobs data structure without acquiring user-level lock
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
    ASSERT(size(jobs)==JLENGTH)

    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, rank, 0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    job_storage(:JLENGTH) = jobs(:)

    call MPI_WIN_UNLOCK(rank, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)
  end subroutine write_unsafe

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
    integer(kind=i4_kind)                :: start_job(JLENGTH)
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
    already_done = 0
    remember_last = start_job(JLEFT)

    ! needed for termination
    my_resp_start = start_job(JRIGHT) - start_job(JLEFT)

    ! Job storage holds all the jobs currently in use
    ! direct storage, because in own memory used here
    call write_and_unlock(my_rank, start_job)
  end subroutine dlb_setup

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

  logical function storage_is_empty(rank)
    implicit none
    integer(i4_kind), intent(in) :: rank
    ! *** end of interface ***

    integer(i4_kind) :: jobs(JLENGTH)

    call read_unsafe(rank, jobs)

    storage_is_empty = empty(jobs)
  end function storage_is_empty

end module dlb_impl
