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
  use dlb_common, only: my_rank, n_procs, termination_master
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

  ! store all the jobs, belonging to this processor and the lock struct here:
  integer(kind=i4_kind), pointer    :: job_storage(:) ! (jobs_len)

  integer(kind=i4_kind)             :: already_done ! stores how many jobs the proc has done from
                                      ! the current interval
  logical                           :: terminated!, finishedjob ! for termination algorithm
  integer(kind=i4_kind),allocatable :: requ2(:) ! requests of send are stored till relaese
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
    type(c_ptr) :: c_job_pointer ! needed for MPI RMA handling (for c pseudo pointer)
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
    !  Purpose: Returns next bunch of up to n jobs.
    !  If slice(2) <= slice(1) there are no more jobs there, else returns the jobs
    !  done by the procs should be slice(1) + 1 to slice(2) in
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
    integer(i4_kind) :: rank, ierr
    integer(i4_kind) :: jobs(JLENGTH)
    integer(i4_kind) :: stolen_jobs(JLENGTH)
    integer(i4_kind) :: local_jobs(JLENGTH)
    logical :: ok

    !
    ! First try to get jobs from local storage
    !
    ok = .false.
    do while ( .not. storage_is_empty(my_rank) )
        !
        ! Stealing from myself:
        !
        ok = try_read_modify_write(my_rank, steal_local, n, jobs)
        if ( ok ) exit ! while loop
    enddo

    if ( .not. ok ) then
        !
        ! Apparently the local storage is empty:
        !
        ASSERT(storage_is_empty(my_rank))

        !
        ! This is the place to report how much of the work
        ! has been completed so far and reset the counters.
        ! For a report we need an "owner" of the jobs that we were
        ! executing, get this by looking into the (empty) local queue,
        ! that is suposed to keep a valid owner field:
        !
        call read_unsafe(my_rank, jobs)
        call report_or_store(jobs(JOWNER), already_done)
        already_done = 0

        !
        ! Try stealing from random workers
        ! until termination is announced:
        !
        do while ( .not. check_messages() )
            ! check like above but also for termination message from termination master

            rank = select_victim(my_rank, n_procs)

            !
            ! try to get jobs from rank, if rank's memory occupied by another or contains
            ! nothing to steal, this will return false:
            !
            ok = try_read_modify_write(rank, steal_remote, n, stolen_jobs)
            if ( ok ) exit ! while loop
        enddo

        !
        ! Stealing from remote succeded:
        !
        if ( ok ) then
            !
            ! Early "stealing" from myself --- do it before storing
            ! the stolen interval in publically availbale storage.
            ! Ensures progress, avoids stealing back-and-forth.
            !
            ok = steal_local(n, stolen_jobs, local_jobs, jobs)
            ASSERT(ok)

            !
            ! This stores the rest for later delivery in the OWN job-storage
            !
            call write_unsafe(my_rank, local_jobs)
        endif
    endif

    !
    ! Increment the counter for delivered jobs.
    ! It is somewhat too early to declare them "completed",
    ! but once delivered to "userspace" these jobs cannot
    ! be stolen anymore and may be considered
    ! "scheduled for execution":
    !
    already_done = already_done + length(jobs)

    ! NOTE: named constants are now known outside.
    ! Return only the start and endpoint of the job slice:
    slice(1) = jobs(JLEFT)
    slice(2) = jobs(JRIGHT)

    !
    ! Here a syncronization feature, the very last call to dlb_give_more(...)
    ! does not return until all workers pass this barrier.
    !
    ! FIXME: this is a speciality of RMA implementation, to avoid
    ! the case of others stealing unrelated jobs from the next round of
    ! call dlb_setup(...); do while ( dlb_give_more(...) ) ...
    !
    if ( empty(jobs) ) then
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
    use dlb_common, only: report_by, reports_pending &
        , end_requests, send_resp_done, end_communication
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
            ASSERT(stat(MPI_SOURCE)/=my_rank)

            !
            ! Handle a fresh cumulative report:
            !
            call report_by(stat(MPI_SOURCE), message(2))

            if ( reports_pending() == 0) then
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
    use dlb_common, only: has_last_done, send_termination, end_requests, end_communication
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

  subroutine report_or_store(owner, num_jobs_done)
    !  Purpose: If a job is finished, this cleans up afterwards
    !           Needed for termination algorithm, there are two
    !           cases, it was a job of the own responsibilty or
    !           one from another, first case just change my number
    !            second case, send to victim, how many of his jobs
    !            were finished
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: report_to, reports_pending, send_resp_done
    implicit none
    integer(i4_kind), intent(in) :: owner
    integer(i4_kind), intent(in) :: num_jobs_done
    !** End of interface *****************************************

    !print *, time_stamp_prefix(MPI_Wtime()), "finished a job, now report or store", my_jobs
    ! my_jobs hold recent last point, as proc started from beginning and
    ! steal from the back, this means that from the initial starting point on
    ! (stored in start_job) to this one, all jobs were done
    ! if my_jobs(JRIGHT)/= start-job(JRIGHT) someone has stolen jobs

    !
    ! Report the number of scheduled jobs:
    !
    call report_to(owner, num_jobs_done)

    !
    ! Here we apparenly try to detect the termination early:
    !
    if ( owner == my_rank ) then
      if ( reports_pending() == 0) then
        if (my_rank == termination_master) then
          call check_termination(my_rank)
        else
          call send_resp_done(requ2)
        endif
      endif
    endif
  end subroutine report_or_store

  function try_read_modify_write(rank, modify, iarg, jobs) result(ok)
    !
    ! Perform read-modify-write sequence
    !
    use dlb_common, only: set_empty_job
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
    use dlb_common, only: i4_kind, JLENGTH, steal_work_for_rma, split_at
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
    use dlb_common, only: i4_kind, JLENGTH, reserve_workm, split_at
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
    use dlb_common, only: dlb_common_setup, set_start_job
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: job(L_JOB)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: start_job(JLENGTH)
    !------------ Executable code --------------------------------

    ! these variables are for the termination algorithm
    terminated = .false.

    ! set starting values for the jobs, needed also in this way for
    ! termination, as they will be stored also in the job_storage
    ! start_job should only be changed if all current jobs are finished
    ! and there is a try to steal new ones
    start_job = set_start_job(job)

    call dlb_common_setup(start_job(JRIGHT) - start_job(JLEFT))

    already_done = 0

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
