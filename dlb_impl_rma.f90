!===============================================================
! Public interface of module
!===============================================================
module dlb_impl
  !---------------------------------------------------------------
  !
  !  Purpose: takes care about dynamical load balancing, uses an RMA
  !           (MPI) object to store the job informations (only the
  !           number of the job) and allows other routines to steal
  !           them, if they have no own left unfortunatelly the MPI
  !           one-sided works different on different systems, this
  !           routine is only useful if the RMA can be accessed while
  !           the target proc is not in MPI-context
  !
  ! Interface:
  !
  !           call dlb_init() - once before first acces
  !
  !           call dlb_setup(init_job) - once every time a dlb should
  !              be used, init_job should be the part of the current
  !              proc of an initial job distribution
  !
  !           dlb_give_more(n, jobs) - n should be the number of jobs
  !              requested at once, the next time dlb_give_more is
  !              called again, all jobs from jobs should be finished,
  !              jobs are at most n, they are jsut the number of the
  !              jobs, which still have to be transformed between each
  !              other, it should be done the error slice from jobs(0)
  !              +1 to jobs(1) if dlb_give_more returns
  !              jobs(0)==jobs(1) there are on no proc any jobs left
  !
  !          The algorithm: it follows in principle the Algorithm 2
  !              from the first reference, each proc has an local RMA
  !              memory containing job informations, it takes jobs
  !              form the left (stp values are increased, till stp ==
  !              ep) then it searches the job storages of the other
  !              procs for jobs left, this is done by a code similar
  !              to the one descriebed in the second reference, but
  !              here on each proc and without specifieng the ranges
  !              to work on stolen work is put in the own storage,
  !              after taking the current job
  !
  !          Termination algorithm: called (at least once) "Fixed
  !              Energy Distributed Termination Algorithm" to avoid
  !              confusion, here the term "energy" is not used,
  !              talking about respoinsibility (resp) instead, every
  !              system starts with a part of responsibility given to
  !              him, if procs steal from him, they have later to send
  !              him a message saying how many of his jobs, they have
  !              done, they always report to the proc who had the resp
  !              first, thus source is given away with job each proc
  !              lowers his resp about the values given back from any
  !              proc and about the jobs he has done himself, when
  !              finished them, if he has his resp at zero, he sends a
  !              message to termination_master if termination master
  !              has a message from all the procs, that their resp is
  !              0 he sends a message to all procs, telling them to
  !              terminated the algorithm
  !
  !  Module called by: ...
  !
  !
  !  References: "Scalable Work Stealing", James Dinan, D. Brian
  !              Larkins, Sriram Krishnamoorthy, Jarek Nieplocha,
  !              P. Sadayappan, Proc. of 21st intl. Conference on
  !              Supercomputing (SC).  Portland, OR, Nov. 14-20, 2009
  !              (for work stealing algorithm)
  !
  !              "Implementing Byte-Range Locks Using MPI One-Sided
  !              Communication", Rajeev Thakur, Robert Ross, and
  !              Robert Latham (in EuroPVM/MPI) (read modify write
  !              algorithm with the same ideas)
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
  use dlb_common, only: i4_kind, comm_world
  use dlb_common, only: time_stamp ! for debug only
  use dlb_common, only: DONE_JOB, NO_WORK_LEFT, RESP_DONE, JLENGTH, L_JOB, JOWNER, JLEFT, JRIGHT
  use dlb_common, only: my_rank, n_procs, termination_master
  use dlb_common, only: main_wait_all, main_wait_max, main_wait_last
  use dlb_common, only: max_work, last_work, average_work, num_jobs
  use dlb_common, only: dlb_time, min_work, second_last_work
  use dlb_common, only: timer_give_more, timer_give_more_last
  use dlb_common, only: i4_kind_mpi, i4_kind_1
  implicit none
  save            ! save all variables defined in this module
  private         ! by default, all names are private
  !== Interrupt end of public interface of module =================
  ! Program from outside might want to know the thread-safety-level required form DLB
  integer(kind=i4_kind_1), parameter, public :: DLB_THREAD_REQUIRED = MPI_THREAD_SINGLE

  !------------ public functions and subroutines ------------------
  public :: dlb_init, dlb_finalize, dlb_setup, dlb_give_more

  !================================================================
  ! End of public interface of module
  !================================================================

  !------------ Declaration of types ------------------------------

  !------------ Declaration of constants and variables ----
  integer(kind=i4_kind), parameter  :: ON = 1, OFF = 0 ! For read-modify-write
  integer(kind=i4_kind)             :: jobs_len  ! Length of complete jobs storage
  integer(kind=i4_kind_1)            :: win ! for the RMA object

  ! Read Modify Write Error codes:
  integer(kind=i4_kind), parameter  :: RMW_SUCCESS = 0
  integer(kind=i4_kind), parameter  :: RMW_LOCKED = 1
  integer(kind=i4_kind), parameter  :: RMW_MODIFY_ERROR = 2

  ! store all the jobs, belonging to this processor and the lock struct here:
  integer(kind=i4_kind), pointer    :: job_storage(:) ! (jobs_len)

  integer(kind=i4_kind)             :: already_done ! stores how many jobs the proc has done from
                                      ! the current interval
  logical                           :: terminated!, finishedjob ! for termination algorithm
  integer(kind=i4_kind_1),allocatable :: requ2(:) ! requests of send are stored till relaese
  ! Variables need for debug and efficiency testing
  integer(kind=i4_kind)             :: many_tries, many_searches !how many times asked for jobs
  integer(kind=i4_kind)             :: many_locked, many_zeros, self_many_locked
  !----------------------------------------------------------------
  !------------ Subroutines ---------------------------------------
contains

  subroutine dlb_init(world)
    !
    !  Purpose: Initialization of the objects needed for running the
    !           dlb job scheduling, the most part is of setting up an
    !           RMA object with MPI to have it ready to use for the
    !           rest of the dlb model. It may be set up only once,
    !           even if there are several dlbs wanted, the initalizing
    !           of a specific run should be done with dlb_setup It is
    !           also recommended to call this subroutine only once as
    !           it needs parallelization of all processes
    !
    use iso_c_binding, only: c_ptr, c_f_pointer
    use dlb_common, only: dlb_common_init, OUTPUT_BORDER
    use dlb_common, only: set_empty_job
    implicit none
    integer(kind=i4_kind_1), intent(in) :: world
    ! *** end of interface ***

    type(c_ptr) :: c_job_pointer ! needed for MPI RMA handling (for c pseudo pointer)
    integer(kind=i4_kind_1)              :: sizeofint
    integer(kind=i4_kind_1)              :: ierr
    integer(kind=MPI_ADDRESS_KIND)       :: size_alloc, size_all

    ! some aliases, highly in use during the whole module
    call dlb_common_init(world)

    ! find out, how much there is to store and allocate (with MPI) the
    ! memory
    call MPI_TYPE_EXTENT(i4_kind_mpi, sizeofint, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    jobs_len = JLENGTH + n_procs
    size_alloc = jobs_len * sizeofint

    call MPI_ALLOC_MEM(size_alloc, MPI_INFO_NULL, c_job_pointer, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    ! this connects a c like pointer with our job_storage, as MPI can
    ! not handle our pointers as they are
    call c_f_pointer(c_job_pointer, job_storage, [jobs_len])

    size_all = jobs_len * sizeofint ! for having it in the correct kind

    ! win (an integer) is set up, so that the RMA-processes can call
    ! on it they will then get acces to the local stored job_storage
    ! of the corresponding proc
    call MPI_WIN_CREATE(job_storage, size_all, sizeofint, MPI_INFO_NULL, &
                        comm_world, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    ! needed to make certain, that in the next steps, all the procs
    ! have all the informations
    call MPI_WIN_FENCE(0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    ! initalize the job storage: not yet any task but ensure no lock
    call write_and_unlock(my_rank, set_empty_job())

    ! if a dlb run would be too early after init, some processor might
    ! steal from an undefined storage, after this at least it can only
    ! steal empty jobs if a processor is not yet available
    call MPI_WIN_FENCE(0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    if (my_rank ==0 .and. 0 < OUTPUT_BORDER) then
        print *, "DLB init: using variant 'rma'"
        print *, "DLB init: This variant uses the remote memory access of MPI implementation"
        print *, "DLB init: it needs real asynchronous RMA actions by MPI for working properly"
    endif
  end subroutine dlb_init

  subroutine dlb_finalize()
    !
    !  Purpose: shuts down the dlb objects (especially job_storage)
    !  when it is not further needed, should be called, after ALL dlb
    !  runs have complete
    !
    use dlb_common, only: dlb_common_finalize
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind_1)              :: ierr
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
    !
    !  Purpose: Returns next bunch of up to n jobs.  If slice(2) <=
    !  slice(1) there are no more jobs there, else returns the jobs
    !  done by the procs should be slice(1) + 1 to slice(2) in the
    !  related job list first the jobs are tried to get from the local
    !  storage of the current proc, if there are no more, it will try
    !  to steal some more work from others, till the separate
    !  termination algorithm states, that everything is done
    !
    use dlb_common, only: select_victim, steal_local, steal_remote &
        , length, empty, OUTPUT_BORDER
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(i4_kind), intent(in)  :: n
    integer(i4_kind), intent(out) :: slice(2)
    !** End of interface *****************************************

    !------------ Declaration of local variables -----------------
    integer(i4_kind_1) :: rank
    integer(i4_kind_1) :: ierr
    integer(i4_kind) :: jobs(JLENGTH)
    integer(i4_kind) :: stolen_jobs(JLENGTH)
    integer(i4_kind) :: local_jobs(JLENGTH)
    integer(i4_kind) :: ok
    integer(i4_kind_1) :: owner
    logical          :: ok_logical
    double precision                     :: start_timer
    double precision                     :: start_timer_gm
    double precision,save                :: leave_timer = -1
    !------------ Executable code --------------------------------
    start_timer_gm = MPI_Wtime() ! for debugging
    if (num_jobs > 0) then ! for debugging
        second_last_work = last_work
        last_work = MPI_Wtime() - leave_timer
        if (last_work > max_work) max_work = last_work
        if (last_work < min_work) min_work = last_work
        average_work = average_work + last_work
    endif

    !
    ! First try to get jobs from local storage
    !

    ! Initial value should differ from RMW_SUCCESS:
    ok = -1
    do while ( .not. storage_is_empty(my_rank) )
        !
        ! Stealing from myself:
        !
        ok = try_read_modify_write(my_rank, steal_local, n, jobs)
        if ( ok == RMW_SUCCESS ) exit ! the while loop

        ! for debugging only:
        if ( ok == RMW_LOCKED ) self_many_locked = self_many_locked + 1
    enddo

    if ( ok /= RMW_SUCCESS ) then
        !
        ! Apparently the local storage is empty:
        !
        ASSERT(storage_is_empty(my_rank))

        !
        ! This is the place to report how much of the work has been
        ! completed so far and reset the counters.  For a report we
        ! need an "owner" of the jobs that we were executing, get this
        ! by looking into the (empty) local queue, that is suposed to
        ! keep a valid owner field:
        !
        many_searches = many_searches + 1 ! just for debugging
        call read_unsafe(my_rank, jobs)
        owner = jobs(JOWNER)
        call report_or_store(owner, already_done)
        already_done = 0

        !
        ! Try stealing from random workers until termination is
        ! announced:
        !
        start_timer = MPI_Wtime() ! for debugging
        do while ( .not. check_messages() )
            ! check like above but also for termination message from
            ! termination master

            rank = select_victim(my_rank, n_procs)

            !
            ! try to get jobs from rank, if ranks memory occupied by
            ! another or contains nothing to steal, this will return
            ! false:
            !
            many_tries = many_tries + 1 ! just for debugging
            ok = try_read_modify_write(rank, steal_remote, n, stolen_jobs)
            if ( ok == RMW_SUCCESS ) exit ! the while loop

            ! for debugging only:
            select case ( ok )
            case (RMW_LOCKED) ! for debugging separated
                ! Other processor holds the lock
                many_locked = many_locked + 1

            case (RMW_MODIFY_ERROR) ! for debugging separated
                ! In this case victims job storage was empty
                many_zeros = many_zeros + 1

            case default
                ! nothing
            end select
        enddo
        main_wait_last = MPI_Wtime() - start_timer ! for debugging
        ! store debugging information:
        main_wait_all = main_wait_all + main_wait_last
        if (main_wait_last > main_wait_max) main_wait_max = main_wait_last
        ! end store debugging information


        !
        ! Stealing from remote succeded:
        !
        if ( ok == RMW_SUCCESS ) then
            !
            ! Early "stealing" from myself --- do it before storing
            ! the stolen interval in publically availbale storage.
            ! Ensures progress, avoids stealing back-and-forth.
            !
            ok_logical = steal_local(n, stolen_jobs, local_jobs, jobs)
            ASSERT(ok_logical)

            !
            ! This stores the rest for later delivery in the OWN
            ! job-storage
            !
            call write_unsafe(my_rank, local_jobs)
        endif
    endif

    !
    ! Increment the counter for delivered jobs.  It is somewhat too
    ! early to declare them "completed", but once delivered to
    ! "userspace" these jobs cannot be stolen anymore and may be
    ! considered "scheduled for execution":
    !
    already_done = already_done + length(jobs)

    ! NOTE: named constants are not known outside.  Return only the
    ! start and endpoint of the job slice:
    slice(1) = jobs(JLEFT)
    slice(2) = jobs(JRIGHT)

    !
    ! Here a syncronization feature, the very last call to
    ! dlb_give_more(...)  does not return until all workers pass this
    ! barrier.
    !
    ! FIXME: this is a speciality of RMA implementation, to avoid the
    ! case of others stealing unrelated jobs from the next round of
    ! call dlb_setup(...); do while ( dlb_give_more(...) ) ...
    !
    if ( empty(jobs)) then
       dlb_time = MPI_Wtime() - dlb_time ! for debugging
       timer_give_more_last = MPI_Wtime() - start_timer_gm ! for debugging
       if (1 < OUTPUT_BORDER) then
           print *, my_rank, "tried", many_searches, "for new jobs and stealing", many_tries
           print *, my_rank, "was locked", many_locked, "got zero", many_zeros
           print *, my_rank, "was locked on own memory", self_many_locked
           write(*, '(I3, " M: waited (all, max, last)", G20.10, G20.10, G20.10) '), my_rank, &
                  main_wait_all, main_wait_max, main_wait_last
           write(*, '(I3, " M: work slices lasted (average, max, min)", G20.10, G20.10, G20.10)'), my_rank,&
              average_work/ num_jobs, max_work, min_work
           write(*, '(I3, " M: the two of my last work slices lasted", G20.10, G20.10)'), my_rank,&
                            second_last_work, last_work

           write(*, '(I3, " M: time spend in dlb", G20.10, "time processor was working " , G20.10)'), my_rank,&
              dlb_time, average_work
       endif
       call MPI_BARRIER(comm_world, ierr)
       ASSERT(ierr==MPI_SUCCESS)
    else
       leave_timer = MPI_Wtime() ! for debugging
       timer_give_more = timer_give_more + leave_timer - start_timer_gm ! for debugging
       num_jobs = num_jobs + slice(2) - slice(1) ! for debugging
    endif
    call time_stamp("dlb_give_more: exit",3)
  end subroutine dlb_give_more

  logical function check_messages()
    !
    !  Purpose: checks if any message has arrived, checks for
    !           messages: Someone finished stolen job slice. Someone
    !           has finished its responsibilty (only termination
    !           master). There are no more jobs (message from
    !           termination_master to finish)
    !
    use dlb_common, only: report_by, reports_pending &
        , end_requests, send_resp_done, end_communication
    use dlb_common, only: print_statistics
    use dlb_common, only: iprobe, recv
    implicit none
    !------------ Declaration of formal parameters ---------------
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind_1)              :: stat(MPI_STATUS_SIZE)
    integer(kind=i4_kind)                :: message(JLENGTH)
    integer(kind=i4_kind_1)              :: src, tag
    integer(kind=i4_kind_1)              :: your_rank
    !------------ Executable code --------------------------------

    ! check for any message
    do while ( iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, stat) )

        src = stat(MPI_SOURCE)
        tag = stat(MPI_TAG)

        ! FIXME: specialize receives depending on tag:
        call recv(message, src, tag, stat)

        select case ( tag )

        case ( DONE_JOB )
            ! someone finished stolen job slice
            ASSERT(message(1)>0)
            ASSERT(src/=my_rank)

            !
            ! Handle a fresh cumulative report:
            !
            call report_by(src, message(1))

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
            ASSERT(my_rank==termination_master)

            your_rank = message(1)
            call check_termination(your_rank)

        case ( NO_WORK_LEFT )
            ! termination message from termination master
            ASSERT(message(1)==0)
            ASSERT(src==termination_master)

            terminated = .true.
            call print_statistics()
            ! NOW all my messages HAVE to be complete, so close (without delay)
            call end_requests(requ2)
            call end_communication()

        case default
            ! This message makes no sense in this context:
            print *, "ERROR: got message with unexpected content:", message
            call abort()
        end select
    enddo
    check_messages = terminated

    if (terminated) then
      call time_stamp("TERMINATING!", 2)
    endif
  end function check_messages

  subroutine check_termination(proc)
    !
    !  Purpose: only on termination_master, checks if all procs have
    !           reported termination
    !
    use dlb_common, only: has_last_done, send_termination, end_requests, end_communication
    use dlb_common, only: print_statistics
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind_1), intent(in)    :: proc
    !** End of interface *****************************************

    if (.not. has_last_done(proc)) RETURN

    ! there will be only a send to the other procs, telling them to
    ! terminate thus the termination_master sets its termination here
    terminated = .true.

    ! debug prints:
    call print_statistics()

    ! This is used by termination master to announce termination:
    call send_termination()

    call end_requests(requ2)
    call end_communication()
  end subroutine check_termination

  subroutine report_or_store(owner, num_jobs_done)
    !
    !  Purpose: If a job is finished, this cleans up afterwards Needed
    !           for termination algorithm, there are two cases, it was
    !           a job of the own responsibilty or one from another,
    !           first case just change my number second case, send to
    !           victim, how many of his jobs were finished
    !
    use dlb_common, only: report_to, reports_pending, send_resp_done
    implicit none
    integer(i4_kind_1), intent(in) :: owner
    integer(i4_kind), intent(in) :: num_jobs_done
    !** End of interface *****************************************

    !print *, "finished a job, now report or store", my_jobs
    ! my_jobs hold recent last point, as proc started from beginning and
    ! steal from the back, this means that from the initial starting point on
    ! (stored in start_job) to this one, all jobs were done
    ! if my_jobs(JRIGHT)/= start-job(JRIGHT) someone has stolen jobs

    !
    ! Report the number of scheduled jobs, report_to() is not doing
    ! anything usefull if num_jobs_done == 0 anyway:
    !
    call report_to(owner, num_jobs_done)

    !
    ! Here we apparenly try to detect the termination early:
    !
    if ( owner == my_rank ) then
      if ( reports_pending() == 0 ) then
        if (my_rank == termination_master) then
          call check_termination(my_rank)
        else
          call send_resp_done(requ2)
        endif
      endif
    endif
  end subroutine report_or_store

  function try_read_modify_write(rank, modify, iarg, jobs) result(error)
    !
    ! Perform read-modify-write sequence returns error code, 0 means
    ! success, else describes reasons for failure
    !
    use dlb_common, only: set_empty_job
    implicit none
    integer(i4_kind_1), intent(in):: rank
    integer(i4_kind), intent(in)  :: iarg
    integer(i4_kind), intent(out) :: jobs(:) ! (JLENGTH)
    integer(i4_kind)              :: error ! resulting error code 0 == success
    !
    ! Input argument "modify(...)" is a procedure that takes an
    ! integer argument "iarg", a jobs descriptor "orig" and produces
    ! two job descriptors "left" and "right" of which one is written
    ! back in a "write_and_unloc" step and the other is returned as
    ! the result of "try_read_modify_write".  If the (logical) return
    ! status of "modify(...)" is false, the "write" step of
    ! "read-modify-write" is aborted:
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
    logical          :: ok
    !
    ! "orig" job descriptor is fetched from "rank", split into
    !   1) "left"  --- which written back to "rank"
    !   2) "right" --- that is to be returned in "jobs"

    !
    ! NOTE: size(jobs) is JLENGTH, (jobs(JLEFT), jobs(JRIGHT)] specify
    ! the interval, the rest is metadata that should be copied around.
    !
    ASSERT(size(jobs)==JLENGTH)

    jobs = set_empty_job()

    !
    ! Try reading job info from "rank" into "orig" job descriptor,
    ! returns false if not successfull:
    !
    error = try_lock_and_read(rank, orig)

    ! Return if locking failed:
    if ( error /= RMW_SUCCESS ) RETURN

    ! Modify step:
    ok = modify(iarg, orig, left, jobs)

    !
    ! If "modify" step failed then skip "modify-write" in
    ! "read-modify-write", only do unlock:
    !
    if ( .not. ok ) then
        call unlock(rank)
        ! error code, needed for debugging
        error = RMW_MODIFY_ERROR
        RETURN
    endif

    !
    ! Succeeded therefore no error, needed for more than only
    ! debugging:
    !
    error = RMW_SUCCESS

    !
    ! Write back and release the lock:
    !
    call write_and_unlock(rank, left)

    !
    ! ... and return "jobs".
    !
  end function try_read_modify_write

  function try_lock_and_read(rank, jobs) result(error)
    !
    ! Try getting a lock indexed by rank and if that succeeds, read
    ! data into jobs(1:2)
    !
    use dlb_common, only: my_rank, n_procs
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(i4_kind_1), intent(in)  :: rank
    integer(i4_kind), intent(out) :: jobs(:) ! (JLENGTH)
    integer(i4_kind)              :: error ! error code
    ! *** end of interface ***
    integer(i4_kind_1)          :: N
    integer(i4_kind_1)        :: ierr
    integer(i4_kind), target  :: win_data(jobs_len) ! FIXME: why target?
    integer(MPI_ADDRESS_KIND) :: displacement
    integer(MPI_ADDRESS_KIND), parameter :: zero = 0

    ASSERT(size(jobs)==JLENGTH)

    ! First GET-PUT round, MPI only ensures that after MPI_UNLOCK the
    ! MPI-RMA accesses are finished, thus modify has to be done out of
    ! this lock There are two function calls inside: one to get the
    ! data and check if it may be accessed second one to show to other
    ! procs, that this one is interested in modifing the data

    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, rank, 0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    ! divide the storage in pieces to avoid illegal two operations on the same storage in
    ! the same lock area
    ! consider that my_rank starts with 0
    ! get all before my own lock [...[{my_lock}...
    N =  my_rank + JLENGTH ! Number of tasks to get before own lock place
    call MPI_GET(win_data(1:N), N, i4_kind_mpi, rank, zero, N, i4_kind_mpi, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)
    ! get all after my own lock ...{my_lock}]...]
    displacement = my_rank + JLENGTH + 1
    N = (jobs_len-JLENGTH) - my_rank -1
    if (N > 0) then
        call MPI_GET(win_data(jobs_len-N+1:jobs_len), N, i4_kind_mpi, rank, displacement, &
                N, i4_kind_mpi, win, ierr)
        ASSERT(ierr==MPI_SUCCESS)
    endif

    ! set my own lock ...[{my_lock}]...
    N = 1
    displacement = my_rank + JLENGTH ! for getting it in the correct kind
    call MPI_PUT(ON, N, i4_kind_mpi, rank, displacement, N, i4_kind_mpi, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    call MPI_WIN_UNLOCK(rank, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    win_data(my_rank + 1 + JLENGTH) = OFF
    !
    ! Check if there are any procs, saying that they want to acces the
    ! memory
    !
    if ( all(win_data(JLENGTH+1:) == OFF) ) then
        jobs(:) = win_data(1:JLENGTH)
        error = RMW_SUCCESS
    else
        jobs(:) = -1 ! junk
        error = RMW_LOCKED
        ! Do nothing. Dont even try to undo our attempt to acquire the
        ! lock as the one that is holding the lock will overwrite the
        ! lock data structure when finished. See
        ! unlock/write_and_unlock below.
    endif
  end function try_lock_and_read

  subroutine unlock(rank)
    !
    ! Release previousely acquired lock indexed by rank.
    !
    use dlb_common, only: n_procs
    implicit none
    integer(i4_kind_1), intent(in) :: rank
    ! *** end of interface ***

    integer(i4_kind_1) :: ierr
    integer(i4_kind), target :: zeros(n_procs) ! FIXME: why target?
    integer(MPI_ADDRESS_KIND), parameter :: displacement = JLENGTH ! long int

    zeros(:) = OFF

    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, rank, 0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    call MPI_PUT(zeros, size(zeros), i4_kind_mpi, rank, displacement, size(zeros), i4_kind_mpi, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    call MPI_WIN_UNLOCK(rank, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)
  end subroutine unlock

  subroutine write_and_unlock(rank, jobs)
    !
    ! Update job range description and release previousely acquired
    ! lock.
    !
    implicit none
    integer(i4_kind_1), intent(in) :: rank
    integer(i4_kind), intent(in) :: jobs(:)
    ! *** end of interface ***

    integer(i4_kind_1)        :: ierr
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
    win_data(JLENGTH+1:) = OFF

    !
    ! PUT data and overwrite lock data structure with zeros in one
    ! epoch:
    !
    call MPI_WIN_LOCK(MPI_LOCK_EXCLUSIVE, rank, 0, win, ierr)
    ASSERT(ierr==MPI_SUCCESS)

    !
    ! FIXME: should we avoid two branches and always invoke
    !        MPI_PUT(..., rank, ...) even for rank == my_rank?
    !
    if (rank == my_rank) then
      job_storage = win_data
    else
      call MPI_PUT(win_data, size(win_data), i4_kind_mpi, rank, zero, size(win_data), i4_kind_mpi, win, ierr)
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
    integer(i4_kind_1), intent(in)  :: rank
    integer(i4_kind), intent(out) :: jobs(JLENGTH)
    ! *** end of interface ***

    integer(i4_kind_1) :: ierr

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
    integer(i4_kind_1), intent(in) :: rank
    integer(i4_kind), intent(in)   :: jobs(:)
    ! *** end of interface ***

    integer(i4_kind_1) :: ierr

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
    !
    !  Purpose: initialization of a dlb run, each proc should call it
    !           with inital jobs. The inital jobs should be a static
    !           distribution of all available jobs, each job should be
    !           given to one and only one of the procs jobs should be
    !           given as a job range (STP, EP), where all jobs should
    !           be the numbers from START to END, with START <= STP <=
    !           EP <= END
    !
    use dlb_common, only: dlb_common_setup, set_start_job, length
    implicit none
    integer(i4_kind), intent(in) :: job(:) ! (2)
    ! *** end of interface ***

    integer(i4_kind) :: start_job(JLENGTH)

    ASSERT(size(job)==2)
    ASSERT(2==L_JOB)

    dlb_time = MPI_Wtime() ! for debugging

    ! these variables are for the termination algorithm
    terminated = .false.

    ! set starting values for the jobs, needed also in this way for
    ! termination, as they will be stored also in the job_storage
    ! start_job should only be changed if all current jobs are
    ! finished and there is a try to steal new ones
    start_job = set_start_job(job)

    call dlb_common_setup(length(start_job))

    ! initalize some debug variables
    many_tries = 0
    many_searches = 0
    many_locked = 0
    self_many_locked = 0
    many_zeros = 0
    ! end initalizing debug variables

    already_done = 0

    ! Job storage holds all the jobs currently in use direct storage,
    ! because in own memory used here Other processors might already
    ! be active and trying to steal thus lock might already be set
    ASSERT (storage_is_empty(my_rank))
    call write_unsafe(my_rank, start_job)
  end subroutine dlb_setup

  logical function storage_is_empty(rank)
    use dlb_common, only: empty, JLENGTH
    implicit none
    integer(i4_kind_1), intent(in) :: rank
    ! *** end of interface ***

    integer(i4_kind) :: jobs(JLENGTH)

    call read_unsafe(rank, jobs)

    storage_is_empty = empty(jobs)
  end function storage_is_empty

end module dlb_impl
