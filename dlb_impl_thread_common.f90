module dlb_impl_thread_common
# include "dlb.h"
  use iso_c_binding
  use dlb_common, only: dlb_common_init
  use dlb_common, only: i4_kind, n_procs, termination_master
  use dlb_common, only: time_stamp ! for debug only
  use dlb_common, only: JLENGTH, JRIGHT, JLEFT, NO_WORK_LEFT
  use dlb_common, only: has_last_done, set_empty_job, add_request, send_termination
  use dlb_common, only: masterserver
  implicit none

  interface
    !
    ! These interfaces need to be consistent with implementations
    ! in thread_wrapper.c
    !
    subroutine th_inits() bind(C)
    end subroutine th_inits

    subroutine th_exit() bind(C)
    end subroutine th_exit

    subroutine th_join_all() bind(C)
    end subroutine th_join_all

    subroutine th_create_all() bind(C)
    end subroutine th_create_all

    subroutine th_create_one() bind(C)
    end subroutine th_create_one

    subroutine th_mutex_lock(lock) bind(C)
      use iso_c_binding
      implicit none
      integer(C_INT), intent(in) :: lock
    end subroutine th_mutex_lock

    subroutine th_mutex_unlock(lock) bind(C)
      use iso_c_binding
      implicit none
      integer(C_INT), intent(in) :: lock
    end subroutine th_mutex_unlock

    subroutine th_cond_signal(condition) bind(C)
      use iso_c_binding
      implicit none
      integer(C_INT), intent(in) :: condition
    end subroutine th_cond_signal

    subroutine th_cond_wait(condition, mutex) bind(C)
      use iso_c_binding
      implicit none
      integer(C_INT), intent(in) :: condition, mutex
    end subroutine th_cond_wait

    subroutine th_rwlock_rdlock(rwlock) bind(C)
      use iso_c_binding
      implicit none
      integer(C_INT), intent(in) :: rwlock
    end subroutine th_rwlock_rdlock

    subroutine th_rwlock_wrlock(rwlock) bind(C)
      use iso_c_binding
      implicit none
      integer(C_INT), intent(in) :: rwlock
    end subroutine th_rwlock_wrlock

    subroutine th_rwlock_unlock(rwlock) bind(C)
      use iso_c_binding
      implicit none
      integer(C_INT), intent(in) :: rwlock
    end subroutine th_rwlock_unlock

    ! and one routine to use the c timings:
    subroutine c_sleep(time) bind(C) ! in microseconds
      use iso_c_binding
      implicit none
      integer(C_INT), intent(in) ::time
    end subroutine
  end interface

  public :: clear_up

  !------------ Declaration of types ------------------------------

  !------------ Declaration of constants and variables ----
  integer(kind=i4_kind), parameter, public  :: JOBS_LEN = JLENGTH  ! Length of complete jobs storage
  ! IDs of mutexes, use base-0 indices:
  integer(kind=i4_kind), parameter, public :: LOCK_JS   = 0
  integer(kind=i4_kind)             :: job_storage(jobs_len) ! store all the jobs, belonging to this processor
  logical                           :: terminated ! for termination algorithm
  integer(kind=i4_kind), allocatable :: messagesJA(:,:)

  contains
    subroutine rdlock()
      implicit none

      call th_rwlock_rdlock(0)
    end subroutine rdlock

    subroutine wrlock()
      implicit none

      call th_rwlock_wrlock(0)
    end subroutine wrlock

    subroutine unlock()
      implicit none

      call th_rwlock_unlock(0)
    end subroutine unlock

  subroutine dlb_thread_init(world)
    !  Purpose: initalization of needed stuff
    !           is in one thread context
    !           does not start the threads yet
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: dlb_common_init
    implicit none
    integer, intent(in) :: world
    ! *** end of interface ***

    call dlb_common_init(world)

    call th_inits()
  end subroutine dlb_thread_init

  subroutine thread_setup()
    !  Purpose: initalization of needed stuff
    !           is in one thread context
    !           does not start the threads yet
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer :: alloc_stat
    allocate(messagesJA(JLENGTH, n_procs), stat = alloc_stat)
    ASSERT(alloc_stat==0)

  end subroutine thread_setup


  subroutine divide_jobs(partner, requ)
    !  Purpose: share jobs from job_storage with partner, tell
    !           partner what he got
    !
    ! Context: mailbox thread.
    !          2 threads: secretary thread
    !
    ! Locks: LOCK_JS.
    !
    ! Signals: COND_JS_UPDATE.
    !
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: divide_work
    use dlb_common, only: split_at, isend
    use dlb_common, only: WORK_DONAT
    USE_MPI
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: partner
    integer, allocatable :: requ(:)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: tag
    integer(kind=i4_kind)                :: w, req
    integer(kind=i4_kind)                :: g_jobs(JLENGTH)
    integer(kind=i4_kind)                :: remaining(JLENGTH)
    !------------ Executable code --------------------------------
    call th_mutex_lock(LOCK_JS)

    ! Share the jobs (a half, or less if olnly master has some):
    w = divide_work(job_storage(1:2), n_procs)

    g_jobs = job_storage
    tag = WORK_DONAT
    if (w == 0) then ! nothing to give, set empty

      !!! variant with master, if master cannot give work back, there is no more
      ! work for the proc, thus tell him to terminate
      if (masterserver) then
         tag = NO_WORK_LEFT
         call check_termination(partner)
         ! don't send too many messages to myself, anyhow, termination master
         ! has to wait, till all procs got termination send back
         if (partner == termination_master) then
           call th_mutex_unlock(LOCK_JS)
           return
         endif
      endif
      !!! end variant with master
      g_jobs = set_empty_job()
    else ! take the last w jobs of the job-storage
      call split_at(job_storage(JLEFT) + w, job_storage, g_jobs, remaining)
      job_storage = remaining
    endif
    messagesJA(:, partner+1) = g_jobs
    call time_stamp("share jobs with other",5)

    call isend(messagesJA(:, partner+1), partner, tag, req)

    call add_request(req, requ)
    call th_mutex_unlock(LOCK_JS)
  end subroutine divide_jobs

  subroutine check_termination(proc)
    !  Purpose: only on termination_master, checks if all procs
    !           have reported termination
    !
    ! Context: mailbox thread. (termination master only)
    !          2 Threads: secretary thread
    !
    ! Locks: wrlock on "terminated" flag.
    !
    ! Signals: none.
    !
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: print_statistics
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in)    :: proc
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    !------------ Executable code --------------------------------
    if (.not. has_last_done(proc)) RETURN

    ! there will be only a send to the other procs, telling them to terminate
    ! thus the termination_master sets its termination here
    call print_statistics()
    call wrlock()
    terminated = .true.
    call unlock()

    ! masterserver handles termination seperatly, this here is only for its own termination
    ! I'm not sure what MPI does with requests of the size 0, thus quit if there is only one
    ! processor
    if (.not. (masterserver .or. n_procs == 1)) call send_termination()

  end subroutine check_termination

  logical function termination()
    ! Purpose: make lock around terminated, but have it as one function
    !
    ! Context: main, control, and mailbox threads.
    !          2 threads: main and secretary thread
    !
    ! Locks: rdlock
    !
    implicit none
    ! *** end of interface ***

    call rdlock()
    termination = terminated
    call unlock()
  end function termination

   subroutine end_threads()
     integer :: alloc_stat
     if (allocated(messagesJA)) then
       deallocate(messagesJA, stat=alloc_stat)
       ASSERT(alloc_stat==0)
     endif
   end subroutine end_threads

  subroutine clear_up(my_last, last_proc, arrived, requ)
    ! Purpose: clear_up will finish the still outstanding communications of the
    !          thread variants, after the flag termination was set. The most computational
    !          costy part of this is a MPI_ALLGATHER.
    !          Each proc tells threre from which proc he waits for an answer and gives the
    !          number of his message counter to this proc. This number tells the other proc,
    !          if he has already answered, but the answer has not yet arrived, or if he has
    !          to really wait for the message. If every proc now on how many messages he has to
    !          wait, he does so (responds if neccessary) and finishes.
    !          A MPI_barrier in the main code ensures, that he will not get messges belonging
    !          to the next mpi cycle.
    ! Context for 3Threads: mailbox thread.
    !             2 Threads: secretary thread
    USE_MPI, only: MPI_IN_PLACE, MPI_DATATYPE_NULL, MPI_INTEGER4, &
         MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_TAG, MPI_SOURCE, MPI_STATUS_SIZE, &
         MPI_SUCCESS
    use dlb_common, only: comm_world, my_rank
    use dlb_common, only: WORK_DONAT, WORK_REQUEST
    use dlb_common, only: end_requests, isend, recv
    implicit none
    integer(kind=i4_kind), intent(in) :: my_last(:), arrived(:), last_proc
    integer, allocatable  :: requ(:)
    !** End of interface *****************************************

    integer(i4_kind) :: ierr, i, count_req, req
    integer(i4_kind) :: rec_buff(n_procs * 2)
    integer(i4_kind) :: stat(MPI_STATUS_SIZE)
    integer(i4_kind) :: message_s(JLENGTH), message_r(JLENGTH)
    !------------ Executable code --------------------------------

    rec_buff = 0
    rec_buff(2 * my_rank + 1) = last_proc ! Last I sended to or -1
    if (last_proc > -1) then
        rec_buff(2 * my_rank + 2) = my_last(last_proc + 1) ! the request number I sended to him
    endif
    call MPI_ALLGATHER(MPI_IN_PLACE, 0, MPI_DATATYPE_NULL, rec_buff, 2, MPI_INTEGER4, comm_world, ierr)
    ASSERT(ierr==MPI_SUCCESS)
    count_req = 0

    ! If I am waiting for a job donation also
    if (last_proc > -1) count_req = 1

    do i = 1, n_procs
      if (rec_buff(2*i-1) == my_rank) then
         ! This one sended it last message to me
         ! now check if it has already arrived (answerded)
         if (rec_buff(2*i) .NE. arrived(i)) count_req = count_req + 1
      endif
    enddo

    ! Cycle over all left messages, blocking MPI_RECV, as there is nothing else to do
    do i = 1, count_req
        call recv(message_r, MPI_ANY_SOURCE, MPI_ANY_TAG, stat)
        select case(stat(MPI_TAG))
        case (WORK_DONAT)
          cycle
        case (WORK_REQUEST)
           ! FIXME: tag is the only info we send:
           message_s(:) = 0
           call isend(message_s, stat(MPI_SOURCE), WORK_DONAT, req)
           call add_request(req, requ)
        case default
          print *, my_rank, "got Message", message_r, ", which I was not waiting for"
          stop "got unexpected message"
        end select
    enddo
    ! Now all request are finished and could be ended
    call end_requests(requ)
  end subroutine clear_up

end module dlb_impl_thread_common
