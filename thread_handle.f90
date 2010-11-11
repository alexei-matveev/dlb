module thread_handle
# include "dlb.h"
  use iso_c_binding
  use dlb_common, only: dlb_common_init, dlb_common_finalize
  use dlb_common, only: i4_kind, r8_kind, comm_world, n_procs, termination_master
  use dlb_common, only: time_stamp, time_stamp_prefix ! for debug only
  use dlb_common, only: SJOB_LEN, J_EP, J_STP, MSGTAG, NO_WORK_LEFT
  use dlb_common, only: has_last_done, set_empty_job, add_request, send_termination
  use dlb_common, only: masterserver
  USE_MPI
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

  !------------ Declaration of types ------------------------------

  !------------ Declaration of constants and variables ----
  integer(kind=i4_kind), parameter, public  :: WORK_REQUEST = 4, WORK_DONAT = 5 ! messages for work request
  integer(kind=i4_kind), parameter, public  :: JOBS_LEN = SJOB_LEN  ! Length of complete jobs storage
  ! IDs of mutexes, use base-0 indices:
  integer(kind=i4_kind), parameter, public :: LOCK_JS   = 0
  integer(kind=i4_kind)             :: job_storage(jobs_len) ! store all the jobs, belonging to this processor
  logical                           :: terminated ! for termination algorithm


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

  subroutine dlb_thread_init()
    !  Purpose: initalization of needed stuff
    !           is in one thread context
    !           does not start the threads yet
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: dlb_common_init
    implicit none
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    call dlb_common_init()
    call th_inits()
  end subroutine dlb_thread_init

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
    use dlb_common, only: divide_work, divide_work_master
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: partner
    integer, allocatable :: requ(:)
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, w, req
    integer(kind=i4_kind)                :: g_jobs(SJOB_LEN), message(1 + SJOB_LEN)
    !------------ Executable code --------------------------------
    call th_mutex_lock(LOCK_JS)

    if (masterserver) then
      !!! variant with masterserver: here only a part of the jobs available may be
      !!! given away
      w = divide_work_master(job_storage, n_procs)
    else
      !!! chare the jobs equally distributed
      w = divide_work(job_storage)
    endif

    g_jobs = job_storage
    message(1) = WORK_DONAT
    if (w == 0) then ! nothing to give, set empty

      !!! variant with master, if master cannot give work back, there is no more
      ! work for the proc, thus tell him to terminate
      if (masterserver) then
         message(1) = NO_WORK_LEFT
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
      g_jobs(J_STP)  = g_jobs(J_EP) - w
      job_storage(J_EP) = g_jobs(J_STP)
    endif
    message(2:) = g_jobs
    call time_stamp("chare jobs with other",5)
    call MPI_ISEND(message, 1+SJOB_LEN, MPI_INTEGER4, partner, MSGTAG, comm_world, req, ierr)
    ASSERT(ierr==MPI_SUCCESS)
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
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in)    :: proc
    !** End of interface *****************************************
    !------------ Declaration of local variables -----------------
    !------------ Executable code --------------------------------
    if (.not. has_last_done(proc)) RETURN

    ! there will be only a send to the other procs, telling them to terminate
    ! thus the termination_master sets its termination here
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


end module thread_handle
