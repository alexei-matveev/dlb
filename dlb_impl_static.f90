!===============================================================
! Public interface of module
!===============================================================
module dlb_impl

!---------------------------------------------------------------
!
!  Purpose: takes care about dynamical load balancing, is the static
!           routine, meaning, not dynamical, but for comparision with them
!           with exactly the same interface
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
!  Module called by: ...
!
!
!  References:
!
!  Author: AN
!  Date: 09/10
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
USE_MPI, only: MPI_THREAD_SINGLE
use dlb_common, only: i4_kind, JLENGTH
implicit none
save            ! save all variables defined in this module
private         ! by default, all names are private

public :: dlb_init, dlb_finalize, dlb_setup, dlb_give_more !for using the module

! Program from outside might want to know the thread-safety-level required form DLB
integer(i4_kind), parameter, public :: DLB_THREAD_REQUIRED = MPI_THREAD_SINGLE

!== Interrupt end of public interface of module =================

integer(i4_kind) :: job_storage(JLENGTH) ! store all the jobs, belonging to this processor

!for debugging:
double precision  :: dlb_time
contains

  subroutine dlb_init()
    !  Purpose: initalization of needed stuff
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: set_empty_job, dlb_common_init
    use dlb_common, only: OUTPUT_BORDER, my_rank
    implicit none
    !** End of interface *****************************************

    job_storage = set_empty_job()

    call dlb_common_init()

    if (my_rank == 0 .and. 0 < OUTPUT_BORDER) then
        print *, "DLB init: using variant 'static'"
        print *, "DLB init: This variant is a 'non-dynamical DLB' routine"
    endif
  end subroutine dlb_init

  subroutine dlb_finalize()
    !  Purpose: cleaning up everything, after last call
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: dlb_common_finalize
    implicit none

    call dlb_common_finalize()
  end subroutine dlb_finalize

  subroutine dlb_give_more(n, my_job)
    !  Purpose: Returns next bunch of up to n jobs, if my_job(1)<=
    !  my_job(2) there are no more jobs there, else returns the jobs
    !  done by the procs should be my_job(1) + 1 to my_job(2) in
    !  the related job list
    !------------ Modules used ------------------- ---------------
    USE_MPI, only: MPI_Wtime
    use dlb_common, only: steal_local, JLEFT, JRIGHT
    use dlb_common, only: OUTPUT_BORDER, empty, my_rank
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(i4_kind), intent(in)  :: n
    integer(i4_kind), intent(out) :: my_job(:)
    !** End of interface *****************************************

    integer(i4_kind) :: jobs(JLENGTH), remaining(JLENGTH)

    !
    ! Return of an empty job interval will be interpreted as
    ! "no jobs left", thus refuse requests for zero jobs:
    !
    ASSERT(n>0)
    ASSERT(size(my_job)==2)

    if ( steal_local(n, job_storage, remaining, jobs) ) then
        job_storage = remaining
    endif

    !
    ! Named constants are not known outside:
    !
    my_job(1) = jobs(JLEFT)
    my_job(2) = jobs(JRIGHT)

    if (1 < OUTPUT_BORDER .and. empty(jobs)) then
         ! output for debugging, only reduced inforamtions needed compared to
         ! dynamical cases
         write(*, '(I3, " M: time spend in dlb", G20.10)') my_rank, MPI_Wtime() - dlb_time
    endif
  end subroutine dlb_give_more

  subroutine dlb_setup(job)
    !  Purpose: initialization of a dlb run, each proc should call
    !           it with inital jobs. The inital jobs should be a
    !           static distribution of all available jobs, each job
    !           should be given to one and only one of the procs
    !           jobs should be given as a job range (STP, EP), where
    !           all jobs should be the numbers from START to END, with
    !           START <= STP <= EP <= END
    !------------ Modules used ------------------- ---------------
    USE_MPI, only: MPI_Wtime
    use dlb_common, only: JLEFT, JRIGHT, JOWNER, my_rank
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(i4_kind), intent(in) :: job(:)
    !** End of interface *****************************************

    ASSERT(size(job)==2)

    job_storage(JLEFT) = job(1)
    job_storage(JRIGHT) = job(2)
    job_storage(JOWNER) = my_rank ! FIXME: is this field ever used?

    dlb_time = MPI_Wtime() ! for debugging
  end subroutine dlb_setup

  !--------------- End of module ----------------------------------
end module dlb_impl
