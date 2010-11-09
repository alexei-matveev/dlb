!===============================================================
! Public interface of module
!===============================================================
module dlb

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
use dlb_common, only: dlb_common_init, dlb_common_finalize
implicit none
! ONLY FOR DEBBUGING WITHOUT PARAGAUSS
  include 'mpif.h'
! END ONLY FOR DEBUGGING
save            ! save all variables defined in this module
private         ! by default, all names are private
!== Interrupt end of public interface of module =================
 !------------ Declaration of types ------------------------------
! ONLY FOR DEBBUGING WITHOUT PARAGAUSS
! integer with 4 bytes, range 9 decimal digits
integer, parameter :: integer_kind = selected_int_kind(9)
integer, parameter :: i4_kind = integer_kind
! real with 8 bytes, precision 15 decimal digits
integer, parameter :: double_precision_kind = selected_real_kind(15)
integer, parameter :: r8_kind = double_precision_kind
integer(kind=i4_kind) :: my_rank
double precision :: time_offset = -1.0
! END ONLY FOR DEBUGGING

 integer(kind=i4_kind), parameter  :: L_JOB = 2  ! Length of job to give back from interface
 integer(kind=i4_kind), parameter  :: SJOB_LEN = L_JOB ! Length of a single job in interface
 integer(kind=i4_kind), parameter  :: J_STP = 1 ! Number in job, where stp (start point) is stored
 integer(kind=i4_kind), parameter  :: J_EP = 2 ! Number in job, where ep (end point) is stored
 integer(kind=i4_kind), parameter  :: JOBS_LEN = SJOB_LEN  ! Length of complete jobs storage
 integer(kind=i4_kind)             :: job_storage(jobs_len) ! store all the jobs, belonging to this processor

public dlb_init, dlb_finalize, dlb_setup, dlb_give_more !for using the module

contains
  subroutine dlb_init()
    !  Purpose: initalization of needed stuff
    !------------ Modules used ------------------- ---------------
    implicit none
    !** End of interface *****************************************
    call dlb_common_init()
  end subroutine dlb_init
  !*************************************************************
  subroutine dlb_finalize()
    !  Purpose: cleaning up everything, after last call
    !------------ Modules used ------------------- ---------------
    implicit none
    call dlb_common_finalize()
  end subroutine dlb_finalize
  !*************************************************************
  subroutine dlb_give_more(n, my_job)
    !  Purpose: Returns next bunch of up to n jobs, if jobs(J_EP)<=
    !  jobs(J_STP) there are no more jobs there, else returns the jobs
    !  done by the procs should be jobs(J_STP) + 1 to jobs(J_EP) in
    !  the related job list
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: n
    integer(kind=i4_kind), intent(out  ) :: my_job(L_JOB)
!   !** End of interface *****************************************
!   !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, w
    integer(i4_kind), target             :: jobs(SJOB_LEN)
!   !------------ Executable code --------------------------------
    ! First try to get a job from local storage
    w = min(job_storage(J_EP) - job_storage(J_STP), n)
    w = max(w, 0)
    jobs = job_storage(:SJOB_LEN) ! first SJOB_LEN hold the job
    jobs(J_EP)  = jobs(J_STP) + w
    job_storage(J_STP) = jobs(J_EP)
    my_job = jobs(:L_JOB)
  end subroutine dlb_give_more
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
    job_storage(:L_JOB) = job
  end subroutine dlb_setup
  !*************************************************************
  !--------------- End of module ----------------------------------
end module dlb

