!===============================================================
! Public interface of module
!===============================================================
module dlb
!---------------------------------------------------------------
!
!  Purpose: takes care about dynamical load balancing, there are two different
!           cases possible: one where all jobs are equal in name and a second
!           one where each job has a special colour
!           It is assumed that several succeeding jobs have the same color, thus
!           the color is wanted in ranges, It is assured that all the jobs given back
!           have the same color, color should be given as an integer
!           There are several dlb routines possible, which could be used to generate
!           the dynamical balancing themselves
!           INTERFACE to others:
!           call dlb_init() - once before first acces
!           call dlb_finalize() - once after last acces
!
!           There are two choices for the actual DLB run, they can be used alternatly
!           but routines of them may not be mixed up.
!           First WITHOUT COLORS (e.g. all jobs are equal):
!           call dlb_setup(N) - once every time a dlb should
!                               be used, N should be the number of jobs
!           dlb_give_more(n, jobs) :
!              n should be the number of jobs requested at once, the next
!              time dlb_give_more is called again, all jobs from jobs should
!              be finished, jobs are at most n, they are just the number of the
!              jobs, which still have to be transformed between each other,
!              it should be done the error slice from jobs(0) +1 to jobs(1)
!              if dlb_give_more returns jobs(0)==jobs(1) there is no chance that
!              this proc will get any job during this dlb-round
!           Second WITH COLORS (e.ge. each job has an integer number attached to it, which
!              gives the "color" of the job. It is supposed that several succeeding jobs have
!              the same color, thus the color is given for job slices)
!           call dlb_setup_color(distr) - once every time a dlb should be used,
!                                distr is an array containing elements
!                                like: (/color, startnumber, endnumber/),
!                                it is possible to have several not succeeding
!                                 jobsi with the same color, then for each one the
!                                number has to be given in its own array element
!                                startnumber and endnumber of each color are independent of
!                                each other, they may have overlapping intervals, in this
!                                case one has to ensure, that the right jobs are done, as
!                                the DLB routine gives back the numbers of the array
!           dlb_give_more_color(n, color, jobs):
!              the same as dlb_give_more, but for the color case
!              gives back the numbers between startnumber and endnumber of the
!              element color, it only gives jobs of the same color, even if it
!              still has some left but from another color, but it also will only give
!              back empty jobs if all is finished
!  Module called by: ...
!
!
!  References:
!
!  Author: AN
!  Date: 10/10
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

!
! There are different implementations of DLB available, the interface
! looks all the same. The linking will define which one is used.
! See "use dlb_impl" instances in text, these are the references to
! the actual implementation.
!

# include "dlb.h"
! Need here some stuff, that is already defined elsewere
use dlb_common, only: J_STP, J_EP, L_JOB
use dlb_common, only: masterserver, termination_master
use dlb_common, only: i4_kind
implicit none
save            ! save all variables defined in this module
private         ! by default, all names are private
!== Interrupt end of public interface of module =================
 !------------ Declaration of types ------------------------------

public dlb_init, dlb_finalize, dlb_setup, dlb_setup_color, dlb_give_more
public dlb_give_more_color

! This global variables are only needed for the color-case
! Ensure that if J_STP and J_EP should be changed some time that J_COLOR is
! still valid
integer(i4_kind), parameter   :: J_COLOR = 3
! storage for the distribution of jobs over the colors, should hold exactly
! the distr one has given in dlb_setup_color, start_color is a helpe varialbe
! for linking the intern job-numbers (succeeding ones) on the job-ids of the
! job distribution
integer(i4_kind), allocatable :: job_distribution(:,:), start_color(:)
! in the color case one may not be able to hand over all jobs at once, thus store them here
integer(i4_kind)              :: current_jobs(L_JOB)

contains

  subroutine dlb_init()
    !  Purpose: initalization of needed stuff
    !------------ Modules used ------------------- ---------------
    use dlb_impl, only: dlb_impl_init => dlb_init
    implicit none
    !** End of interface *****************************************

    call dlb_impl_init()
    current_jobs = 0
  end subroutine dlb_init

  subroutine dlb_finalize()
    !  Purpose: cleaning up everything, after last call
    !------------ Modules used ------------------- ---------------
    use dlb_impl, only: dlb_impl_finalize => dlb_finalize
    implicit none
    ! *** end of interface ***

    integer(i4_kind)              :: ierr

    ! Theu should not be there anymore but one may wnat to ensure this
    if (allocated(job_distribution)) then
      deallocate(job_distribution, stat = ierr)
      ASSERT(ierr==0)
    endif
    if (allocated(start_color)) then
      deallocate(start_color, stat = ierr)
      ASSERT(ierr==0)
    endif
    call dlb_impl_finalize()
  end subroutine dlb_finalize

  subroutine distribute_jobs(N, n_procs, my_rank)
    !  Purpose: given the number of jobs alltogether, decides how many
    !           will be done on each proc and where is start and endpoint
    !           in the global job range, this will be fed directly in
    !           the dlb_setup of the dlb routine
    !           each one should get an equal amount of them, if it
    !           is not equally dividable the first ones get one more
    !------------ Modules used ------------------- ---------------
    use dlb_impl, only: dlb_impl_setup => dlb_setup
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: N, n_procs, my_rank
    !** End of interface *****************************************

    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: jobs_per_proc, rest, my_jobs(L_JOB)

    jobs_per_proc = N / n_procs
    my_jobs(J_STP) = jobs_per_proc * my_rank
    ! if it is not dividable, distribute the rest
    ! this will shift the start point of each (but the first) proc
    rest = N - jobs_per_proc * n_procs
    if (my_rank < rest) then
       my_jobs(J_STP) = my_jobs(J_STP) + my_rank
    else
       my_jobs(J_STP) = my_jobs(J_STP) + rest
    endif

    ! if this proc has to do one more job tell him so
    if (my_rank < rest) jobs_per_proc = jobs_per_proc + 1
    my_jobs(J_EP) = my_jobs(J_STP) + jobs_per_proc

    !print *, my_rank, "has jobs", my_jobs, "of", N
    ! This is the dlb routine setup, which stores the job for the run
    call dlb_impl_setup(my_jobs)
  end subroutine distribute_jobs

  subroutine distribute_jobs_master(N, n_procs, my_rank)
    !  Purpose: given the number of jobs alltogether, decides how many
    !           will be done on each proc and where is start and endpoint
    !           in the global job range, this will be fed directly in
    !           the dlb_setup of the dlb routine
    !           masterserver variant, 70% are distributed beforehand
    !           the rest is on the master
    !------------ Modules used ------------------- ---------------
    use dlb_impl, only: dlb_impl_setup => dlb_setup
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: N, n_procs, my_rank
    !** End of interface *****************************************

    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: jobs_per_proc, rest, my_jobs(L_JOB)

    jobs_per_proc = N / n_procs * 7 / 10
    my_jobs(J_STP) = jobs_per_proc * my_rank
    ! if it is not dividable, distribute the rest
    ! this will shift the start point of each (but the first) proc
    my_jobs(J_EP) = my_jobs(J_STP) + jobs_per_proc
    if (my_rank == termination_master) then
      my_jobs(J_EP) = N
    endif
    !print *, my_rank, "has jobs", my_jobs, "of", N
    ! This is the dlb routine setup, which stores the job for the run
    call dlb_impl_setup(my_jobs)
  end subroutine distribute_jobs_master

  subroutine dlb_setup(N)
    !  Purpose: initialization of a dlb run, each proc should call
    !           it with the number of jobs alltogether. This is the
    !           version without color distingishing, thus this information
    !           is enough
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: my_rank, n_procs
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: N
    ! *** end of interface ***

    if (masterserver) then
      call distribute_jobs_master(N, n_procs, my_rank)
    else
      call distribute_jobs(N, n_procs, my_rank)
    endif
  end subroutine dlb_setup

  subroutine dlb_setup_color(distr)
    !  Purpose: initialization of a dlb run, each proc should call
    !           it with the number of jobs alltogether. This is the
    !           version with color distingishing, thus the distribution
    !           of the jobs over the color have to be given
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: my_rank, n_procs
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: distr(:,:)
    !** End of interface *****************************************

    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, many_colors,i

    if (allocated(job_distribution)) then
      deallocate(job_distribution, stat = ierr)
      ASSERT(ierr==0)
    endif

    if (allocated(start_color)) then
      deallocate(start_color, stat = ierr)
      ASSERT(ierr==0)
    endif

    ! There is also an internal color_link which gives every distribution line just the
    ! line number as color, the "real" color is only used to give it back
    ! there are many_colors internal colors
    many_colors = size(distr,1)
    ASSERT(size(distr, 2) == 3)

    allocate(job_distribution(many_colors, 3), start_color(many_colors+1), stat = ierr)
    ASSERT(ierr==0)

    job_distribution = distr ! to keep the informations
    ! start_color just keeps for every internal color the information with wich of the internal
    ! job-numbers this color will start. The last entry point gives the number of all jobs altogether
    start_color(1) = 0
    do i = 2, many_colors +1
      start_color(i) = start_color(i-1) + job_distribution(i-1,J_EP) - job_distribution(i-1,J_STP)
    enddo

    !
    ! Internally jobs are treated as equal by DLB
    !
    if (masterserver) then
      call distribute_jobs_master( start_color(many_colors + 1), n_procs, my_rank)
    else
      call distribute_jobs( start_color(many_colors + 1), n_procs, my_rank)
    endif
  end subroutine dlb_setup_color

  logical function dlb_give_more(n, my_job)
    !  Purpose: Returns next bunch of up to n jobs, if jobs(J_EP)<=
    !  jobs(J_STP) there are no more jobs there, else returns the jobs
    !  done by the procs should be jobs(J_STP) + 1 to jobs(J_EP) in
    !  the related job list
    !------------ Modules used ------------------- ---------------
    use dlb_impl, only: dlb_impl_give_more => dlb_give_more
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: n
    integer(kind=i4_kind), intent(out  ) :: my_job(L_JOB)
    !** End of interface *****************************************

    call dlb_impl_give_more(n, my_job)
    dlb_give_more = (my_job(J_STP) < my_job(J_EP))
  end function dlb_give_more

  logical function dlb_give_more_color(n, color, my_job)
    !  Purpose: Returns next bunch of up to n jobs, if jobs(J_EP)<=
    !  jobs(J_STP) there are no more jobs there, else returns the jobs
    !  done by the procs should be jobs(J_STP) + 1 to jobs(J_EP) in
    !  the related job list
    !  this version gives also back the color of the jobs and ensures
    !  that the jobs given back all have the same color
    !  it keeps other colored jobs in its own storage
    !------------ Modules used ------------------- ---------------
    use dlb_impl, only: dlb_impl_give_more => dlb_give_more
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: n
    integer(kind=i4_kind), intent(out  ) :: my_job(L_JOB), color
    !** End of interface *****************************************

    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: i,  w, jobs_all, jobs_color, current_color, ierr

    dlb_give_more_color = .true.
    if (current_jobs(J_STP) >= current_jobs(J_EP)) then ! only if the own storage is empty, refill
        call dlb_impl_give_more(n, current_jobs)
    endif

    ! ATTENTION: this if statement contains a return
    if (current_jobs(J_STP) >= current_jobs(J_EP)) then ! got empty job from dlb, thus all done, quit
      dlb_give_more_color = .false.
      my_job = current_jobs
      color = 0
      if (allocated(job_distribution)) then
        deallocate(job_distribution, stat = ierr)
        ASSERT(ierr==0)
      endif

      if (allocated(start_color)) then
        deallocate(start_color, stat = ierr)
        ASSERT(ierr==0)
      endif

      RETURN
    endif

    ! find out which color the first current job has, (in internal color)
    do i = 2, size(start_color)
      if (start_color(i) > current_jobs(J_STP)) then
        current_color = i - 1
        exit
      endif
    enddo
    color = job_distribution(current_color, J_COLOR) ! transform it in the color, the calling program
    ! will want
    jobs_all = current_jobs(J_EP) - current_jobs(J_STP) ! how many jobs do I have
    jobs_color = start_color(current_color + 1) - current_jobs(J_STP) ! how many jobs are there left
    ! of the color
    w = min(jobs_all, jobs_color)
    ! now share the own storage with the calling program
    my_job(J_STP) = job_distribution(current_color,J_STP) + current_jobs(J_STP) - start_color(current_color)
    my_job(J_EP) = my_job(J_STP) + w
    current_jobs(J_STP) = current_jobs(J_STP) + w
  end function dlb_give_more_color

end module dlb
