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
use dlb_common, only: JLEFT, JRIGHT, L_JOB
use dlb_common, only: i4_kind
use dlb_impl, only: DLB_THREAD_REQUIRED

implicit none
save            ! save all variables defined in this module
private         ! by default, all names are private
!== Interrupt end of public interface of module =================
 !------------ Declaration of types ------------------------------

public dlb_init, dlb_finalize, dlb_setup, dlb_setup_color, dlb_give_more
public dlb_give_more_color
public DLB_THREAD_REQUIRED

! storage for the distribution of jobs over the colors, should hold exactly
! the distr one has given in dlb_setup_color, start_color is a helpe varialbe
! for linking the intern job-numbers (succeeding ones) on the job-ids of the
! job distribution
integer(i4_kind), allocatable :: start_color(:)

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

    !
    ! Only if colored-version was in use:
    !
    if (allocated(start_color)) then
      deallocate(start_color, stat = ierr)
      ASSERT(ierr==0)
    endif

    call dlb_impl_finalize()
  end subroutine dlb_finalize

  subroutine dlb_setup(N)
    !  Purpose: initialization of a dlb run, each proc should call
    !           it with the number of jobs alltogether. This is the
    !           version without color distingishing, thus this information
    !           is enough
    !------------ Modules used ------------------- ---------------
    use dlb_common, only: distribute_jobs, n_procs, my_rank
    use dlb_impl, only: dlb_impl_setup => dlb_setup
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: N
    ! *** end of interface ***

    !
    ! FIXME: why my_rank as an argument?
    !        Should the input to dlb_impl_setup(...)
    !        depend on where it is executed?
    !
    call dlb_impl_setup(distribute_jobs(N, n_procs, my_rank))
  end subroutine dlb_setup

  subroutine dlb_setup_color(N)
    !  Purpose: initialization of a dlb run, each proc should call
    !           it with the number of jobs alltogether. This is the
    !           version with color distingishing, thus the distribution
    !           of the jobs over the color have to be given
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: N(:)
    !** End of interface *****************************************

    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: ierr, i

    if (allocated(start_color)) then
      deallocate(start_color, stat = ierr)
      ASSERT(ierr==0)
    endif

    !
    ! There are size(N) many colors:
    !
    allocate(start_color(size(N) + 1), stat = ierr)
    ASSERT(ierr==0)

    ! start_color just keeps for every internal color the information with wich of the internal
    ! job-numbers this color will start. The last entry point gives the number of all jobs altogether
    start_color(1) = 0
    do i = 2, size(N) + 1
      start_color(i) = start_color(i - 1) + N(i - 1)
    enddo

    !
    ! Internally jobs are treated as equal by DLB:
    !
    call dlb_setup(sum(N))
  end subroutine dlb_setup_color

  logical function dlb_give_more(n, my_job)
    !  Purpose: Returns next bunch of up to n jobs, if jobs(JRIGHT)<=
    !  jobs(JLEFT) there are no more jobs there, else returns the jobs
    !  done by the procs should be jobs(JLEFT) + 1 to jobs(JRIGHT) in
    !  the related job list
    !------------ Modules used ------------------- ---------------
    use dlb_impl, only: dlb_impl_give_more => dlb_give_more
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: n
    integer(kind=i4_kind), intent(out  ) :: my_job(L_JOB)
    !** End of interface *****************************************

    call dlb_impl_give_more(n, my_job)
    dlb_give_more = (my_job(JLEFT) < my_job(JRIGHT))
  end function dlb_give_more

  function dlb_give_more_color(n, color, my_job) result(more)
    !  Purpose: Returns next bunch of up to n jobs, if jobs(JRIGHT)<=
    !  jobs(JLEFT) there are no more jobs there, else returns the jobs
    !  done by the procs should be jobs(JLEFT) + 1 to jobs(JRIGHT) in
    !  the related job list
    !  this version gives also back the color of the jobs and ensures
    !  that the jobs given back all have the same color
    !  it keeps other colored jobs in its own storage
    !------------ Modules used ------------------- ---------------
    implicit none
    !------------ Declaration of formal parameters ---------------
    integer(kind=i4_kind), intent(in   ) :: n
    integer(kind=i4_kind), intent(out  ) :: my_job(L_JOB), color
    logical                              :: more ! result
    !** End of interface *****************************************

    !------------ Declaration of local variables -----------------
    integer(kind=i4_kind)                :: i,  w, jobs_all, jobs_color, ierr

    if (current_jobs(JLEFT) < current_jobs(JRIGHT)) then
        ! some are left over from the last time:
        more = .true.
    else
        ! only if the own storage is empty, refill:
        more = dlb_give_more(n, current_jobs)
    endif

    !
    ! ATTENTION: this if statement contains a return
    !
    if ( .not. more ) then
        ! got empty job from dlb, thus all done, quit
        my_job = current_jobs
        color = 0

        if (allocated(start_color)) then
            deallocate(start_color, stat = ierr)
            ASSERT(ierr==0)
        endif

        RETURN
    endif

    !
    ! Find out which color the first current job has:
    !
    do i = 2, size(start_color)
      if (start_color(i) > current_jobs(JLEFT)) then
        color = i - 1
        exit
      endif
    enddo

    ! will want
    jobs_all = current_jobs(JRIGHT) - current_jobs(JLEFT) ! how many jobs do I have
    jobs_color = start_color(color + 1) - current_jobs(JLEFT) ! how many jobs are there left
    ! of the color
    w = min(jobs_all, jobs_color)
    ! now share the own storage with the calling program
    my_job(JLEFT) = current_jobs(JLEFT) - start_color(color)
    my_job(JRIGHT) = my_job(JLEFT) + w
    current_jobs(JLEFT) = current_jobs(JLEFT) + w
  end function dlb_give_more_color

end module dlb
