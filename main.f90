!==========================================================
! Interface for testing
!=========================================================
program main
! Purpose: starts tests
! End of public interface

use dlb_module
use test2, only: test_calc
implicit none
include 'mpif.h'
integer :: rank, n_procs
integer :: ierr, prov
integer :: i, res
integer :: my_jobs(2)
integer, pointer :: sec(:)

integer, parameter :: num_jobs = 10
integer, parameter :: n = 3
integer, target    :: my_parts(n)

call MPI_INIT_THREAD(MPI_THREAD_MULTIPLE, prov, ierr)

select case(prov)
case (MPI_THREAD_SINGLE)
  print *, 'MPI_THREAD_SINGLE'
case (MPI_THREAD_FUNNELED)
  print *, 'MPI_THREAD_FUNNELED'
case (MPI_THREAD_SERIALIZED)
  print *, 'MPI_THREAD_SERIALIZED'
case (MPI_THREAD_MULTIPLE)
  print *, 'MPI_THREAD_MULTIPLE'
case default
  print *, 'provided capabilities=',prov
end select

call MPI_COMM_RANK( MPI_COMM_WORLD, rank, ierr )
call MPI_COMM_SIZE( MPI_COMM_WORLD, n_procs, ierr)
print *, "Hello World!, I'm", rank, "of", n_procs

!
! Place all jobs on highest rank for testing:
!
if (rank+1 == n_procs) then
  my_jobs(1) = 0
  my_jobs(2) = num_jobs
else
  my_jobs(1) = 0
  my_jobs(2) = 0
endif
print *, rank, ":My jobs:", my_jobs

call time_stamp("START")

call dlb_init()

call dlb_setup(my_jobs) !,.false.)
do while (more_work(n, sec))
   call time_stamp("START USEFUL WORK")
   do i = 1, size(sec)
     res = test_calc(sec(i))
   enddo
   call time_stamp("STOP USEFUL WORK")
enddo

call dlb_finalize()

call time_stamp("END")

call MPI_FINALIZE(ierr)

contains

  logical function more_work(n, slice)
    integer, intent(in) :: n
    integer, intent(out), pointer :: slice(:)
    ! *** end of interface ***

    integer :: job(2)

    call dlb_give_more(n, job)
    my_jobs = job
    more_work = more_points(n, slice)
  end function more_work

  logical function more_points(n, slice)
    integer, intent(in) :: n
    integer, intent(out), pointer :: slice(:)
    ! *** end of interface ***

    integer, save :: counter = 0
    integer :: i

    if (counter < my_jobs(1)) counter = my_jobs(1)

    more_points = .true.
    if (counter >= my_jobs(2)) more_points = .false.

    if (more_points) then
      if (my_jobs(2) > n + counter ) then
         do i = 1, n
            my_parts(i) = counter + i
         enddo
         counter = counter + n
         slice => my_parts(:)
      else
        do i = 1, my_jobs(2) - counter
          my_parts(i) = counter + i
        enddo
        slice => my_parts(:my_jobs(2) - counter)
        my_jobs(1) = my_jobs(2)
        counter = 0 !my_jobs(2)
      endif
    else
      slice => NULL()
      counter = 0
    endif
  end function more_points

end program main
