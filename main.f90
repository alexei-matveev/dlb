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

integer, parameter :: num_jobs = 20
integer, parameter :: n = 3
integer, target    :: my_parts(n)

double precision :: time
double precision, allocatable :: times(:, :)

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
! times(:, 1) --- elapsed
! times(:, 2) --- working (usful work)
!
allocate(times(0:n_procs-1, 2))
times = 0.0

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

! starting time:
times(rank, 1) = MPI_WTIME()

call dlb_init()

call dlb_setup(my_jobs)
do while (more_work(n, sec))
   ! measure working time:
   time = MPI_WTIME()

   call time_stamp("START USEFUL WORK")

   do i = 1, size(sec)
     res = test_calc(sec(i))
   enddo

   call time_stamp("STOP USEFUL WORK")

   ! increment working time by this slice:
   times(rank, 2) = times(rank, 2) + (MPI_WTIME() - time)
enddo

call dlb_finalize()

call time_stamp("END")

! elapsed time:
times(rank, 1) = MPI_WTIME() - times(rank, 1)

call reduce(times)

if ( rank == 0 ) then
  print *,'elapsed times = ', times(:, 1)
  print *,'working times = ', times(:, 2)
  print *,'efficiency =', sum(times(:, 2)) / maxval(times(:, 1)) / size(times, 1)
endif

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

  subroutine reduce(array)
    implicit none
    double precision, intent(inout) :: array(:, :)
    ! *** end of interface ***

    integer :: rank, ierr

    call MPI_COMM_RANK( MPI_COMM_WORLD, rank, ierr )
    if ( ierr /= MPI_SUCCESS ) stop "error in MPI_COMM_RANK"

    if ( rank == 0 ) then
      call MPI_REDUCE( MPI_IN_PLACE, array, size(array), MPI_DOUBLE_PRECISION, MPI_SUM, 0, MPI_COMM_WORLD, ierr)
    else
      call MPI_REDUCE( array, MPI_IN_PLACE, size(array), MPI_DOUBLE_PRECISION, MPI_SUM, 0, MPI_COMM_WORLD, ierr)
    endif
    if ( ierr /= MPI_SUCCESS ) stop "error in MPI_REDUCE"
  end subroutine reduce

end program main
