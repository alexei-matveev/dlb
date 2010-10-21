!==========================================================
! Interface for testing
!=========================================================
program main
! Purpose: starts tests
! End of public interface

use dlb_common, only: time_stamp ! for debug prints
use dlb
use test2, only: test_calc
implicit none
include 'mpif.h'
integer :: rank, n_procs, my_amount
integer :: ierr, prov
integer :: i, res
integer :: my_jobs(2)
integer, pointer :: sec(:)

integer, parameter :: num_jobs = 20
logical, parameter :: equal =  .false.
integer, parameter :: n = 1
integer, target    :: my_parts(n)
double precision :: sum_all(2,1)

double precision :: time
double precision, allocatable :: times(:, :), times2(:,:), loops(:)
integer, allocatable :: jobs_done(:,:)

!call sleep(30)

call MPI_INIT_THREAD(MPI_THREAD_MULTIPLE, prov, ierr)
sum_all = 0
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
if ( rank == 0 ) then
  print *, "=============================================================================="
  if (equal) then
     print *, "Run with", num_jobs, "jobs, equal distributed"
  else
     print *, "Run with", num_jobs, "jobs, worst case"
  endif
endif
!print *, "Hello World!, I'm", rank, "of", n_procs

!
! times(:, 1) --- elapsed
! times(:, 2) --- working (usful work)
!
allocate(times(0:n_procs-1, 2))
allocate(times2(0:n_procs-1, 5))
allocate(loops(0:n_procs-1))
allocate(jobs_done(0:n_procs-1,2))
times = 0.0
times2 = 0.0
jobs_done = 0
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
if (equal) then
  my_amount = num_jobs / n_procs
  my_jobs(1) = (rank) * my_amount
  my_jobs(2) = my_jobs(1) + my_amount
  if (rank+1 == n_procs) my_jobs(2) = num_jobs
endif
print *, rank, ":My jobs:", my_jobs
jobs_done(rank, 1) = my_jobs(2) - my_jobs(1)
call time_stamp("START",0)

! starting time:
times(rank, 1) = MPI_WTIME()
times2(rank,1) = MPI_WTIME()

call dlb_init()

times2(rank,2) = MPI_WTIME()
call dlb_setup(my_jobs)
times2(rank,3) = MPI_WTIME()
do while (more_work(n, sec))
   ! measure working time:
   time = MPI_WTIME()

   !call time_stamp("START USEFUL WORK",0)

   do i = 1, size(sec)
     res = test_calc(sec(i))
     sum_all(1,:) = sum_all(1,:) + res
     sum_all(2,:) = sum_all(2,:) + sec(i)
     jobs_done(rank, 2) = jobs_done(rank, 2) + 1
   enddo

   !call time_stamp("STOP USEFUL WORK",0)

   ! increment working time by this slice:
   times(rank, 2) = times(rank, 2) + (MPI_WTIME() - time)
enddo

times2(rank,4) = MPI_WTIME()
call dlb_finalize()
times2(rank,5) = MPI_WTIME()

call time_stamp("END",0)
! elapsed time:
times(rank, 1) = MPI_WTIME() - times(rank, 1)

call reduce(times)
call reduce(times2)
call reduce(sum_all)
call reduceint(jobs_done)

if ( rank == 0 ) then
  print *,'elapsed times = ', times(:, 1)
  print *,'working times = ', times(:, 2)
  print *,'efficiency =', sum(times(:, 2)) / maxval(times(:, 1)) / size(times, 1)
  print *, 'maximum elapsed times = ', maxval(times(:,1))
  print *, 'maximum difference in working times =', maxval(times(:,2)) - minval(times(:,2))
  print *, 'result of calculation =', sum_all(1,:)
  print *, 'sum of job numbers = ', sum_all(2,:)
  print *, 'Timings: Time points:'
  print *, times2(:,1)
  print *, times2(:,2)
  print *, times2(:,3)
  print *, times2(:,4)
  print *, times2(:,5)
  print *, 'Timings: durations:'
  print *, "dlb_init", times2(:,2) - times2(:,1)
  print *, "dlb_setup", times2(:,3) - times2(:,2)
  loops = times2(:,4) - times2(:,3)
  print *, "loops", loops(:)
  print *, 'maximum difference in loops =', maxval(loops(:)) - minval(loops(:))
  print *, "dlb_finalize", times2(:,5) - times2(:,4)
  print *, "start jobs: ", jobs_done(:,1)
  print *, "jobs done on the processors: ", jobs_done(:,2)
  print *, "max/min jobs on procs: ", maxval(jobs_done(:,2)), minval(jobs_done(:,2))
  print *, "away or get", jobs_done(:,2) - jobs_done(:,1)
  print *, "away or get max/min", maxval(jobs_done(:,2) - jobs_done(:,1)), minval(jobs_done(:,2) - jobs_done(:,1))
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

  subroutine reduceint(array)
    implicit none
    integer, intent(inout) :: array(:, :)
    ! *** end of interface ***

    integer :: rank, ierr

    call MPI_COMM_RANK( MPI_COMM_WORLD, rank, ierr )
    if ( ierr /= MPI_SUCCESS ) stop "error in MPI_COMM_RANK"

    if ( rank == 0 ) then
      call MPI_REDUCE( MPI_IN_PLACE, array, size(array), MPI_INTEGER4, MPI_SUM, 0, MPI_COMM_WORLD, ierr)
    else
      call MPI_REDUCE( array, MPI_IN_PLACE, size(array), MPI_INTEGER4, MPI_SUM, 0, MPI_COMM_WORLD, ierr)
    endif
    if ( ierr /= MPI_SUCCESS ) stop "error in MPI_REDUCE"
  end subroutine reduceint

end program main
