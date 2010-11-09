program main
! Purpose: starts tests

use dlb, only: dlb_init, dlb_finalize, dlb_setup, dlb_give_more
use test, only: echo

use dlb_common, only: time_stamp ! for debug prints
implicit none
include 'mpif.h'

integer, parameter :: NJOBS = 20
integer, parameter :: MAXJOBS = 1
integer, parameter :: NTIMES = 3

integer :: rank, n_procs
integer :: ierr, prov
integer :: i, k

double precision :: time
double precision, allocatable :: times(:, :)
double precision, allocatable :: time_stamps(:,:), loops(:)

integer, allocatable :: jobs_done(:)
integer, allocatable :: sub_total(:)

integer :: interval(2)

!call sleep(30)

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

if ( rank == 0 ) then
    print *, "Run with", NJOBS, "jobs in total"
endif

!
! times(:, 1) --- elapsed
! times(:, 2) --- working (usful work)
!
allocate(times(0:n_procs-1, 2))
allocate(time_stamps(0:n_procs-1, 5))
allocate(loops(0:n_procs-1))
allocate(jobs_done(0:n_procs-1), sub_total(0:n_procs-1))
times = 0.0
time_stamps = 0.0

call time_stamp("START",0)

! starting time:
times(rank, 1) = MPI_WTIME()
time_stamps(rank,1) = MPI_WTIME()

!
! Needs to be done once:
!
call dlb_init()

time_stamps(rank,2) = MPI_WTIME()

!
! Repeat the loop several times:
!
do k = 1, NTIMES
    jobs_done(:) = 0
    sub_total(:) = 0

    !
    ! Prepare a loop over jobs:
    !
    call dlb_setup(NJOBS)

    ! FIXME: this time stamp is bogus:
    time_stamps(rank, 3) = MPI_WTIME()

    do while ( dlb_give_more(MAXJOBS, interval) )
        ! measure working time:
        time = MPI_WTIME()

        do i = interval(1) + 1, interval(2)
            !
            ! Test calculation "echo(i)" is supposed to burn some CPU time
            ! and return job ID "i" unchanged:
            !
            sub_total(rank) = sub_total(rank) + echo(i)
            jobs_done(rank) = jobs_done(rank) + 1
        enddo

        ! increment working time by this slice:
        times(rank, 2) = times(rank, 2) + (MPI_WTIME() - time)
    enddo
enddo

time_stamps(rank,4) = MPI_WTIME()

!
! Do once:
!
call dlb_finalize()

!
! The rest is debug and diagnostics.
!

time_stamps(rank,5) = MPI_WTIME()

call time_stamp("END",0)
! elapsed time:
times(rank, 1) = MPI_WTIME() - times(rank, 1)

call reduce_double_2D(times)
call reduce_double_2D(time_stamps)
call reduce_int_1D(sub_total)
call reduce_int_1D(jobs_done)

if ( rank == 0 ) then

    if( sum(jobs_done) /= NJOBS )then
        print *, "Error: NJOBS=", NJOBS, "but sum(jobs_done)=", sum(jobs_done)
        stop "jobs done are too few or too many"
    endif

    if( sum(sub_total) /= NJOBS * (NJOBS + 1) / 2 )then
        stop "wrong result"
    endif

    print *, 'elapsed times = ', times(:, 1)
    print *, 'working times = ', times(:, 2)
    print *, 'efficiency =', sum(times(:, 2)) / maxval(times(:, 1)) / size(times, 1)
    print *, 'maximum elapsed times = ', maxval(times(:,1))
    print *, 'maximum difference in working times =', maxval(times(:,2)) - minval(times(:,2))
    print *, 'Timings: Time points:'
    print *, time_stamps(:,1)
    print *, time_stamps(:,2)
    print *, time_stamps(:,3)
    print *, time_stamps(:,4)
    print *, time_stamps(:,5)
    print *, 'Timings: durations:'
    print *, "dlb_init", time_stamps(:,2) - time_stamps(:,1)
    print *, "dlb_setup", time_stamps(:,3) - time_stamps(:,2)
    loops = time_stamps(:,4) - time_stamps(:,3)
    print *, "loops", loops(:)
    print *, 'maximum difference in loops =', maxval(loops(:)) - minval(loops(:))
    print *, "dlb_finalize", time_stamps(:,5) - time_stamps(:,4)
    print *, "jobs done on the processors: ", jobs_done(:)
    print *, "max/min jobs on procs: ", maxval(jobs_done(:)), minval(jobs_done(:))
endif

call MPI_FINALIZE(ierr)

contains

  subroutine reduce_int_1D(array)
    implicit none
    integer, intent(inout) :: array(:)
    ! *** end of interface ***

    call reduce_int_buf(array, size(array))
  end subroutine reduce_int_1D

  subroutine reduce_double_2D(array)
    implicit none
    double precision, intent(inout) :: array(:, :)
    ! *** end of interface ***

    call reduce_double_buf(array, size(array))
  end subroutine reduce_double_2D

  subroutine reduce_double_buf(buf, length)
    implicit none
    double precision, intent(inout) :: buf(length)
    integer, intent(in)    :: length
    ! *** end of interface ***

    integer :: rank, ierr

    call MPI_COMM_RANK( MPI_COMM_WORLD, rank, ierr )
    if ( ierr /= MPI_SUCCESS ) stop "error in MPI_COMM_RANK"

    if ( rank == 0 ) then
      call MPI_REDUCE( MPI_IN_PLACE, buf, length, MPI_DOUBLE_PRECISION, MPI_SUM, 0, MPI_COMM_WORLD, ierr)
    else
      call MPI_REDUCE( buf, MPI_IN_PLACE, length, MPI_DOUBLE_PRECISION, MPI_SUM, 0, MPI_COMM_WORLD, ierr)
    endif
    if ( ierr /= MPI_SUCCESS ) stop "error in MPI_REDUCE"
  end subroutine reduce_double_buf

  subroutine reduce_int_buf(buf, length)
    implicit none
    integer, intent(inout) :: buf(length)
    integer, intent(in)    :: length
    ! *** end of interface ***

    integer :: rank, ierr

    call MPI_COMM_RANK( MPI_COMM_WORLD, rank, ierr )
    if ( ierr /= MPI_SUCCESS ) stop "error in MPI_COMM_RANK"

    if ( rank == 0 ) then
      call MPI_REDUCE( MPI_IN_PLACE, buf, length, MPI_INTEGER, MPI_SUM, 0, MPI_COMM_WORLD, ierr)
    else
      call MPI_REDUCE( buf, MPI_IN_PLACE, length, MPI_INTEGER, MPI_SUM, 0, MPI_COMM_WORLD, ierr)
    endif
    if ( ierr /= MPI_SUCCESS ) stop "error in MPI_REDUCE"
  end subroutine reduce_int_buf

end program main
