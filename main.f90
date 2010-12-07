program main
! Purpose: starts tests

use dlb, only: dlb_init, dlb_finalize, dlb_setup, dlb_give_more
use test, only: echo
# include "dlb.h"
USE_MPI

use dlb_common, only: time_stamp ! for debug prints
implicit none
!include 'mpif.h'

integer            :: NJOBS = 20
integer, parameter :: MAXJOBS = 1
integer, parameter :: NTIMES = 3

integer :: rank, n_procs
integer :: ierr, prov
integer :: i, k

double precision :: time
double precision, allocatable :: times(:, :)

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

!
! Parse command line (after having given the chance for MPI
! to modify it)
!
call get_args(NJOBS)

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
allocate(jobs_done(0:n_procs-1), sub_total(0:n_procs-1))
times = 0.0

call time_stamp("START",0)

! starting time:
times(rank, 1) = MPI_WTIME()

!
! Needs to be done once:
!
call dlb_init()


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


!
! Do once:
!
call dlb_finalize()

!
! The rest is debug and diagnostics.
!


call time_stamp("END",0)
! elapsed time:
times(rank, 1) = MPI_WTIME() - times(rank, 1)

call reduce_double_2D(times)
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
    print *, "jobs done on the processors: ", jobs_done(:)
    print *, "max/min jobs on procs: ", maxval(jobs_done(:)), minval(jobs_done(:))
endif

call MPI_FINALIZE(ierr)

contains

  subroutine get_args(njobs)
    implicit none
    integer, intent(out) :: njobs
    ! *** end of interface ***

    character(len=128) :: arg
    integer :: stat

    ! requires F2003-bits for argv support:
    call get_command_argument(1, arg, status=stat)

    if ( stat /= 0 ) then
        arg = "20"
    endif

    read(arg, *, iostat=stat) njobs

    if ( stat /= 0 ) then
      stop "Usage: ./test_dlb [NJOBS]"
    endif
  end subroutine get_args

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
