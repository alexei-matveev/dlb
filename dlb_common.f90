!===============================================================
! Public interface of module
!===============================================================
module dlb_common
  !---------------------------------------------------------------
  !
  !  Purpose: code common for all DLB implementations
  !
  !  Module called by: ...
  !
  !
  !  References:
  !
  !  Author: AN
  !  Date: 08/10->09/10
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
  use mpi, only: MPI_COMM_WORLD, MPI_WTIME
  implicit none
  save            ! save all variables defined in this module
  private         ! by default, all names are private
  !== Interrupt end of public interface of module =================

  !------------ public functions and subroutines ------------------

  public :: select_victim!(rank, np) -> integer victim

  public :: reserve_workm!(m, jobs) -> integer n
  public :: reserve_workh!(m, jobs) -> integer n
  public :: steal_work_for_rma!(m, jobs) -> integer n

  public :: time_stamp_prefix
  public :: time_stamp
  public :: assert_n

  ! integer with 4 bytes, range 9 decimal digits
  integer, parameter, public :: i4_kind = selected_int_kind(9)

  ! integer with 8 bytes, range 9 decimal digits
  integer, parameter :: i8_kind = 8

  ! real with 8 bytes, precision 15 decimal digits
  integer, parameter, public :: r8_kind = selected_real_kind(15)

  integer, parameter, public :: comm_world = MPI_COMM_WORLD

  !================================================================
  ! End of public interface of module
  !================================================================

  !------------ Declaration of types ------------------------------

  !------------ Declaration of constants and variables ----

  integer(i4_kind), public :: dlb_common_my_rank = -1 ! for debug prints only
  integer(i4_kind), parameter :: output_border = 2

  double precision :: time_offset = -1.0
  !----------------------------------------------------------------
  !------------ Subroutines ---------------------------------------
contains

! ONLY FOR DEBBUGING (WITHOUT PARAGAUSS)
  function time_stamp_prefix(time) result(prefix)
    implicit none
    double precision, intent(in) :: time
    character(len=28) :: prefix
    ! *** end of interface ***

    write(prefix, '("#", I3, G20.10)') dlb_common_my_rank, time - time_offset
  end function time_stamp_prefix

  subroutine time_stamp(msg, output_level)
    implicit none
    character(len=*), intent(in) :: msg
    integer(i4_kind), intent(in) :: output_level
    ! *** end of interface ***

    double precision :: time

    time = MPI_Wtime()
    if ( time_offset < 0.0 ) then
      time_offset = 0.0
     print *, time_stamp_prefix(time), "(BASE TIME)"
      time_offset = time
    endif

   if(output_level < output_border) print *, time_stamp_prefix(time), msg
  end subroutine time_stamp

  subroutine assert_n(exp,num)
    implicit none
    logical, intent(in) :: exp
    integer, intent(in) :: num
    if (.not. exp) then
       print *, "ASSERT FAILED", num
       call abort
    endif
  end subroutine assert_n
! END ONLY FOR DEBUGGING

  function select_victim(rank, np) result(victim)
     ! Purpose: decide of who to try next to get any jobs
     implicit none
     integer(i4_kind), intent(in) :: rank, np
     integer(i4_kind)             :: victim
     ! *** end of interface ***

     victim = select_victim_random(rank, np)
     ! victim = select_victim_r(rank, np)
  end function select_victim

  function select_victim_r(rank, np) result(victim)
     ! Purpose: decide of who to try next to get any jobs
     ! Each in a row
     implicit none
     integer(i4_kind), intent(in) :: rank, np
     integer(i4_kind)             :: victim
     ! *** end of interface ***

     integer(kind=i4_kind), save :: count = 1

     victim = mod(rank + count, np)
     count = count + 1
     if (victim == rank) then
        victim = mod(rank + count, np)
        count = count + 1
     endif
  end function select_victim_r


  function select_victim_random(rank, np) result(victim)
     ! Purpose: decide of who to try next to get any jobs
     ! Uses primitive pseudorandom code X_n+1 = (aX_n + b)mod m
     implicit none
     integer(i4_kind), intent(in) :: rank, np
     integer(i4_kind)             :: victim
     ! *** end of interface ***

     integer(kind=i8_kind),parameter  :: a =134775813, b = 1 ! see Virtual Pascal/Borland Delphi
     integer(kind=i8_kind),parameter  :: m = 2**32
     integer(kind=i8_kind), save :: seed = -1
     ! ASSERT(.false.)
     ! Does not work yet, due to too much overflow (negative procs)
     if (seed == -1) then
        seed = rank
     endif
     seed = mod( (a*seed+b),m)
     victim = mod(seed, np-1)
     if (victim >= rank) victim = victim + 1
  end function select_victim_random

  pure function reserve_workm(m, jobs) result(n)
    ! PURPOSE: give back number of jobs to take, should be up to m, but
    ! less if there are not so many available
    implicit none
    integer(i4_kind), intent(in) :: m
    integer(i4_kind), intent(in) :: jobs(2)
    integer(i4_kind)             :: n ! result
    !** End of interface *****************************************

    n = min(jobs(2) - jobs(1), m)
    n = max(n, 0)
  end function reserve_workm

  pure function reserve_workh(m, jobs) result(n)
    ! Purpose: give back number of jobs to take, half what is there
    implicit none
    integer(i4_kind), intent(in) :: m ! FIXME: appears to be unused?
    integer(i4_kind), intent(in) :: jobs(2)
    integer(i4_kind)             :: n ! result
    !** End of interface *****************************************

    ! FIXME: comment outdated?
    ! if the number of job batches (of size m) is not odd, then
    ! the stealing proc should get more, as it starts working on them
    ! immediatelly
    ! n =  (many_jobs /(2* m))* m

    ! give half of all jobs:
    n =  (jobs(2) - jobs(1)) / 2
    n = max(n, 0)
  end function reserve_workh

  pure function steal_work_for_rma(m, jobs) result(n)
    ! Purpose: give back number of jobs to take, half what is there
    implicit none
    integer(i4_kind), intent(in) :: m ! FIXME: appears to be unused?
    integer(i4_kind), intent(in) :: jobs(2)
    integer(i4_kind)             :: n ! result
    !** End of interface *****************************************
    ! give half of all jobs:
    n =  (jobs(2) - jobs(1)) /( 2 * m) * m
    n = max(n, 0)
    ! but give more, if job numbers not odd
    n =  (jobs(2) - jobs(1)) - n

  end function steal_work_for_rma

end module dlb_common
