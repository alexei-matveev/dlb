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

  public :: time_stamp_prefix
  public :: time_stamp
  public :: assert_n

  ! integer with 4 bytes, range 9 decimal digits
  integer, parameter, public :: i4_kind = selected_int_kind(9)

  ! real with 8 bytes, precision 15 decimal digits
  integer, parameter, public :: r8_kind = selected_real_kind(15)

  integer, parameter, public :: comm_world = MPI_COMM_WORLD

  !================================================================
  ! End of public interface of module
  !================================================================

  !------------ Declaration of types ------------------------------

  !------------ Declaration of constants and variables ----

  integer(i4_kind), public :: dlb_common_my_rank = -1 ! for debug prints only

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

  subroutine time_stamp(msg)
    implicit none
    character(len=*), intent(in) :: msg
    ! *** end of interface ***

    double precision :: time

    time = MPI_Wtime()
    if ( time_offset < 0.0 ) then
      time_offset = 0.0
!     print *, time_stamp_prefix(time), "(BASE TIME)"
      time_offset = time
    endif

!   print *, time_stamp_prefix(time), msg
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

     victim = select_victim_random2(rank, np)
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

  function select_victim_random2(rank, np) result(victim)
     ! Purpose: decide of who to try next to get any jobs
     ! Uses primitive pseudorandom code X_n+1 = (aX_n + b)mod m
     implicit none
     integer(i4_kind), intent(in) :: rank, np
     integer(i4_kind)             :: victim
     ! *** end of interface ***

     !integer, save, dimension(1) :: random
     integer, allocatable, save :: random(:)
     real(kind=r8_kind), allocatable     ::  harv(:)
     integer(kind=i4_kind)  :: i, s, alloc_stat

     !f (random(1)==-1) random(1) = rank
     call random_seed()
     call random_seed(size=s)
     if (.not. allocated(random)) then
       allocate(random(s), stat=alloc_stat)
       !ASSERT(alloc_stat==0)
       call assert_n(alloc_stat==0, 4)
       call random_seed(get=random)
       do i = 1, s
          random(i) = rank + i
       enddo
!      print *, rank, s, random
     endif
     allocate(harv(s), stat=alloc_stat)
     !ASSERT(alloc_stat==0)
     !all assert_n(alloc_stat==0, 4)

     !print *,rank, "random=", random
     call random_seed(put=random)
     call random_number(harv)
     !print *,rank, "harv=",harv
     victim = int(harv(1) * np)
     i = 0
     do while (victim == rank .and. i < 10)
       call random_number(harv)
       victim = int(harv(1) * np)
       i = i + 1
     enddo
     if ((victim == np) .or. (victim == rank)) then
       victim = select_victim_r(rank, np)
     endif
     call random_number(harv)
     call random_seed(get=random)
  end function select_victim_random2

  function select_victim_random(rank, np) result(victim)
     ! Purpose: decide of who to try next to get any jobs
     ! Uses primitive pseudorandom code X_n+1 = (aX_n + b)mod m
     implicit none
     integer(i4_kind), intent(in) :: rank, np
     integer(i4_kind)             :: victim
     ! *** end of interface ***

     integer(kind=i4_kind),parameter  :: a =134775813, b = 1 ! see Virtual Pascal/Borland Delphi
     integer(kind=i4_kind),parameter  :: m = 1000 ! FIXME: shoud we use other integer for 2**32 (fit to rest)
     integer(kind=i4_kind), save :: seed = -1
     integer(kind=i4_kind)  :: i, seedold

     ! ASSERT(.false.)
     ! Does not work yet, due to too much overflow (negative procs)
     if (seed == -1) then
        seed = rank
     endif
     seedold = seed
     seed = mod( (a*seed+b),m)
     victim = mod(seed, np)
     i = 0
     do while (victim == rank .and. i < 10)
       seed = mod( (a*seed+b),m)
       victim = mod(seed, np)
       i = i + 1
     enddo
!    print *, rank, seedold,a*seedold+b , seed, select_victim_random, i
     if (victim == rank) victim = select_victim_r(rank, np)
  end function select_victim_random

end module dlb_common
