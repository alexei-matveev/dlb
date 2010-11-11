module test

  implicit none
  private

  public :: echo

  real, private, save :: global_state

  contains

  integer function echo(i)
    implicit none
    integer, intent(in) :: i
    ! *** end of interface ***

    call burn_cpu(i)

    !
    ! Echo input:
    !
    echo = i
  end function echo

  subroutine burn_cpu(i)
    use dlb_common, only: irand
    implicit none
    integer, intent(in) :: i
    ! *** end of interface ***

    integer, parameter :: LOOP = 7
    integer, parameter :: IMIN = LOOP - 3
    integer, parameter :: IMAX = LOOP + 3
    integer(8) :: i8 ! long int

    integer :: random_cost

    integer :: j, l , l1, l2, l3, l4, l5, l6, l7, l8, l9
    real :: k

    !
    ! A pseudo random number between IMIN and IMAX:
    !
    i8 = i ! conversion to long int
    random_cost = IMIN + mod(irand(irand(irand(i8))), IMAX - IMIN + 1)
    ! print *, "random_cost(", i,") =", random_cost, "between", IMIN, "and", IMAX

    k = 0
    !
    ! Randomize burned CPU time:
    !
    do j = 1, random_cost
      k = real(j)
      k = k + sqrt(k)
    do l = 1, LOOP
      k = real(j)
      k = k + sqrt(k)
    do l1 = 1, LOOP
      k = real(j)
      k = k + sqrt(k)
    do l2 = 1, LOOP
      k = real(j)
      k = k + sqrt(k)
    do l3 = 1, LOOP
      k = real(j)
      k = k + sqrt(k)
    do l4 = 1, LOOP
      k = real(j)
      k = k + sqrt(k)
    do l5 = 1, LOOP
      k = real(j)
      k = k + sqrt(k)
    do l6 = 1, LOOP
      k = real(j)
      k = k + sqrt(k)
    do l7 = 1, LOOP
      k = real(j)
      k = k + sqrt(k)
    do l8 = 1, LOOP
      k = real(j)
      k = k + sqrt(k)
    do l9 = 1, LOOP
      k = real(j)
      k = k + sqrt(k)
    enddo
    enddo
    enddo
    enddo
    enddo
    enddo
    enddo
    enddo
    enddo
    enddo
    enddo
    !
    ! make result dependent on k, otherwise all loops above
    ! are optimized out:
    !
    global_state = k
  end subroutine burn_cpu

end module test
