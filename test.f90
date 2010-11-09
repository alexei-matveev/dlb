module test

  implicit none
  private
  public :: test_calc

  integer :: i = 8
  integer, parameter :: LOOP = 7

  contains

  integer function test_calc(num)
    implicit none
    integer, intent(in) :: num
    ! *** end of interface ***

    test_calc = test_calc2(num)
  end function test_calc

  function test_calc1(num)
    implicit none
    integer, intent(in) :: num
    integer :: test_calc1
    ! *** end of interface ***

    call sleep(1)
    !if (mod(num,4) == 0) call sleep(3)
    test_calc1 = num *2 - 90 + i ** 2 + num ** 3 /7
  end function test_calc1

  function test_calc2(num)
    implicit none
    integer, intent(in) :: num
    ! *** end of interface ***

    integer :: test_calc2
    integer :: j, l , l1, l2, l3, l4, l5, l6, l7, l8, l9
    real :: k

    k = 0
    do j = 1, LOOP
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
    test_calc2 = num *2 - 90 + i ** 2 + num ** 3 /7 + k
  end function test_calc2
end module test
