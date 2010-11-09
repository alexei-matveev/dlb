module test

  implicit none
  private

  public :: echo

  integer, private, save :: global_state

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

  subroutine burn_cpu(num)
    implicit none
    integer, intent(in) :: num
    ! *** end of interface ***

    integer :: i = 8
    integer, parameter :: LOOP = 7

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
    global_state = num *2 - 90 + i ** 2 + num ** 3 /7 + k
  end subroutine burn_cpu

end module test
