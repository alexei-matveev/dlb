module test2
#include 'thread_wrapper.h'
  implicit none
  public :: test_calc
  integer :: i = 8

  contains
  subroutine start_thread()
    integer :: k
    k = 1
    call th_create(k)
  end subroutine start_thread


  integer function test_calc(num)
    implicit none
    integer, intent(in) :: num
    integer :: o
    !integer :: i
   !do i = 1, 99999999
   !o = 1
   ! call th_mutex_lock(o)
   test_calc = test_calc1(num)
   ! call th_mutex_unlock(o)
   !enddo
  end function test_calc

  function test_calc1(num)
    implicit none
    integer, intent(in) :: num
    integer :: test_calc1
    call sleep(1)
    !if (mod(num,4) == 0) call sleep(3)
    test_calc1 = num *2 - 90 + i ** 2 + num ** 3 /7
  end function test_calc1

  function test_calc2(num)
    implicit none
    integer, intent(in) :: num
    integer :: test_calc2
    integer :: i,j, l , l1, l2, l3, l4, l5, l6, l7, l8, l9, l0
    real :: k
    ! end of interface
    k = 0
    do j = 1, 999999999
      k = real(j)
      k = k + sqrt(k)
    do l = 1, 999999999
      k = real(j)
      k = k + sqrt(k)
    do l1 = 1, 999999999
      k = real(j)
      k = k + sqrt(k)
    do l2 = 1, 999999999
      k = real(j)
      k = k + sqrt(k)
    do l3 = 1, 999999999
      k = real(j)
      k = k + sqrt(k)
    do l4 = 1, 999999999
      k = real(j)
      k = k + sqrt(k)
    do l5 = 1, 999999999
      k = real(j)
      k = k + sqrt(k)
    do l6 = 1, 999999999
      k = real(j)
      k = k + sqrt(k)
    do l7 = 1, 999999999
      k = real(j)
      k = k + sqrt(k)
    do l8 = 1, 999999999
      k = real(j)
      k = k + sqrt(k)
    do l9 = 1, 999999999
      k = real(j)
      k = k + sqrt(k)
    do l0 = 1, 999999999
      k = real(j)
      k = k + sqrt(k)
    do i = 1, 999999999
      k = real(i)
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
    enddo
    enddo
    test_calc2 = num *2 - 90 + i ** 2 + num ** 3 /7
  end function test_calc2
end module test2

  subroutine thread_function(id)
    use test2
    implicit none
    integer :: id, tmp
    print *, "hello world, I'm thread", id
    tmp = test_calc(id+1)
    print *, "RESULT:", tmp
    call th_exit()
  end subroutine
