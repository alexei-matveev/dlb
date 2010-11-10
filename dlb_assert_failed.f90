subroutine dlb_assert_failed(expr, file, line)
  implicit none
  character(len=*), intent(in) :: expr, file
  integer,          intent(in) :: line
  ! *** end of interface ***

  character(len=9) :: buf

  write(buf,'(i9)') line

  print *, "Assert failed: ", trim(expr), "; at ", trim(file)//":"//trim(adjustl(buf))

  call abort()
end subroutine dlb_assert_failed
