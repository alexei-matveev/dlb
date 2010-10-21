!==========================================================
! Interface for testing
!=========================================================
program main
! Purpose: starts tests
! End of public interface

use dlbmpi_module
use test2, only: test_calc, start_thread
!use dlbmpi_module
!use mpi
!include 'def.h'
!use pthread
implicit none
include 'mpif.h'
integer :: rank, n_procs
integer :: ierr, req, prov
integer :: num_jobs, i, res
integer :: my_jobs(2), my_amount
integer :: n, alloc_stat
integer, pointer :: sec(:)
integer, allocatable, target :: my_parts(:)
integer :: job(2)
logical :: mwork
!type(F_PTHREAD_T) :: thread
!type(th_name) :: thread
integer :: thread
integer :: tmp

n = 3
num_jobs = 200
allocate(my_parts(n), stat = alloc_stat)
if (alloc_stat /= 0) print *, "ALLOCATION error"
print *, "possible treads", MPI_THREAD_SINGLE, MPI_THREAD_FUNNELED, MPI_THREAD_SERIALIZED, MPI_THREAD_MULTIPLE
req = MPI_THREAD_MULTIPLE
call MPI_INIT_THREAD(req, prov, ierr)
print *, "got thread environment:", prov, "instead of", req
call MPI_COMM_RANK( MPI_COMM_WORLD, rank, ierr )
call MPI_COMM_SIZE( MPI_COMM_WORLD, n_procs, ierr)
call dlb_init()
call timer("START")
print *, "Hello World!, I'm", rank
!tmp = f_pthread_create(thread, attr, flag, thread_function,2)
!thread = 1
!call th_create(thread)
!call start_thread()
!call th_create2(thread_function2)
!print *, "Create Thread", tmp

my_amount = num_jobs / n_procs
my_jobs(1) = (rank) * my_amount
my_jobs(2) = my_jobs(1) + my_amount
if (rank+1 == n_procs) my_jobs(2) = num_jobs
!call MPI_BARRIER(MPI_COMM_WORLD, ierr)
!call timer("startloop0")
!do while (more_points(n, sec))
!   do i = 1, size(sec)
!     res = test_calc(sec(i))
!     print *,rank,": My result for",sec(i)," is", res, "with", my_jobs
!   enddo
!enddo
!call timer("endloop0")

 call MPI_BARRIER(MPI_COMM_WORLD, ierr)
if (rank+1 == n_procs) then
  my_jobs(1) = 0
  my_jobs(2) = num_jobs
else
  my_jobs(1) = 0
  my_jobs(2) = 0
endif
print *, rank, ":My jobs:", my_jobs

if (rank == 0) print *, "**************************************************"
call MPI_BARRIER(MPI_COMM_WORLD, ierr)
call timer("startloop1")
call dlb_setup(my_jobs,.false.)
do while (more_work(n, sec))
   do i = 1, size(sec)
     res = test_calc(sec(i))
     print *,rank,": My result for",sec(i)," is", res, "with", my_jobs
   enddo
enddo
call timer("endloop1")
call MPI_BARRIER(MPI_COMM_WORLD, ierr)
if (rank+1 == n_procs) then
  my_jobs(1) = 0
  my_jobs(2) = num_jobs
else
  my_jobs(1) = 0
  my_jobs(2) = 0
endif
if (rank == 0) print *, "**************************************************"
!call MPI_BARRIER(MPI_COMM_WORLD, ierr)
!call timer("startloop2")
!call dlb_setup(my_jobs,.true.)
!do while (more_work(n, sec))
!   do i = 1, size(sec)
!     res = test_calc(sec(i))
!     print *,rank,": My result for",sec(i)," is", res, "with", my_jobs
!   enddo
!enddo
!call timer("endloop2")

call MPI_BARRIER(MPI_COMM_WORLD, ierr)

!my_amount = num_jobs / n_procs
!my_jobs(1) = (rank) * my_amount
!my_jobs(2) = my_jobs(1) + my_amount
!if (rank+1 == n_procs) my_jobs(2) = num_jobs
!if (rank == 0) print *, "**************************************************"
!call MPI_BARRIER(MPI_COMM_WORLD, ierr)
!call timer("startloop3")
!call dlb_setup(my_jobs,.false.)
!call MPI_BARRIER(MPI_COMM_WORLD, ierr)
!!mwork = more_points(n, sec)
!!do while (mwork)
!do while (more_work(n, sec))
!!  call dlb_give_more(n, job)
!   do i = 1, size(sec)
!     res = test_calc(sec(i))
!     print *,rank,": My result for",sec(i)," is", res, "with", my_jobs
!   enddo
!enddo
!call timer("endloop3")
!call MPI_BARRIER(MPI_COMM_WORLD, ierr)
!my_amount = num_jobs * 0.7 / n_procs
!my_jobs(1) = (rank) * my_amount
!my_jobs(2) = my_jobs(1) + my_amount
!if (rank+1 == n_procs) my_jobs(2) = num_jobs
!if (rank == 0) print *, "**************************************************"
!call MPI_BARRIER(MPI_COMM_WORLD, ierr)
!call timer("startloop4")
!call dlb_setup(my_jobs,.true.)
!!call MPI_BARRIER(MPI_COMM_WORLD, ierr)
!!mwork = more_points(n, sec)
!!do while (mwork)
!do while (more_work(n, sec))
!!  call dlb_give_more(n, job)
!   do i = 1, size(sec)
!     res = test_calc(sec(i))
!     print *,rank,": My result for",sec(i)," is", res, "with", my_jobs
!   enddo
!enddo
!call timer("endloop4")
if (rank == 0) print *, "**************************************************"
call MPI_BARRIER(MPI_COMM_WORLD, ierr)
!print *, "reached", rank
call dlb_finalize()
!print *, "reached", rank
!tmp_f_pthread_join(thread)

call MPI_BARRIER(MPI_COMM_WORLD, ierr)
!print *, "ENd Thread", tmp
call MPI_FINALIZE(ierr)

 contains
  logical function more_work(n, slice)
    integer, intent(in) :: n
    integer, intent(out), pointer :: slice(:)
    integer :: job(2)
    call dlb_give_more(n,job)
    my_jobs = job
    more_work = more_points(n, slice)
  end function more_work

  logical function more_points(n, slice)
    integer, save :: counter = 0
    integer, intent(in) :: n
    integer, intent(out), pointer :: slice(:)
    integer :: i
    integer :: job(2)
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
  subroutine thread_function2(id)
    use test2
    implicit none
    integer :: id, tmp
    print *, "hello world, I'm thread", id
    tmp = test_calc(id+1)
    print *, "RESULT:", tmp
    call th_exit()
  end subroutine thread_function2

   subroutine timer(place )
    implicit none
    character(len=*), intent(in) :: place

    print *,"GGG", rank, place,  MPI_Wtime()
  end subroutine timer
end program main
