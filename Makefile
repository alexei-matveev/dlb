# -*- makefile -*-
#  ### Makefile for utilites

v v v v v v v
# executable:
EXE = test_dlb
*************
#### NAME OF EXECUTABLE ####
v v v v v v v
EXE = test_dlb
*************
EXE = test_dlbmpi
EXE2      = test_dlb
EXE3      = test_static

CEXE = $(EXE)
^ ^ ^ ^ ^ ^ ^
^ ^ ^ ^ ^ ^ ^

# default build target:
default: $(EXE)

# compilers:
FC = mpif90
CC = gcc

MPIINCLUDE =    -I/usr/include/mpi
MPILIBS =       -lmpi_f77 -lmpi
THREADLIBS = -lpthread #-lfpthread
CCLIBS = -lstdc++

#### COMPILER FLAGS ####
FFLAGS = -Wall -g -O2 #-fbounds-check
CFLAGS = -Wall -g -O1 -std=c99
LINKFLAGS =

#### LDFLAGS, LIBRARY-PATH ####
LIBS = $(MPILIBS) #$(THREADLIBS)

#
# Objects common for all implementations:
#
objs = main.o test.o

v v v v v v v
#
# Depending on the target set $(dlb_objs):
#
#dlb_objs = dlb_static.o
#dlb_objs = dlb_module.o
dlb_objs = dlbmpi_module.o thread_wrapper.o

# WHY? dlbmpi_module.o: thread_wrapper.o
*************
v v v v v v v
#
# Change this depending on DLB implementation:
#
dlb_objs = dlb_module.o
#dlb_objs = dlbmpi_module.o thread_wrapper.o

test_dlb: main.o test.o $(dlb_objs)
*************
#FFSOURCES = main.f90 test.f90 dlb_module.f90
EXESOURCE = main.o test.o dlbmpi_module.o thread_wrapper.o
EXESOURCE2 = main.o test.o dlb_module.o
EXESOURCE3 = main.o test.o dlb_static.o
^ ^ ^ ^ ^ ^ ^
^ ^ ^ ^ ^ ^ ^

v v v v v v v
main.o: test.o $(dlb_objs)
*************
v v v v v v v
dlbmpi_module.o test.o: thread_wrapper.o

main.o: test.o $(dlb_objs)
*************
dlbmpi_module.o: thread_wrapper.o
main.o: test.o dlbmpi_module.o dlb_module.o dlb_static.o thread_wrapper.o
^ ^ ^ ^ ^ ^ ^
^ ^ ^ ^ ^ ^ ^

%.o: %.c
	$(CC) $(CFLAGS) -c $(<)

%.o: %.f90
	$(FC) $(FFLAGS) $(MPIINCLUDE) -c $(<)

#
# $(@) stays for the target (respective executable)
# $(^) stays for all prerequisites, see deps above
#
$(EXE): $(objs) $(dlb_objs)
	$(FC) $(FFLAGS) $(LINKFLAGS) $(LIBS) $(CCLIBS) $(MPIINCLUDE) $(^) -o $(@)

##### SPECIAL COMMANDS #####
clean:
	rm -f *.o
	rm -f *.mod
	rm -f *~
	rm -f $(EXE)

