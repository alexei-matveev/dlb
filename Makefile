# -*- makefile -*-
#  ### Makefile for utilites

#### NAME OF EXECUTABLE ####
v v v v v v v
EXE = test_dlb
*************
EXE = test_dlbmpi
EXE2      = test_dlb
EXE3      = test_static

CEXE = $(EXE)
^ ^ ^ ^ ^ ^ ^

#### COMPILER ####
FC = mpif90
#FC =  mpif77
CC = gcc

MPIINCLUDE =    -I/usr/include/mpi
MPILIBS =       -lmpi_f77 -lmpi
THREADLIBS = -lpthread #-lfpthread
CCLIBS = -lstdc++

#### COMPILER FLAGS ####
# These are not critical:
FFLAGS = -Wall -g -O2 #-fbounds-check
CFLAGS = -Wall -g -O1
LINKFLAGS =

#### LDFLAGS, LIBRARY-PATH ####
LIBS = $(MPILIBS) #$(THREADLIBS)

all: $(CEXE)

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

v v v v v v v
dlbmpi_module.o test.o: thread_wrapper.o

main.o: test.o $(dlb_objs)
*************
dlbmpi_module.o: thread_wrapper.o
main.o: test.o dlbmpi_module.o dlb_module.o dlb_static.o thread_wrapper.o
^ ^ ^ ^ ^ ^ ^

%.o: %.cpp
	$(CC) $(THREADLIBS) $(CCLIBS) -c thread_wrapper.cpp

%.o: %.f90
	$(FC) $(FFLAGS) $(LIBS) $(MPIINCLUDE) -c $<

#
# $(@) stays for the target (respective executable)
# $(^) stays for all prerequisites, see deps above
#
$(EXE):
	$(FC) $(FFLAGS) $(LINKFLAGS) $(LIBS) $(CCLIBS) $(MPIINCLUDE) $(^) -o $(@)

$(EXE2): $(EXESOURCE2)
	$(FC) $(FFLAGS) $(LINKFLAGS) $(LIBS)$(CCLIBS) $(MPIINCLUDE) $(EXESOURCE2) -o $(EXE2)

$(EXE3): $(EXESOURCE3)
	$(FC) $(FFLAGS) $(LINKFLAGS) $(LIBS)$(CCLIBS) $(MPIINCLUDE) $(EXESOURCE3) -o $(EXE3)
##### SPECIAL COMMANDS #####
clean:
	rm -f *.o
	rm -f *.mod
	rm -f *~
	rm -f $(CEXE)

