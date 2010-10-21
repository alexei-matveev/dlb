# -*- makefile -*-
#  ### Makefile for utilites

#### NAME OF EXECUTABLE ####
EXE = test_dlbmpi
EXE2      = test_dlb
EXE3      = test_static

CEXE = $(EXE)

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
FFLAGS = -g -O2 #-fbounds-check
CFLAGS = -g -O1
LINKFLAGS =

#### LDFLAGS, LIBRARY-PATH ####
LIBS = $(MPILIBS) $(THREADLIBS)

all: $(CEXE)

#FFSOURCES = main.f90 test.f90 dlb_module.f90
EXESOURCE = main.o test.o dlbmpi_module.o thread_wrapper.o
EXESOURCE2 = main.o test.o dlb_module.o
EXESOURCE3 = main.o test.o dlb_static.o

dlbmpi_module.o: thread_wrapper.o
main.o: test.o dlbmpi_module.o dlb_module.o dlb_static.o thread_wrapper.o

%.o: %.cpp
	$(CC) $(THREADLIBS) $(CCLIBS) -c thread_wrapper.cpp

%.o: %.f90
	$(FC) $(FFLAGS) $(LIBS) $(MPIINCLUDE) -c $<

$(EXE): $(EXESOURCE)
	$(FC) $(FFLAGS) $(LINKFLAGS) $(LIBS) $(CCLIBS) $(MPIINCLUDE) $(EXESOURCE) -o $(EXE)

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

