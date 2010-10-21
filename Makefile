# -*- makefile -*-
#  ### Makefile for utilites

#### NAME OF EXECUTABLE ####
EXE = test_dlbmpi

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

all: $(EXE)

#FFSOURCES = main.f90 test.f90 dlb_module.f90
EXESOURCE = main.o test.o dlbmpi_module.o thread_wrapper.o

dlbmpi_module.o test.o: thread_wrapper.o
main.o: test.o dlbmpi_module.o

%.o: %.cpp
	$(CC) $(THREADLIBS) $(CCLIBS) -c thread_wrapper.cpp

%.o: %.f90
	$(FC) $(FFLAGS) $(LIBS) $(MPIINCLUDE) -c $<

$(EXE): $(EXESOURCE)
	$(FC) $(FFLAGS) $(LINKFLAGS) $(LIBS) $(CCLIBS) $(MPIINCLUDE) $(EXESOURCE) -o $(EXE)

##### SPECIAL COMMANDS #####
clean:
	rm -f *.o
	rm -f *.mod
	rm -f *~
	rm -f $(EXE)

