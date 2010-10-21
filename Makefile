# -*- makefile -*-
#  ### Makefile for utilites

# executable:
EXE = test_dlb

# default build target:
default: $(EXE)

# compilers:
FC = mpif90
CC = gcc

MPIINCLUDE =    #-I/usr/include/mpi
MPILIBS =       #-lmpi_f77 -lmpi #-lopen-rte -lopen-pal
THREADLIBS = -lpthread

#### COMPILER FLAGS ####
FFLAGS = -frecursive -g -O2 #-fbounds-check # Intel: -diag-enable warn
CFLAGS = -Wall -g -O1 -std=c99
LINKFLAGS =

#### LDFLAGS, LIBRARY-PATH ####
LIBS = $(MPILIBS) #$(THREADLIBS)

#
# Objects common for all implementations:
#
objs = main.o test.o dlb_common.o

#
# Depending on the target set $(dlb_objs):
#
#dlb_objs = dlb_static.o
#dlb_objs = dlb_module.o
dlb_objs = dlbmpi_module.o thread_wrapper.o

# WHY? dlbmpi_module.o: thread_wrapper.o

main.o: test.o $(dlb_objs)
dlb_module.o dlbmpi_module.o: dlb_common.o

%.o: %.c
	$(CC) $(CFLAGS) -c $(<)

%.o: %.f90
	$(FC) $(FFLAGS) $(MPIINCLUDE) -c $(<)

#
# $(@) stays for the target (respective executable)
# $(^) stays for all prerequisites, see deps above
#
$(EXE): $(objs) $(dlb_objs)
	$(FC) $(FFLAGS) $(LINKFLAGS) $(LIBS) $(MPIINCLUDE) $(^) -o $(@)

##### SPECIAL COMMANDS #####
clean:
	rm -f *.o
	rm -f *.mod
	rm -f *~
	rm -f $(EXE)

