# -*- makefile -*-
#  ### Makefile for dlb library
# if used without PG, set DLB_EXTERNAL = 1
# else it will take the comilers and
# other needed variables from the machine.inc
# file of PG
DLB_EXTERNAL = 0

# This value specifes the amount of output (of DLB).
# See output level in README for description.
# (0 means no output)
OUTPUT_BORDER = 0

#
# DLB library, used for tests and in PG:
#
libdlb.a = libdlb.a

#
# Executable for independant test case:
#
test_dlb = test_dlb

#
# Default targets:
#
all: $(libdlb.a) $(test_dlb)

ifeq ($(DLB_EXTERNAL), 0)
# for inclusion of for example the right fortran (or C) compiler
# But be aware that the compiler flags will be reset
include ../machine.inc
else
FC = mpif90
FFLAGS =  -g -O2
CC = mpicc
AR = ar
RANLIB = ranlib
DLB_VARIANT = 0
endif

#### RESET COMPILER FLAGS ####
CFLAGS = -Wall -g -O1 -std=c99 -D_XOPEN_SOURCE=500
# set _XOPEN_SOURCE=500 to make rwlocks available

CPP = cpp --traditional-cpp -DOUTPUT_BORDER=$(OUTPUT_BORDER)
#CPP = cpp --traditional-cpp  -I../include

#
# Depending on the target set $(dlb_objs):
#

ifndef DLB_VARIANT
	# default variant DLB_VARIANT = 0 should work in any case
	DLB_VARIANT = 0
endif

ifeq ($(DLB_VARIANT), 0)
	dlb_objs = dlb_impl_static.o
endif
ifeq ($(DLB_VARIANT), 1)
	dlb_objs = dlb_impl_rma.o
endif
ifeq ($(DLB_VARIANT), 2)
	dlb_objs = dlb_impl_thread_single.o dlb_impl_thread_common.o thread_wrapper.o
endif
ifeq ($(DLB_VARIANT), 3)
	dlb_objs = dlb_impl_thread_multiple.o dlb_impl_thread_common.o thread_wrapper.o
endif

# in the library should also be the genearl file as well as the extensions
objs =  dlb.o dlb_common.o $(dlb_objs) \
	dlb_assert_failed.o dlb_mpi.o

# This is the dlb library
$(libdlb.a): $(objs)
	$(AR) ruv $@  $(^)
	$(RANLIB) $@

# for including the library in the test example
LIBS = -L. -ldlb $(MPILIBS)

# dependencies
dlb_impl_rma.o dlb_impl_thread_multiple.o dlb_impl_thread_common.o dlb_impl_thread_single.o dlb_impl_static.o: dlb_common.o
dlb_impl_thread_multiple.o: dlb_impl_thread_common.o thread_wrapper.o
dlb_impl_thread_single.o: dlb_impl_thread_common.o thread_wrapper.o
dlb_impl_rma.o dlb_impl_thread_multiple.o dlb_impl_thread_common.o dlb_impl_thread_single.o dlb_impl_static.o: dlb_mpi.o
main.o dlb_common.o dlb.o: dlb_mpi.o
dlb_impl_thread_common.o: thread_wrapper.o
dlb.o: $(dlb_objs) dlb_common.o
test.o: dlb_common.o
main.o: test.o $(libdlb.a)

%.o: %.c
	$(CC) $(CFLAGS) -c $(<)

.PRECIOUS: %.F90
%.F90: %.f90
	$(CPP) $(<) > $(*).F90

%.o: %.F90
	$(FC) $(FFLAGS) $(MPIINCLUDE) -c $(<)

# this is how the test example should be build
$(test_dlb): main.o test.o $(libdlb.a)
	$(FC) -o $(@) main.o test.o $(LIBS)

##### SPECIAL COMMANDS #####
clean:
	rm -f *.o
	rm -f *.F90
	rm -f *.mod
	rm -f *~
	rm -f $(test_dlb)
	rm -f $(libdlb.a)
