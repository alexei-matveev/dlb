# -*- makefile -*-
#  ### Makefile for dlb library

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

# for inclusion of for example the right fortran (or C) compiler
# But be aware that the compiler flags will be reset
include ../machine.inc

#### RESET COMPILER FLAGS ####
FFLAGS = -frecursive -g -O2 #-fbounds-check # Intel: -diag-enable warn
CFLAGS = -Wall -g -O1 -std=c99 -D_XOPEN_SOURCE=500
# set _XOPEN_SOURCE=500 to make rwlocks available

CPP = cpp --traditional-cpp
#CPP = cpp --traditional-cpp  -I../include

#
# Depending on the target set $(dlb_objs):
#
dlb_objs = dlb_impl_static.o
#dlb_objs = dlb_impl_rma.o
#dlb_objs = dlb_impl_thread_multiple.o thread_handle.o thread_wrapper.o
#dlb_objs = dlb_impl_thread_single.o thread_handle.o thread_wrapper.o

# in the library should also be the genearl file as well as the extensions
objs =  dlb2.o dlb_common.o $(dlb_objs)

# This is the dlb library
$(libdlb.a): $(objs)
	$(AR) ruv $@  $(^)
	$(RANLIB) $@

# for including the library in the test example
LIBS = -L. -ldlb

# dependencies
dlb_impl_rma.o dlb_impl_thread_multiple.o thread_handle.o dlb_impl_thread_single.o dlb_impl_static.o: dlb_common.o
dlb_impl_thread_multiple.o: thread_handle.o thread_wrapper.o
dlb_impl_thread_single.o: thread_handle.o thread_wrapper.o
thread_handle.o: thread_wrapper.o
dlb2.o: $(dlb_objs)
main.o: test.o $(libdlb.a)

%.o: %.c
	$(CC) $(CFLAGS) -c $(<)

.PRECIOUS: %.F90
%.F90: %.f90
	$(CPP) $(<) > $(*).F90

%.o: %.F90
	$(FC) $(FFLAGS) -c $(<)

# this is how the test example should be build
$(test_dlb): main.o test.o $(libdlb.a)
	$(FC) $(FFLAGS) $(LIBS) $(^) -o $(@)

##### SPECIAL COMMANDS #####
clean:
	rm -f *.o
	rm -f *.F90
	rm -f *.mod
	rm -f *~
	rm -f $(test_dlb)
	rm -f $(libdlb.a)
