#
# Make has no scopes, not to pollute the namespace
# use $(DLB)/..., or DLB- prefix for targets and $(DLB-...)
# prefix for variables. A notable exception so far is the
# top-level traget
#
#       $(libdlb.a)
#

#
# We expect the before "include"ing this file the
# variable $(DLB) is set to a suitable prefix.
#
# CURDIR is set on every make or $(MAKE) -C dir:
#
DLB ?= $(CURDIR)

#
# DLB library, used for tests and in PG,
# this may be used to refer to the targets from outside:
#
libdlb.a = $(DLB)/libdlb.a

#
# This value specifes the amount of output (of DLB).
# See output level in README for description.
# (0 means no output)
#
DLB_OUTPUT_LEVEL ?= 0

#
# Depending on the implementaiton set $(DLB-fobjs) and $(DLB-cobjs),
# default variant DLB_VARIANT = 0 should work in any case:
#
DLB_VARIANT ?= 0

#
# Fortran objects:
#
DLB-fobjs =  $(DLB)/dlb.o $(DLB)/dlb_common.o

#
# C objects:
#
DLB-cobjs =

ifeq ($(DLB_VARIANT), 0)
	DLB-fobjs += $(DLB)/dlb_impl_static.o
endif

ifeq ($(DLB_VARIANT), 1)
	DLB-fobjs += $(DLB)/dlb_impl_rma.o
endif

ifeq ($(DLB_VARIANT), 2)
	DLB-fobjs += $(DLB)/dlb_impl_thread_single.o $(DLB)/dlb_impl_thread_common.o
	DLB-cobjs = $(DLB)/thread_wrapper.o
endif

ifeq ($(DLB_VARIANT), 3)
	DLB-fobjs += $(DLB)/dlb_impl_thread_multiple.o $(DLB)/dlb_impl_thread_common.o
	DLB-cobjs = $(DLB)/thread_wrapper.o
endif

DLB-fobjs +=  $(DLB)/dlb_assert_failed.o $(DLB)/dlb_mpi.o

#
# This is the DLB library:
#
$(libdlb.a): $(DLB-fobjs) $(DLB-cobjs)
	$(AR) ruv $@  $(^)
	$(RANLIB) $@

DLB-clean:
	rm -f $(libdlb.a)

.PHONY: DLB-clean

#
# Below we modify "global" variables, prerequisites of
# top-level targets and set target specific compilation
# flags ...
#

#
# These (global) variables (f90objs, cobjs) are used in Make.rules to build and
# include dependencies:
#
f90objs += $(DLB-fobjs)
cobjs += $(DLB-cobjs)

#
# For historical reasons DLB files are compiled/preprocessed
# with different flags:
#

#
# Set _XOPEN_SOURCE=500 to make rwlocks available
#
$(DLB-cobjs): CFLAGS = -Wall -g -O1 -std=c99 -D_XOPEN_SOURCE=500
$(DLB-fobjs:.o=.F90): FPPOPTIONS += -DFPP_OUTPUT_BORDER=$(DLB_OUTPUT_LEVEL)

#
# This is also a top-level (global) target, extend the list
# of dependencies:
#
clean: DLB-clean
