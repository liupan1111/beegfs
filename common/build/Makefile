include ../../build/Makefile

COMMON_CODEGEN_SOURCES = \
	../codegen/GenerateConfigFields.cpp

COMMON_CODEGEN_OUTPUTS = \
	../generated/CommonConfigFields.h \
	../generated/CommonConfigFields.inc

../source/common/toolkit/ring/rte_ring_standalone.c.o: ../source/common/toolkit/ring/rte_ring_standalone.c
	@echo "[CC]    $<"
	$V$(CC) -std=c11 -D_GNU_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -fPIC \
		-Wall -Wextra -Wno-unused-parameter -MMD -MF$<.d -MT$<.o -MP -o$<.o \
		-c $(realpath $<)

$(call build-codegen,\
   ../build/CommonConfigFields_generate,\
   $(COMMON_CODEGEN_SOURCES),\
   beegfs-base beegfs-codegen,\
   ../source,\
   $(COMMON_CODEGEN_OUTPUTS))

$(call build-static-library,\
   beegfs-common,\
   $(shell find ../source/common -iname '*.cpp') \
      ../source/common/toolkit/ring/rte_ring_standalone.c,\
   beegfs-base dl nl3-route,\
   ../source,\
   $(COMMON_CODEGEN_OUTPUTS))

$(call build-shared-library,\
   beegfs_ib,\
   $(shell find ../ib_lib -iname '*.cpp'),\
   beegfs-base rdma,\
   ../source ../ib_lib,\
   $(COMMON_CODEGEN_OUTPUTS))

$(call build-test,\
   test-runner,\
   $(shell find ../tests -name '*.cpp'),\
   beegfs-base common,\
   ../tests,\
   $(COMMON_CODEGEN_OUTPUTS))
