#
#  Makefile
#  YCSB-cpp
#
#  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
#  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
#  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
#


#---------------------build config-------------------------

# Database bindings
BIND_WIREDTIGER ?= 0
BIND_LEVELDB ?= 0
BIND_ROCKSDB ?= 0
BIND_LMDB ?= 0
BIND_TREELINE ?= 0

# Extra options
DEBUG_BUILD ?= 0
EXTRA_CXXFLAGS ?=
EXTRA_LDFLAGS ?=

# HdrHistogram for tail latency report
BIND_HDRHISTOGRAM ?= 1
# Build and statically link library, submodule required
BUILD_HDRHISTOGRAM ?= 1

#----------------------------------------------------------

ifeq ($(DEBUG_BUILD), 1)
	CXXFLAGS += -g
else
	CXXFLAGS += -O2
	CPPFLAGS += -DNDEBUG
endif

ifeq ($(BIND_WIREDTIGER), 1)
	LDFLAGS += -lwiredtiger -ltcmalloc
#-lz -lsnappy -lzstd -lbz2 -llz4
	SOURCES += $(wildcard wiredtiger/*.cc)
endif

ifeq ($(BIND_LEVELDB), 1)
	LDFLAGS += -lleveldb
	SOURCES += $(wildcard leveldb/*.cc)
endif

ifeq ($(BIND_ROCKSDB), 1)
	EXTRA_LDFLAGS += -L/home/dzl/rocksdb
	LDFLAGS += -lrocksdb -ldl -lz 
# -lsnappy -lzstd -lbz2 -llz4
	EXTRA_CXXFLAGS += -I/home/dzl/rocksdb/include
	SOURCES += $(wildcard rocksdb/*.cc)
endif

ifeq ($(BIND_LMDB), 1)
	LDFLAGS += -llmdb
	SOURCES += $(wildcard lmdb/*.cc)
endif

ifeq ($(BIND_TREELINE_CoW), 1)
	EXTRA_LDFLAGS += -L/home/dzl/treeline_CoW/build -L/home/dzl/treeline_CoW/build/_deps/crc32c-build -L/home/dzl/treeline_CoW/build/third_party/masstree -L/home/dzl/treeline_CoW/build/page_grouping
	LDFLAGS += -lpg_treeline_cow -lmasstree -lpg -lcrc32c
	EXTRA_CXXFLAGS += -I/home/dzl/treeline_CoW/include
	SOURCES += $(wildcard treeline_CoW/*.cc)
endif

ifeq ($(BIND_TREELINE), 1)
	EXTRA_LDFLAGS += -L/home/dzl/treeline/build -L/home/dzl/treeline/build/_deps/crc32c-build -L/home/dzl/treeline/build/third_party/masstree -L/home/dzl/treeline/build/page_grouping
	LDFLAGS += -lpg_treeline -lmasstree -lpg -lcrc32c
	EXTRA_CXXFLAGS += -I/home/dzl/treeline/include
	SOURCES += $(wildcard treeline/*.cc)
endif

ifeq ($(BIND_LSM2LIX), 1)
	EXTRA_LDFLAGS += -L/home/dzl/LSM-LIX
	EXTRA_LDFLAGS += -L/home/dzl/treeline/build -L/home/dzl/treeline/build/_deps/crc32c-build -L/home/dzl/treeline/build/third_party/masstree -L/home/dzl/treeline/build/page_grouping
	EXTRA_LDFLAGS += -L/home/dzl/rocksdb
	LDFLAGS += -llsm2lix
	LDFLAGS += -lpg_treeline -lmasstree -lpg -lcrc32c
	LDFLAGS += -lrocksdb -ldl -lz
	LDFLAGS += -lboost_serialization
	EXTRA_CXXFLAGS += -I/home/dzl/LSM-LIX/include
	EXTRA_CXXFLAGS += -I/home/dzl/treeline/include
	EXTRA_CXXFLAGS += -I/home/dzl/rocksdb/include

	SOURCES += $(wildcard lsm2lix/*.cc)
endif

CXXFLAGS += -std=c++17 -Wall -pthread $(EXTRA_CXXFLAGS) -I./
LDFLAGS += $(EXTRA_LDFLAGS) -lpthread
SOURCES += $(wildcard core/*.cc)
OBJECTS += $(SOURCES:.cc=.o)
DEPS += $(SOURCES:.cc=.d)
EXEC = ycsb

HDRHISTOGRAM_DIR = HdrHistogram_c
HDRHISTOGRAM_LIB = $(HDRHISTOGRAM_DIR)/src/libhdr_histogram_static.a

ifeq ($(BIND_HDRHISTOGRAM), 1)
ifeq ($(BUILD_HDRHISTOGRAM), 1)
	CXXFLAGS += -I$(HDRHISTOGRAM_DIR)/include
	OBJECTS += $(HDRHISTOGRAM_LIB)
else
	LDFLAGS += -lhdr_histogram
endif
CPPFLAGS += -DHDRMEASUREMENT
endif

all: $(EXEC)

$(EXEC): $(OBJECTS)
	@$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@
	@echo "  LD      " $@

.cc.o:
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<
	@echo "  CC      " $@

%.d: %.cc
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MM -MT '$(<:.cc=.o)' -o $@ $<

$(HDRHISTOGRAM_DIR)/CMakeLists.txt:
	@echo "Download HdrHistogram_c"
	@git submodule update --init

$(HDRHISTOGRAM_DIR)/Makefile: $(HDRHISTOGRAM_DIR)/CMakeLists.txt
	@cmake -DCMAKE_BUILD_TYPE=Release -S $(HDRHISTOGRAM_DIR) -B $(HDRHISTOGRAM_DIR)


$(HDRHISTOGRAM_LIB): $(HDRHISTOGRAM_DIR)/Makefile
	@echo "Build HdrHistogram_c"
	@make -C $(HDRHISTOGRAM_DIR)

ifneq ($(MAKECMDGOALS),clean)
-include $(DEPS)
endif

clean:
	find . -name "*.[od]" -delete
	$(RM) $(EXEC)

.PHONY: clean
