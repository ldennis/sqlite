include ../main.mk

export SRCHOME=..
LIB=libsqlite.a

MODULE=sqlite

MEMGENOBJ=mem_$(MODULE).o
OBJS= analyze.o attach.o auth.o bitvec.o btmutex.o build.o callback.o	\
	  complete.o ctime.o decimal.o dbstat.o delete.o dttz.o		\
	  expr.o fault.o func.o global.o hash.o insert.o 	        \
	  legacy.o loadext.o main.o malloc.o mem1.o memjournal.o	\
	  mutex.o mutex_noop.o mutex_unix.o os.o os_unix.o pragma.o	\
	  prepare.o printf.o random.o resolve.o rowset.o select.o	\
	  sqlite_tunables.o status.o table.o tokenize.o trigger.o	\
	  update.o utf.o util.o vdbe.o vdbeapi.o vdbeaux.o vdbeblob.o	\
	  vdbemem.o vdbesort.o vdbetrace.o vtab.o walker.o where.o	\
	  comdb2build.o comdb2lua.o comdb2vdbe.o $(MEMGENOBJ)

include $(SRCHOME)/sqlite/sqlite_common.defines


OBJS_GEN=parse.o opcodes.o inline/serialget.o inline/memcompare.o inline/vdbecompare.o

OBJS_EXT=ext/comdb2systbl/tables.o \
ext/comdb2systbl/columns.o         \
ext/comdb2systbl/keys.o            \
ext/comdb2systbl/keyscomponents.o  \
ext/comdb2systbl/constraints.o     \
ext/comdb2systbl/tablesizes.o

SQLITE_GENC=parse.c opcodes.c inline/serialget.c inline/memcompare.c	\
inline/vdbecompare.c
SQLITE_GENH=parse.h opcodes.h keywordhash.h

CFLAGS+=$(SQLITE_FLAGS)
CFLAGS+=-Iinline -I$(SRCHOME)/sqlite					\
-I$(SRCHOME)/sqlite/ext/comdb2systbl -I$(SRCHOME)/datetime		\
-I$(SRCHOME)/util -I$(SRCHOME)/dfp/decNumber -I$(SRCHOME)/dfp/dfpal	\
-I$(SRCHOME)/csc2 -I$(SRCHOME)/cdb2api -I$(SRCHOME)/schemachange	\
-I$(SRCHOME)/bdb -I$(SRCHOME)/net -I$(SRCHOME)/protobuf			\
-I$(SRCHOME)/dlmalloc -I$(SRCHOME)/lua -I$(SRCHOME)/db			\
-I. $(OPTBBINCLUDE)

all: $(LIB)

MEMGEN=mem_$(MODULE).c

$(MEMGEN): $(patsubst %.o,%.c,$(SOURCES)) $(SQLITE_GENC) $(SQLITE_GENH)
	$(SRCHOME)/util/mem_codegen.sh $(MODULE)

SQLITE_TOOLS=mkkeywordhash lemon

$(OBJS_GEN): $(MEMGEN)
$(OBJS_EXT): $(MEMGEN)
$(OBJS): $(OBJS_GEN)
$(OBJS): $(SQLITE_TOOLS)


keywordhash.h: mkkeywordhash
	./mkkeywordhash > keywordhash.h

parse.c: lemon parse.y lempar.c
	rm -rf inline/; mkdir inline
	./lemon $(SQLITE_FLAGS) parse.y
	mv parse.h parse.h.temp
	tclsh addopcodes.tcl > parse.h
	cat parse.h vdbe.c | tclsh mkopcodeh.tcl > opcodes.h
	sort -n -b -k 3 opcodes.h | tclsh mkopcodec.tcl> opcodes.c

opcodes.h: parse.c
opcodes.c: parse.c
parse.h: parse.c

inline/memcompare.c:  parse.c vdbeaux.c inline/serialget.c
	echo '#include "serialget.c"' > inline/memcompare.c
	sed -n "/START_INLINE_MEMCOMPARE/,/END_INLINE_MEMCOMPARE/p" vdbeaux.c >> inline/memcompare.c

inline/serialget.c: parse.c vdbeaux.c
	sed -n "/START_INLINE_SERIALGET/,/END_INLINE_SERIALGET/p" vdbeaux.c > inline/serialget.c

inline/vdbecompare.c: parse.c inline/memcompare.c
	echo '/* This file, vdbecompare.c, is automatically generated from vdbeaux.c. See libsqlite.mk. */' > inline/vdbecompare.c
	echo '#include "memcompare.c"' >> inline/vdbecompare.c
	sed -n "/START_INLINE_VDBECOMPARE/,/END_INLINE_VDBECOMPARE/p" vdbeaux.c >> inline/vdbecompare.c

$(LIB): $(OBJS) $(OBJS_GEN)  $(OBJS_EXT)
	$(AR) $(ARFLAGS) $@ $^

.PHONY:
clean: 
	rm -f $(LIB) $(OBJS) $(OBJS_GEN) $(OBJS_EXT) $(SQLITE_GENC) $(SQLITE_GENH) $(SQLITE_TOOLS) mkkeywordhash.o lemon.o
	rm -rf inline
	rm -f parse.h.temp parse.out
	rm -fr $(MEMGEN) mem_$(MODULE).[ch]
