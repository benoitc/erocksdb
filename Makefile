BUILD_CONFIG = make_config.mk
REBAR ?= rebar3


all: compile test

get-deps:
	./c_src/build_deps.sh get-deps

rm-deps:
	./c_src/build_deps.sh rm-deps

compile:
	./c_src/build_deps.sh
	PLATFORM_LDFLAGS="`cat c_src/rocksdb/${BUILD_CONFIG} |grep PLATFORM_LDFLAGS| awk -F= '{print $$2}'|sed -e 's/-lsnappy//'`" $(REBAR) compile

test: compile
	$(REBAR) eunit

clean:
	$(REBAR) clean
