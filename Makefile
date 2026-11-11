CXX      ?= g++
CC       ?= cc
CXXFLAGS ?= -O2 -g -std=c++17 -fPIC -Wall -Wextra
LDLIBS   := -ldl -pthread

all: libbblite.so writer

libbblite.so: src/bblite.cpp
	$(CXX) $(CXXFLAGS) -shared $< -o $@ $(LDLIBS)

writer: test/writer.c
	$(CC) -O2 -o $@ $<

test: all
	bash test/smoke.sh

clean:
	rm -f libbblite.so writer

.PHONY: all test clean
