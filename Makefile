path := $(shell find / -name "rdkafkacpp.h" 2> /dev/null)

OBJS = test.o connector.o flow-messages-enriched.o

HPP = connector.hpp flow-messages-enriched.pb.h

CC = g++
OPTIONS = -c
INCLUDINGS = -include $(path)
PREFLAGS = -Wall -std=gnu++17 -g
POSTFLAGS = -lrdkafka++ -lcrypto -lprotobuf

test: test.o connector.o flow-messages-enriched.o
	$(CC) $(PREFLAGS) -o test $(OBJS) $(POSTFLAGS)

test.o: $(HPP) test.cpp
	$(CC) $(OPTIONS) $(PREFLAGS) $(INCLUDINGS) test.cpp -o test.o $(POSTFLAGS)

connector.o: $(HPP) connector.cpp
	$(CC) $(OPTIONS) $(PREFLAGS) $(INCLUDINGS) connector.cpp -o connector.o $(POSTFLAGS)

flow-messages-enriched.o: $(HPP) flow-messages-enriched.pb.cc
	$(CC) $(OPTIONS) $(PREFLAGS) flow-messages-enriched.pb.cc -o flow-messages-enriched.o $(POSTFLAGS)

clean:
	rm -f *.o test
