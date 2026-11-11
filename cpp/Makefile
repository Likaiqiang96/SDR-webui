CXX      ?= g++
CXXFLAGS ?= -O2 -std=c++17 -pthread -Wall -Wextra -Wno-unused-parameter
LIBS     := -lSoapySDR -lfftw3f -lopus -pthread
sdrwebd: sdrwebd.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LIBS)
clean:
	rm -f sdrwebd
