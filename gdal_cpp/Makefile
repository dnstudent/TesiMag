CXXFLAGS = -Wall -Wextra -Wpedantic -Werror -std=c++17

open: 01_open.cpp
	clang++ 01_open.cpp $(shell pkg-config --cflags --libs gdal) $(CXXFLAGS) -o open