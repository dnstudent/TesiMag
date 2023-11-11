WARNINGS = -Wall -Wextra -Wpedantic
CPPSTD = -std=c++20

dataset: dataset.cpp
	$(CXX) -o $@ $(CXXFLAGS) $(WARNINGS) $(CPPSTD) $< $$(pkg-config --cflags --libs arrow arrow-dataset arrow-filesystem)
