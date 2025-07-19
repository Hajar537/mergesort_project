CXX = g++
CXXFLAGS = -std=c++17 -O2 -fopenmp
TARGET = mergesort_omp
SRC = main_omp.cpp record.cpp

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRC)

clean:
	rm -f $(TARGET) ~/mergesort_project/data/temp/*.bin

