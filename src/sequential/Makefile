CXX = g++
CXXFLAGS = -std=c++17 -O2
TARGET = mergesort_seq
SRC = main.cpp record.cpp

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRC)

clean:
	rm -f $(TARGET) ~/mergesort_project/data/temp/*.bin

