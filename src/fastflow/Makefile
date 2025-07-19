CXX = g++
CXXFLAGS = -std=c++17 -O2 -I/home/h.makhlouf/mergesort_project/fastflow -pthread
TARGET = mergesort_ff
SRC = main_ff.cpp ../sequential/record.cpp

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRC)
clean:
	rm -f $(TARGET) ~/mergesort_project/data/*.bin ~/mergesort_project/data/temp/*.bin
