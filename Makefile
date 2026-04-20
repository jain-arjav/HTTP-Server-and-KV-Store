CXX = g++
CXXFLAGS = -std=c++17 -Wall -g -I src
SRC = src/main.cpp
OUT = build/main

all: $(OUT)

$(OUT): $(SRC)
	@mkdir -p build
	$(CXX) $(CXXFLAGS) -o $(OUT) $(SRC)

run: $(OUT)
	./$(OUT)

clean:
	rm -rf build

.PHONY: all run clean
