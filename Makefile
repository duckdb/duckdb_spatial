GENERATOR=
ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
endif
ifeq (${OSX_BUILD_UNIVERSAL}, 1)
	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_UNIVERSAL=1
endif
debug:
	mkdir -p build/debug && \
	cd build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) -DCMAKE_BUILD_TYPE=Debug ../../CMakeLists.txt -B. && \
	cmake --build . --config Debug

release:
	mkdir -p build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) -DCMAKE_BUILD_TYPE=RelWithDebInfo ../../CMakeLists.txt -B. && \
	cmake --build . --config Release

test: debug
	./build/debug/base/Build/DuckDB/test/unittest --test-dir ./geo "[sql]"

test_debug: debug
	lldb ./build/debug/base/Build/DuckDB/test/unittest -- --test-dir ./geo "[sql]"

test_release: release
	./build/release/base/Build/DuckDB/test/unittest --test-dir ./geo "[sql]"

recursive_wildcard=$(foreach d,$(wildcard $(1:=/*)),$(call recursive_wildcard,$d,$2) $(filter $(subst *,%,$2),$d))
format:
	clang-format --sort-includes=0 -style=file -i $(call recursive_wildcard,geo/src,*.cpp)
	clang-format --sort-includes=0 -style=file -i $(call recursive_wildcard,geo/include,*.hpp)
	cmake-format -i geo/CMakeLists.txt

clean:
	rm -rf build
