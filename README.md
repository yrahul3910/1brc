# 1BRC

## Debug setup

You'll need the following:

* Zig 0.15.2
* Tracy 0.12. Clone [the repo](https://github.com/wolfpld/tracy/), then
```sh
git checkout 87924ac
cmake -B profiler/build -S profiler -DCMAKE_BUILD_TYPE=Release
cmake --build profiler/build --config Release --parallel
./profiler/build/tracy-profiler
```
which is the commit for 0.12.
* The `zig-tracy` repo: [https://github.com/tealsnow/zig-tracy/](https://github.com/tealsnow/zig-tracy/).

Then:
```sh
zig build
./zig-out/bin/_1brc
```

## Release

Tracy is disabled in release mode, so you just need Zig 0.15.2 (probably). You then build with:
```sh
zig build -Doptimize=ReleaseFast
```

