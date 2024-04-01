# DuckDB Spatial Extension

🚧 WORK IN PROGRESS 🚧

**Table of contents**
- [DuckDB Spatial Extension](#duckdb-spatial-extension)
- [What is this?](#what-is-this)
- [How do I get it?](#how-do-i-get-it)
  - [Through the DuckDB CLI](#through-the-duckdb-cli)
  - [Development builds](#development-builds)
  - [Building from source](#building-from-source)
- [Example Usage](#example-usage)
- [Supported Functions](#supported-functions-and-documentation)
- [Internals and Technical Details](#internals-and-technical-details)
- [Limitations and Roadmap](#limitations-and-roadmap)

# What is this?
This is a prototype of a geospatial extension for DuckDB that adds support for working with spatial data and functions in the form of a `GEOMETRY` type based on the the "Simple Features" geometry model, as well as non-standard specialized columnar DuckDB native geometry types that provide better compression and faster execution in exchange for flexibility.

Please note that this extension is still in a very early stage of development, and the internal storage format for the geometry types may change indiscriminately between commits. We are actively working on it, and we welcome both contributions and feedback. Please see the [function table](docs/functions.md) or the [roadmap entries](https://github.com/duckdblabs/duckdb_spatial/labels/roadmap) for the current implementation status.

If you or your organization have any interest in sponsoring development of this extension, or have any particular use cases you'd like to see prioritized or supported, please consider [sponsoring the DuckDB foundation](https://duckdb.org/foundation/) or [contacting DuckDB Labs](https://duckdblabs.com) for commercial support.

# How do I get it?

## Through the DuckDB CLI
You can install the extension for DuckDB through the DuckDB CLI like you would do for other first party extensions. Simply execute: ```INSTALL spatial; LOAD spatial```!

## Development builds
You can also grab the lastest builds directly from the CI runs or the release page here on GitHub and install manually.

Once you have downloaded the extension for your platform, you need to:
- Unzip the archive
- Start duckdb with the `-unsigned` flag to allow loading unsigned extensions. (This won't be neccessary in the future)
- Run `INSTALL 'absolute/or/relative/path/to/the/unzipped/extension';`
- The extension is now installed, you can now load it with `LOAD spatial;` whenever you want to use it.

You can also build the extension yourself following the instructions below.

## Building from source
This extension is based on the [DuckDB extension template](https://github.com/duckdb/extension-template).

**Dependencies**

You need a recent version of CMake (3.20) and a C++11 compatible compiler.
You also need OpenSSL on your system. On ubuntu you can install it with `sudo apt install libssl-dev`, on macOS you can install it with `brew install openssl`. Note that brew installs openssl in a non-standard location, so you may need to set a `OPENSSL_ROOT_DIR=$(brew --prefix openssl)` environment variable when building.

We bundle all the other required dependencies in the `third_party` directory, which should be automatically built and statically linked into the extension. This may take some time the first time you build, but subsequent builds should be much faster.

We also highly recommend that you install [Ninja](https://ninja-build.org) which you can select when building by setting the `GEN=ninja` environment variable.

```
git clone --recurse-submodules https://github.com/duckdblabs/duckdb_spatial
cd duckdb_spatial
make debug
```
You can then invoke the built DuckDB (with the extension statically linked)
```
./build/debug/duckdb
```

Please see the Makefile for more options, or the extension template documentation for more details.

# Example Usage

Please see the [example](docs/example.md) for an example on how to use the extension.

# Supported Functions and Documentation

The full list of functions and their documentation is available in the [function reference](docs/functions.md)

# Limitations and Roadmap

The main limitations of this extension currently are:
- No support for spherical geometry (e.g. lat/lon coordinates)
- No support for spatial indexing

These are all things that we want to address eventually, have a look at the open issues and [roadmap entries](https://github.com/duckdblabs/duckdb_spatial/labels/roadmap) for more details. Please feel free to also open an issue if you have a specific use case that you would like to see supported.

# Internals and Technical Details

Please see the [internals documentation](docs/internals.md) for more details on the internal workings of the extension.
