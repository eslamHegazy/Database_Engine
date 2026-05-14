# kalabalaDB

A lightweight Java database engine simulation that demonstrates how a storage manager, query execution layer, and indexing structures work together to process data.

## What this project does

- Implements a basic database engine with table creation, record insertion, deletion, update, and selection.
- Uses a clustering key to organize table rows and manage paged storage on disk.
- Supports B+ tree indexes for fast equality and range queries.
- Supports R-tree spatial indexing for polygon-shaped data.
- Demonstrates query execution through a simple `SQLTerm` API.

## Theory behind the design

This project models the core database engine concepts:

- **Storage management**: tables are split into pages and persisted to disk, with the engine managing page layout and record placement.
- **Clustering key**: one column is chosen as the table's clustering key, and rows are ordered by that key for efficient range access.
- **B+ tree indexing**: used for indexed columns to accelerate search and range operations while keeping sorted leaf pages.
- **R-tree indexing**: used for geometric data types such as `Polygon` to allow spatial queries.
- **Query execution**: conditions are expressed as `SQLTerm` objects and combined with boolean operators to filter rows using available indexes when possible.

## How to run

1. Open a terminal in the `Database_Engine` folder.
2. Build the project:

   ```bash
   make all
   ```

3. Run the main test harness:

   ```bash
   make run-DBApp
   ```

If `make` is not available, compile and run with Java directly:

```bash
javac src/kalabalaDB/*.java libs/BPTree/*.java libs/General/*.java libs/RTree/*.java -d classes/
java -classpath classes/ kalabalaDB.DBAppTest
```

## How to test

- The `DBAppTest` class is the entry point used by the build script and contains example database initialization and query code.
- Running `make run-DBApp` executes that test harness.
- Use `make clean` to remove generated class files and temporary data:

  ```bash
  make clean
  ```

## Project structure

- `src/kalabalaDB`: core engine code and test harness.
- `libs/BPTree`: B+ tree implementation.
- `libs/RTree`: R-tree spatial index implementation.
- `classes`: compiled bytecode output.
- `Makefile`: build and run commands.

## Notes

- This repository is intended as an educational engine prototype, not a production database.
- It focuses on storage and indexing algorithms rather than a full SQL parser or transaction system.
