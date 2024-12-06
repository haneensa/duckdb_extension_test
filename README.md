# Lineage Extension

This experimental repository that extends DuckDB with lineage capture.

---

This is a custom extension that propagate and manage lineage 

## Features
- Wrap each operator with lineage Propagation logic
- Manages lineage propagation internally and hides it from the user
- Expose in-memory lineage as relational tables

## Usage
```bash
LOAD 'lineage_extension.duckdb_extension';
CREATE TABLE test (id INTEGER);
INSERT INTO test VALUES (1), (2), (3);
SELECT * FROM test; // TODO: add a method to indicate we want lineage for this query
// TODO: add a method to query lineage metadata (queries run with lineage, their ids, etc.)
SELECT * FROM lineage("SELECT * FROM TEST"); // TODO: make it easier to specify which lineage data we want
```


## Building
### Managing dependencies

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/lineage_extension/lineage_extension.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `lineage_extension.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.


## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL lineage
LOAD lineage
```
