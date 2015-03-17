# FoundationFS
File Storage system for FoundationDB written in go
Includes an example http server interface with support for basic upload and download of any file type.





## Installation

```bash
go get github.com/cloudmode/FoundationFS
```

## Dependencies

FoundationFS requires:

Go 1.1+ with CGO enabled
FoundationDB C API 2.0.x or 3.0.x (part of the FoundationDB clients package)
Use of this package requires the selection of a FoundationDB API version at runtime. This package currently supports FoundationDB API versions 200 and 300 (although version 300 requires a 3.0.x FoundationDB C library to be installed).

To install the fdb-go package interface (https://github.com/FoundationDB/fdb-go):

```bash
go get github.com/FoundationDB/fdb-go/fdb
```

FoundationFS uses go/codec (using the msgpack codec) for storing values in FDB
such as the file meta data. (https://github.com/ugorji/go)

TO install ghe go/codec package:

```bash
go get github.com/ugorji/go/codec
```

Other dependencies:

```bash
go get github.com/twinj/uuid
```

## Example

To run the example:

```bash
cd examples
go run simple.go
```

The server will be listening on localhost:9090. Direct your browser to localhost:9090/upload, upload
a file and json with the id of the uploaded file returned. Copy and paste the id of the uploaded file
into http://localhost:9090/download?id=<the id returned> and the file will be displayed in your browser

To test with curl:

```bash
curl -i -X POST -H "Content-Type: multipart/form-data" -F "uploadfile=@test.png" http://localhost:9090/upload
curl -o foo.png http://localhost:9090/download?id=9ccc7915307b4351a0de9219d120fc3e

```

## Test Suite

The test suite uses the standard go test runner along with convey, download here.

```bash
go get github.com/smartystreets/goconvey/convey
cd test
go test
```
The test script expects there to be at least one file in the test/data subdirectory which is used
for uploading and downloading tests. It will fail if there are no files in data. 

## License

The MIT License (MIT) - see LICENSE.md for more details