# [HADOOP-18287](https://issues.apache.org/jira/browse/HADOOP-18103). Provide a shim library for modern FS APIs: `hadoop-api-shim`

A shim library for allowing hadoop applications to call the recent higher-performance/cloud-aware
filesystem API methods when available, but still compile against and link to older hadoop releases.

This allows them to call operations which are potentially significantly
higher performance, especially when working with object stores.

For example, the `openFile()` builder API allows a library to declare
the seek policy to use (`sequential`, `random`, etc), which is
used to determine what kind of requests to make of the store.
If a `FileStatus` is passed in, any initial `HEAD` request to determine the file size
can be skipped. This will reduce latency and significantly
improve read performance against s3a and abfs, both of which
implement that feature. The GCS connector would also benefit
from the API changes if/when it adds its own suppoort. 

Another example is the high performance Vector IO API
of [HADOOP-18103](https://issues.apache.org/jira/browse/HADOOP-18103).
On hadoop releases with this API, it will parallelize a list
of read operations.
The base implementation converts these to seek/read calls,
but object stores may implement the reads as a set of
parallel ranged GET requests (as does s3a).
File formats which read stripes of data can use this API
call to gain significant performance improvements.

The library contains a number of shim classes, which can
be instantiated with a supplied instance of a class available
in the Hadoop 3.2.0 APIs.

These classes use reflection to bind to methods not available in the
hadoop 3.2.0 release.

These API calls can be probed for before invocation.
When unavailable, the operations will either fall back to a default
implementation (example: `openFile()`), or raise
an `UnsupportedOperationException`

Note that the presence of the methods does not guarantee that the
invoked instance will succeed, rather that the outcome is the same
as it would be if the method was called directly.

### Why 3.2.0?

1. Hadoop branch-3.2 is the oldest hadoop-3.x branch which has active releases for critical
bugs. It is the de-facto baseline API for all Hadoop runtimes/platforms.
2. It is the oldest Hadoop branch-3 branch getting security fixes: users of older releases MUST upgrade or keep their own version current.
3. It is the first version whose client libraries work with Java 11.
4. It is impossible to assert Hadoop 2 compatibility without actually building and testing on java7. Which is not easy to do any more.

Libraries which want to use these new APIs must build with a hadoop version of 3.2.0
*or later*.


## Shimmed Classes and their API extensions.



## `org.apache.hadoop.fs.shim.api.FSDataInputStreamShim`

| Method                  | Version     | JIRA                                                               | Fallback                   |
|-------------------------|-------------|--------------------------------------------------------------------|----------------------------|
| `openFile()`            | 3.3.0       | [HADOOP-15229](https://issues.apache.org/jira/browse/HADOOP-15229) | `open(Path)`               |
| `msync()`               | 3.3.2       | [HDFS-15567](https://issues.apache.org/jira/browse/HDFS-15567)     | noop                       |
| `hasPathCapability()`   | 3.3.0       | [HADOOP-15691](https://issues.apache.org/jira/browse/HADOOP-15691) | `false`                    |


## `org.apache.hadoop.fs.shim.api.FSDataInputStreamShim`

| Method                     | Version | JIRA                                                               | Fallback  |
|----------------------------|---------|--------------------------------------------------------------------|-----------|
| `ByteBufferPositionedRead` | 3.3.0   | [HDFS-3246](https://issues.apache.org/jira/browse/HDFS-3246])      | Emulation |
| Vectored IO                | 3.3.5   | [HADOOP-18103](https://issues.apache.org/jira/browse/HADOOP-18103) | Emulation |
|                            |         |                                                                    |           |

### [HADOOP-15229](https://issues.apache.org/jira/browse/HADOOP-15229) Add FileSystem builder-based openFile() API to match createFile() (since 3.3.0)

This allows for the operations defined in [HADOOP-16202](https://issues.apache.org/jira/browse/ADOOP-16202) to be used if
set in the builder in `.opt(key, value)` parameters.

### [HDFS-3246](https://issues.apache.org/jira/browse/HDFS-3246]) `ByteBufferPositionedRead` interface (since 3.3.0)

The `ByteBufferPositionedRead` interface allows an application to read/readFully into
a byte buffer from a specific offset.

If the `ByteBufferPositionedRead` interface is not implemented by the wrapped input stream,
the methods it exports, `FSDataInputStream.read(long position, ByteBuffer buf)` and
`FSDataInputStream.readFully(long position, ByteBuffer buf)` 
will both throw `UnsupportedOperationException`.

To verify that a stream implements the API all the way through to the filesystem, 
call `hasCapability("in:preadbytebuffer")`
on the stream.
All streams which implement `ByteBufferPositionedRead` MUST return `true` on this probe
a requirement which is upheld by all those implemented in the hadoop libraries themselves.

The shim library only considers the API available if the methods are found and the capability
probe is true. If, even then, an `UnsuportedOperationException` is thrown, the fallback routine
is used for that call and all subsequent calls.

### [HADOOP-18103](https://issues.apache.org/jira/browse/HADOOP-18103). Vector IO. Since 3.3.5

The Vector IO API of HADOOP-18103 offers a high performance scatter/gather API for accessing columnar data.
By giving the filesystem clients more information about the plan of future reads, they can optimize
their requests to the remote stores, such as combining ranges, issuing ranged GET request to cover those combined ranges, etc.
The S3A connector issues requests for different ranges in parallel, over different HTTP connections.

On Hadoop releases with the API, all input streams which support `seek()` support the API.
That means everything except the ftp and sftp connectors.

## Testing the library.


The `hadoop-api-shim` module's test JAR contains contract tests
(subclasses of `hadoop-common` test `AbstractFSContractTestBase`) for different shim classes.
the library module's implementations will verify that when executed on older versions they work/downgrade/fail as expected.
The XML contracts will declare what APIs are available for that store; separate files will be needed for each
release and (somehow) the appropriate version identified.

These test classes can be tested against different hadoop releases, by having a separate test
module for each targeted version. 

The package `org.apache.hadoop.fs.shim.test.binding` package contains feature classes for each
hadoop version 


For those `ByteBufferPositionedRead` a minihdfs cluster will be neede for the test suite,
or an input stream with methods of the same name and arguments added.
