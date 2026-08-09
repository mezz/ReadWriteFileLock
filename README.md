# ReadWriteFileLock

A small Java 8+ read/write lock for coordinating access across threads,
isolated classloaders, and processes.

## Usage

The examples below use modern Java syntax. For Java 8 source-compatible
examples, see [Usage with Java 8](JAVA_8_USAGE.md).

```java
import net.mezzdev.readwritefilelock.ReadWriteFileLock;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

Path lockFile = Paths.get("cache-use.lock");
ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

try (var readLock = lock.lockForRead()) {
    // Many threads and processes may hold a read lock at once.
}

var readLock = lock.tryLockForRead();
if (readLock != null) {
    try (readLock) {
        // Read work that should be skipped if the lock is already held for writing.
    }
}

try (var writeLock = lock.lockForWrite()) {
    // Only this thread/process may hold the lock.
}

var writeLock = lock.tryLockForWrite();
if (writeLock != null) {
    try (writeLock) {
        // Write work that should be skipped if the lock is already held.
    }
}

var timedWriteLock = lock.tryLockForWrite(1, TimeUnit.MINUTES);
if (timedWriteLock != null) {
    try (timedWriteLock) {
        // Write work after waiting up to one minute for the lock.
    }
}
```

The lock file may remain on disk after use. The active lock is tied to the
process's open file handle, not to the presence of the file. If the process
dies, the operating system closes the handle and releases the lock.

A held lock must be closed by the same thread that acquired it. Closing a lock
more than once from the owning thread is allowed.

## Locking behavior

Each lock combines a fair, classloader-local read/write lock with an operating-
system file lock. Readers in one classloader share the same process file lock,
while the operating-system lock coordinates processes and isolated
classloaders.

Blocking and timed acquisition wait interruptibly on asynchronous file-lock
operations, so cross-process contention does not require polling. Same-JVM
file-lock overlap waits for a release notification, with a one-second fallback
before retrying. Timed acquisition applies one timeout across both locking
layers.

## Comparison with existing options

There are several open source approaches to file locking in Java. This project
is intentionally small: it only provides a direct read/write lock API for a
specific lock file.

### Project comparison:

| Project | Best fit | Pros | Cons |
| --- | --- | --- | --- |
| [ReadWriteFileLock] | Small libraries and tools that need a direct `Path`-based read/write lock. | Minimal API, Java 8+, no logging/container dependencies, same-classloader reader coordination, process and isolated-classloader coordination through `FileLock`. | Narrow scope; no lock upgrades, diagnostics, or lock-file cleanup policy. |
| [Java FileLock] | Low-level file locking when the caller manages all coordination. | Built into the JDK, supports both lock modes at the OS level. | File locks are VM-wide; Java does not allow overlapping locks on the same file in one JVM, so callers must add their own thread-level coordination. |
| [Maven Resolver Named Locks] | Maven/Resolver-style named resource locking. | Actively maintained, supports read/write-style named locks, includes file-lock and other providers. | The API is a named-lock abstraction rather than a direct read/write file-lock utility, and it brings Resolver-oriented concepts/dependencies. |
| [Takari FileLock] | Older Aether/Maven-style file locking. | Similar read/write idea, uses a sidecar lock file, handles same-JVM coordination. | Old project, older dependency style, and a less direct modern `Path`/try-with-resources API. |
| [Apache Archiva FileLock] | Archiva internals. | Published Apache component with read/write lock manager concepts. | Tied to Archiva-era infrastructure and dependencies; not designed as a small standalone utility. |
| [Gemma ReadWriteFileLock] | Gemma's internal file-lock manager. | Close in spirit, implements `ReadWriteLock`, includes timed lock behavior. | Embedded package-private application code, not a standalone Maven Central library. |

### Feature comparison:

| Project | Java support | Lock target | Read/write modes | Same-JVM coordination | Cross-process coordination | Non-blocking acquire | Timed acquire | Standalone library |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [ReadWriteFileLock] | Java 8+ | `Path` | Yes | Yes | Yes | Yes | Yes | Yes |
| [Java FileLock] | Java 1.4+ | `FileChannel` | Yes | No | Yes | Yes | No | JDK<sup><a href="#feature-note-jdk">1</a></sup> |
| [Maven Resolver Named Locks] | Java 8+ | Named URI | Yes | Yes | Provider-dependent<sup><a href="#feature-note-provider-dependent">2</a></sup> | Yes | Yes | Yes |
| [Takari FileLock] | Java 6+ | `File` | Yes | Yes | Yes | No | No | Yes |
| [Apache Archiva FileLock] | Java 8+ | `File` | Partial<sup><a href="#feature-note-archiva-modes">3</a></sup> | Partial<sup><a href="#feature-note-archiva-same-jvm">4</a></sup> | Optional | Partial<sup><a href="#feature-note-archiva-non-blocking">5</a></sup> | Yes | No |
| [Gemma ReadWriteFileLock] | Java 8+ | `Path` | Yes | Yes | Yes | Yes | Yes | No |

Notes:

1. <a id="feature-note-jdk"></a>Built into Java rather than published as a
   separate library.
2. <a id="feature-note-provider-dependent"></a>Maven Resolver needs an
   appropriate named lock provider, such as its file-lock provider.
3. <a id="feature-note-archiva-modes"></a>Archiva has read/write lock-manager
   concepts, but not a direct standalone read/write lock API.
4. <a id="feature-note-archiva-same-jvm"></a>Archiva's same-JVM coordination is
   tied to its lock-manager component model rather than a small per-lock utility.
5. <a id="feature-note-archiva-non-blocking"></a>Archiva's non-blocking behavior
   is available through its broader lock-manager API rather than a direct
   `tryLock...` method on a small lock type.

I recommend using Maven Resolver Named Locks if you want a broader named-lock abstraction or
multiple backends. Use this project when you want a small dependency with a direct API.

[ReadWriteFileLock]: https://github.com/mezz/ReadWriteFileLock
[Java FileLock]: https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/nio/channels/FileLock.html
[Maven Resolver Named Locks]: https://maven.apache.org/resolver/maven-resolver-named-locks/
[Takari FileLock]: https://central.sonatype.com/artifact/io.takari/takari-filelock
[Apache Archiva FileLock]: https://maven.apache.org/archiva/ref/2.2.10/apidocs/org/apache/archiva/common/filelock/package-summary.html
[Gemma ReadWriteFileLock]: https://github.com/PavlidisLab/Gemma/blob/main/gemma-core/src/main/java/ubic/gemma/core/util/locking/ReadWriteFileLock.java

## Installation

ReadWriteFileLock is published to Maven Central:

```xml
<dependency>
    <groupId>net.mezzdev</groupId>
    <artifactId>readwritefilelock</artifactId>
    <version>0.3.1</version>
</dependency>
```

## Requirements

- Java 8 or newer

The jar declares `Automatic-Module-Name: net.mezzdev.readwritefilelock` for JPMS
users without requiring a Java 9+ `module-info.java`.

## Development

Run the test suite with:

```sh
./mvnw verify
```
