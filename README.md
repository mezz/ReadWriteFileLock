# ReadWriteFileLock

A Java read/write lock backed by a file lock, so it works across processes.

The lock has two layers:

- a JVM-local fair read/write lock to coordinate threads in the current process;
- an operating-system file lock to coordinate separate processes.

Java does not allow overlapping file locks on the same file in one JVM process.
To support multiple readers in one process, `ReadWriteFileLock` lets readers
share one process file lock and keeps a holder count for it.

## Usage

```java
import net.mezzdev.readwritefilelock.ReadWriteFileLock;

import java.nio.file.Path;

ReadWriteFileLock lock = ReadWriteFileLock.forFile(Path.of("cache-use.lock"));

try (ReadWriteFileLock.HeldLock ignored = lock.lockForRead()) {
    // Many threads and processes may hold a read lock at once.
}

ReadWriteFileLock.HeldLock readLock = lock.tryLockForRead();
if (readLock != null) {
    try (readLock) {
        // Read work that should be skipped if the lock is already held for writing.
    }
}

try (ReadWriteFileLock.HeldLock ignored = lock.lockForWrite()) {
    // Only this thread/process may hold the lock.
}

ReadWriteFileLock.HeldLock writeLock = lock.tryLockForWrite();
if (writeLock != null) {
    try (writeLock) {
        // Write work that should be skipped if the lock is already held.
    }
}
```

The lock file may remain on disk after use. The active lock is tied to the
process's open file handle, not to the presence of the file. If the process
dies, the operating system closes the handle and releases the lock.

A held lock must be closed by the same thread that acquired it. Closing a lock
more than once from the owning thread is allowed.

## Installation

ReadWriteFileLock is published to Maven Central:

```xml
<dependency>
    <groupId>net.mezzdev</groupId>
    <artifactId>readwritefilelock</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Requirements

- Java 17 or newer

## Development

Run the test suite with:

```sh
./mvnw verify
```
