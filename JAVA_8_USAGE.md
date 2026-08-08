# Usage with Java 8

`ReadWriteFileLock` supports Java 8, but the main README uses a few newer Java
source features for readability. In Java 8 code, use explicit local variable
types and declare resources inside each try-with-resources statement.

```java
import net.mezzdev.readwritefilelock.ReadWriteFileLock;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

Path lockFile = Paths.get("cache-use.lock");
ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

try (ReadWriteFileLock.HeldLock readLock = lock.lockForRead()) {
    // Many threads and processes may hold a read lock at once.
}

ReadWriteFileLock.HeldLock readLock = lock.tryLockForRead();
if (readLock != null) {
    try (ReadWriteFileLock.HeldLock ignored = readLock) {
        // Read work that should be skipped if the lock is already held for writing.
    }
}

try (ReadWriteFileLock.HeldLock writeLock = lock.lockForWrite()) {
    // Only this thread/process may hold the lock.
}

ReadWriteFileLock.HeldLock writeLock = lock.tryLockForWrite();
if (writeLock != null) {
    try (ReadWriteFileLock.HeldLock ignored = writeLock) {
        // Write work that should be skipped if the lock is already held.
    }
}

ReadWriteFileLock.HeldLock timedWriteLock = lock.tryLockForWrite(1, TimeUnit.MINUTES);
if (timedWriteLock != null) {
    try (ReadWriteFileLock.HeldLock ignored = timedWriteLock) {
        // Write work after waiting up to one minute for the lock.
    }
}
```

The API is the same as in newer Java versions. The Java 8-specific differences
are only source syntax:

- use `Paths.get(...)` instead of `Path.of(...)`;
- use explicit local variable types instead of `var`;
- use `try (ReadWriteFileLock.HeldLock ignored = readLock)` instead of
  `try (readLock)`.
