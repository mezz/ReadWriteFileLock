package net.mezzdev.readwritefilelock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static net.mezzdev.readwritefilelock.ProcessLockClient.Command.TRY_READ;
import static net.mezzdev.readwritefilelock.ProcessLockClient.Command.TRY_WRITE;
import static net.mezzdev.readwritefilelock.ProcessLockClient.Result.BUSY;
import static net.mezzdev.readwritefilelock.ProcessLockClient.Result.LOCKED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.abort;

class ReadWriteFileLockTest {
    @TempDir
    Path tempDir;

    @Test
    void forFileReusesNormalizedLockInstance() {
        var normalized = tempDir.resolve("cache.lock");
        var withRedundantSegments = tempDir.resolve("nested").resolve("..").resolve("cache.lock");

        assertSame(ReadWriteFileLock.forFile(normalized), ReadWriteFileLock.forFile(withRedundantSegments));
    }

    @Test
    void forFileReusesLockInstanceForSymlinkedParentWhenSupported() throws Exception {
        var realDirectory = Files.createDirectory(tempDir.resolve("real"));
        var symlinkDirectory = tempDir.resolve("link");
        try {
            Files.createSymbolicLink(symlinkDirectory, realDirectory);
        } catch (UnsupportedOperationException | IOException | SecurityException e) {
            abort("Symbolic links are not available in this test environment.");
        }

        assertSame(
                ReadWriteFileLock.forFile(realDirectory.resolve("cache.lock")),
                ReadWriteFileLock.forFile(symlinkDirectory.resolve("cache.lock"))
        );
    }

    @Test
    void allowsMultipleReadLocksInSameJvm() throws Exception {
        var lock = ReadWriteFileLock.forFile(tempDir.resolve("read.lock"));

        try (
                var first = lock.lockForRead();
                var second = lock.lockForRead();
                var third = lock.tryLockForRead()
        ) {
            assertNotNull(third);
            assertNull(lock.tryLockForWrite());
        }

        try (var write = lock.tryLockForWrite()) {
            assertNotNull(write);
        }
    }

    @Test
    void allowsReentrantWriteLocksInSameThread() throws Exception {
        var lock = ReadWriteFileLock.forFile(tempDir.resolve("reentrant-write.lock"));

        try (
                var first = lock.lockForWrite();
                var second = lock.lockForWrite()
        ) {
            try (var read = lock.tryLockForRead()) {
                assertNotNull(read);
            }
        }

        try (var write = lock.tryLockForWrite()) {
            assertNotNull(write);
        }
    }

    @Test
    void allowsReadLockWhileCurrentThreadHoldsWriteLock() throws Exception {
        var lockFile = tempDir.resolve("write-then-read.lock");
        var lock = ReadWriteFileLock.forFile(lockFile);

        ReadWriteFileLock.HeldLock read;
        try (var write = lock.lockForWrite()) {
            read = lock.lockForRead();
        }

        try (read) {
            assertEquals(BUSY, ProcessLockClient.run(TRY_WRITE, lockFile));
        }
        assertEquals(LOCKED, ProcessLockClient.run(TRY_WRITE, lockFile));
    }

    @Test
    void lockForWriteFailsFastWhenCurrentThreadHoldsReadLock() throws Exception {
        var lock = ReadWriteFileLock.forFile(tempDir.resolve("upgrade.lock"));

        try (var ignored = lock.lockForRead()) {
            assertThrows(IllegalStateException.class, lock::lockForWrite);
            assertNull(lock.tryLockForWrite());
        }
    }

    @Test
    void tryLockForReadReturnsNullWhenWriteLockIsHeldByAnotherThread() throws Exception {
        var lock = ReadWriteFileLock.forFile(tempDir.resolve("try-read.lock"));
        var executor = Executors.newSingleThreadExecutor();

        try (var ignored = lock.lockForWrite()) {
            var result = executor.submit(lock::tryLockForRead);

            assertNull(result.get(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void readLockWaitsForWriteLock() throws Exception {
        var lock = ReadWriteFileLock.forFile(tempDir.resolve("waiting.lock"));
        var executor = Executors.newSingleThreadExecutor();

        try {
            var taskStarted = new CountDownLatch(1);
            var readAcquired = new CountDownLatch(1);
            var releaseRead = new CountDownLatch(1);
            Future<?> readFuture;

            try (var write = lock.lockForWrite()) {
                readFuture = executor.submit(() -> {
                    taskStarted.countDown();
                    try (var ignored = lock.lockForRead()) {
                        readAcquired.countDown();
                        if (!releaseRead.await(1, TimeUnit.SECONDS)) {
                            throw new TimeoutException("Timed out waiting to release read lock.");
                        }
                    }
                    return null;
                });

                assertTrue(taskStarted.await(1, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> readFuture.get(100, TimeUnit.MILLISECONDS));
            }

            assertTrue(readAcquired.await(1, TimeUnit.SECONDS));
            releaseRead.countDown();
            readFuture.get(1, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void tryLockForReadReturnsNullWhenOperatingSystemFileLockIsHeldForWriting() throws Exception {
        var lockFile = tempDir.resolve("try-read-os.lock");
        Files.createFile(lockFile);
        var lock = ReadWriteFileLock.forFile(lockFile);

        try (
                var channel = FileChannel.open(lockFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
                var ignored = channel.lock(0L, Long.MAX_VALUE, false)
        ) {
            assertNull(lock.tryLockForRead());
        }

        try (var read = lock.tryLockForRead()) {
            assertNotNull(read);
        }
    }

    @Test
    void writeLockHoldsOperatingSystemFileLock() throws Exception {
        var lockFile = tempDir.resolve("operating-system.lock");
        var lock = ReadWriteFileLock.forFile(lockFile);

        try (
                var ignored = lock.lockForWrite();
                var channel = FileChannel.open(lockFile, StandardOpenOption.READ, StandardOpenOption.WRITE)
        ) {
            assertThrows(
                    OverlappingFileLockException.class,
                    () -> channel.tryLock(0L, Long.MAX_VALUE, false)
            );
        }
    }

    @Test
    void readLockHoldsOperatingSystemFileLockUntilLastLocalReadLockCloses() throws Exception {
        var lockFile = tempDir.resolve("last-read.lock");
        var lock = ReadWriteFileLock.forFile(lockFile);

        try (var second = lock.lockForRead()) {
            try (var first = lock.lockForRead()) {
                assertNotNull(first);
            }
            assertEquals(BUSY, ProcessLockClient.run(TRY_WRITE, lockFile));
        }
        assertEquals(LOCKED, ProcessLockClient.run(TRY_WRITE, lockFile));
    }

    @Test
    void readLocksCoordinateAcrossProcesses() throws Exception {
        var lockFile = tempDir.resolve("process-read.lock");
        var lock = ReadWriteFileLock.forFile(lockFile);

        try (var ignored = lock.lockForRead()) {
            assertEquals(LOCKED, ProcessLockClient.run(TRY_READ, lockFile));
            assertEquals(BUSY, ProcessLockClient.run(TRY_WRITE, lockFile));
        }

        assertEquals(LOCKED, ProcessLockClient.run(TRY_WRITE, lockFile));
    }

    @Test
    void writeLocksCoordinateAcrossProcesses() throws Exception {
        var lockFile = tempDir.resolve("process-write.lock");
        var lock = ReadWriteFileLock.forFile(lockFile);

        try (var ignored = lock.lockForWrite()) {
            assertEquals(BUSY, ProcessLockClient.run(TRY_READ, lockFile));
            assertEquals(BUSY, ProcessLockClient.run(TRY_WRITE, lockFile));
        }

        assertEquals(LOCKED, ProcessLockClient.run(TRY_READ, lockFile));
        assertEquals(LOCKED, ProcessLockClient.run(TRY_WRITE, lockFile));
    }

    @Test
    void closeFromDifferentThreadDoesNotReleaseReadLock() throws Exception {
        var lockFile = tempDir.resolve("wrong-thread-read.lock");
        var lock = ReadWriteFileLock.forFile(lockFile);

        try (var read = lock.lockForRead()) {
            assertCloseFromDifferentThreadFails(read);
            assertEquals(BUSY, ProcessLockClient.run(TRY_WRITE, lockFile));
        }
        try (var write = lock.tryLockForWrite()) {
            assertNotNull(write);
        }
    }

    @Test
    void closeFromDifferentThreadDoesNotReleaseWriteLock() throws Exception {
        var lockFile = tempDir.resolve("wrong-thread-write.lock");
        var lock = ReadWriteFileLock.forFile(lockFile);

        try (var write = lock.lockForWrite()) {
            assertCloseFromDifferentThreadFails(write);
            assertEquals(BUSY, ProcessLockClient.run(TRY_READ, lockFile));
        }
        try (var read = lock.tryLockForRead()) {
            assertNotNull(read);
        }
    }

    @Test
    void createsParentDirectoriesAndLockFile() throws Exception {
        var lockFile = tempDir.resolve("locks").resolve("nested.lock");
        var lock = ReadWriteFileLock.forFile(lockFile);

        try (var ignored = lock.lockForWrite()) {
            assertTrue(Files.exists(lockFile));
        }
    }

    @Test
    void lockCloseIsIdempotent() throws Exception {
        var lock = ReadWriteFileLock.forFile(tempDir.resolve("idempotent.lock"));

        var held = lock.lockForWrite();
        held.close();
        held.close();

        try (var ignored = lock.lockForWrite()) {
            assertNotNull(ignored);
        }
    }

    private static void assertCloseFromDifferentThreadFails(ReadWriteFileLock.HeldLock lock) throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try {
            var result = executor.submit(() -> {
                lock.close();
                return null;
            });
            var exception = assertThrows(java.util.concurrent.ExecutionException.class, () -> result.get(1, TimeUnit.SECONDS));
            assertTrue(exception.getCause() instanceof IllegalMonitorStateException);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

}
