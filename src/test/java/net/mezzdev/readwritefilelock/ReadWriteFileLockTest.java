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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
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
        Path normalized = tempDir.resolve("cache.lock");
        Path withRedundantSegments = tempDir.resolve("nested").resolve("..").resolve("cache.lock");

        assertSame(ReadWriteFileLock.forFile(normalized), ReadWriteFileLock.forFile(withRedundantSegments));
    }

    @Test
    void forFileReusesLockInstanceForSymlinkedParentWhenSupported() throws Exception {
        Path realDirectory = Files.createDirectory(tempDir.resolve("real"));
        Path symlinkDirectory = tempDir.resolve("link");
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
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(tempDir.resolve("read.lock"));

        try (
                ReadWriteFileLock.HeldLock first = lock.lockForRead();
                ReadWriteFileLock.HeldLock second = lock.lockForRead();
                ReadWriteFileLock.HeldLock third = lock.tryLockForRead()
        ) {
            assertNotNull(third);
            assertNull(lock.tryLockForWrite());
        }

        try (ReadWriteFileLock.HeldLock write = lock.tryLockForWrite()) {
            assertNotNull(write);
        }
    }

    @Test
    void allowsReentrantWriteLocksInSameThread() throws Exception {
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(tempDir.resolve("reentrant-write.lock"));

        try (
                ReadWriteFileLock.HeldLock first = lock.lockForWrite();
                ReadWriteFileLock.HeldLock second = lock.lockForWrite()
        ) {
            try (ReadWriteFileLock.HeldLock read = lock.tryLockForRead()) {
                assertNotNull(read);
            }
        }

        try (ReadWriteFileLock.HeldLock write = lock.tryLockForWrite()) {
            assertNotNull(write);
        }
    }

    @Test
    void allowsReadLockWhileCurrentThreadHoldsWriteLock() throws Exception {
        Path lockFile = tempDir.resolve("write-then-read.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        ReadWriteFileLock.HeldLock read;
        try (ReadWriteFileLock.HeldLock write = lock.lockForWrite()) {
            read = lock.lockForRead();
        }

        try (ReadWriteFileLock.HeldLock ignored = read) {
            assertEquals(BUSY, ProcessLockClient.run(TRY_WRITE, lockFile));
        }
        assertEquals(LOCKED, ProcessLockClient.run(TRY_WRITE, lockFile));
    }

    @Test
    void lockForWriteFailsFastWhenCurrentThreadHoldsReadLock() throws Exception {
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(tempDir.resolve("upgrade.lock"));

        try (ReadWriteFileLock.HeldLock ignored = lock.lockForRead()) {
            assertThrows(IllegalStateException.class, lock::lockForWrite);
            assertNull(lock.tryLockForWrite());
        }
    }

    @Test
    void tryLockForReadReturnsNullWhenWriteLockIsHeldByAnotherThread() throws Exception {
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(tempDir.resolve("try-read.lock"));
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try (ReadWriteFileLock.HeldLock ignored = lock.lockForWrite()) {
            Future<ReadWriteFileLock.HeldLock> result = executor.submit(() -> lock.tryLockForRead());

            assertNull(result.get(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void readLockWaitsForWriteLock() throws Exception {
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(tempDir.resolve("waiting.lock"));
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            CountDownLatch taskStarted = new CountDownLatch(1);
            CountDownLatch readAcquired = new CountDownLatch(1);
            CountDownLatch releaseRead = new CountDownLatch(1);
            Future<?> readFuture;

            try (ReadWriteFileLock.HeldLock write = lock.lockForWrite()) {
                readFuture = executor.submit(() -> {
                    taskStarted.countDown();
                    try (ReadWriteFileLock.HeldLock ignored = lock.lockForRead()) {
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
        Path lockFile = tempDir.resolve("try-read-os.lock");
        Files.createFile(lockFile);
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        try (
                FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
                java.nio.channels.FileLock ignored = channel.lock(0L, Long.MAX_VALUE, false)
        ) {
            assertNull(lock.tryLockForRead());
        }

        try (ReadWriteFileLock.HeldLock read = lock.tryLockForRead()) {
            assertNotNull(read);
        }
    }

    @Test
    void writeLockHoldsOperatingSystemFileLock() throws Exception {
        Path lockFile = tempDir.resolve("operating-system.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        try (
                ReadWriteFileLock.HeldLock ignored = lock.lockForWrite();
                FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.READ, StandardOpenOption.WRITE)
        ) {
            assertThrows(
                    OverlappingFileLockException.class,
                    () -> channel.tryLock(0L, Long.MAX_VALUE, false)
            );
        }
    }

    @Test
    void readLockHoldsOperatingSystemFileLockUntilLastLocalReadLockCloses() throws Exception {
        Path lockFile = tempDir.resolve("last-read.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        try (ReadWriteFileLock.HeldLock second = lock.lockForRead()) {
            try (ReadWriteFileLock.HeldLock first = lock.lockForRead()) {
                assertNotNull(first);
            }
            assertEquals(BUSY, ProcessLockClient.run(TRY_WRITE, lockFile));
        }
        assertEquals(LOCKED, ProcessLockClient.run(TRY_WRITE, lockFile));
    }

    @Test
    void readLocksCoordinateAcrossProcesses() throws Exception {
        Path lockFile = tempDir.resolve("process-read.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        try (ReadWriteFileLock.HeldLock ignored = lock.lockForRead()) {
            assertEquals(LOCKED, ProcessLockClient.run(TRY_READ, lockFile));
            assertEquals(BUSY, ProcessLockClient.run(TRY_WRITE, lockFile));
        }

        assertEquals(LOCKED, ProcessLockClient.run(TRY_WRITE, lockFile));
    }

    @Test
    void writeLocksCoordinateAcrossProcesses() throws Exception {
        Path lockFile = tempDir.resolve("process-write.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        try (ReadWriteFileLock.HeldLock ignored = lock.lockForWrite()) {
            assertEquals(BUSY, ProcessLockClient.run(TRY_READ, lockFile));
            assertEquals(BUSY, ProcessLockClient.run(TRY_WRITE, lockFile));
        }

        assertEquals(LOCKED, ProcessLockClient.run(TRY_READ, lockFile));
        assertEquals(LOCKED, ProcessLockClient.run(TRY_WRITE, lockFile));
    }

    @Test
    void closeFromDifferentThreadDoesNotReleaseReadLock() throws Exception {
        Path lockFile = tempDir.resolve("wrong-thread-read.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        try (ReadWriteFileLock.HeldLock read = lock.lockForRead()) {
            assertCloseFromDifferentThreadFails(read);
            assertEquals(BUSY, ProcessLockClient.run(TRY_WRITE, lockFile));
        }
        try (ReadWriteFileLock.HeldLock write = lock.tryLockForWrite()) {
            assertNotNull(write);
        }
    }

    @Test
    void closeFromDifferentThreadDoesNotReleaseWriteLock() throws Exception {
        Path lockFile = tempDir.resolve("wrong-thread-write.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        try (ReadWriteFileLock.HeldLock write = lock.lockForWrite()) {
            assertCloseFromDifferentThreadFails(write);
            assertEquals(BUSY, ProcessLockClient.run(TRY_READ, lockFile));
        }
        try (ReadWriteFileLock.HeldLock read = lock.tryLockForRead()) {
            assertNotNull(read);
        }
    }

    @Test
    void createsParentDirectoriesAndLockFile() throws Exception {
        Path lockFile = tempDir.resolve("locks").resolve("nested.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        try (ReadWriteFileLock.HeldLock ignored = lock.lockForWrite()) {
            assertTrue(Files.exists(lockFile));
        }
    }

    @Test
    void lockCloseIsIdempotent() throws Exception {
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(tempDir.resolve("idempotent.lock"));

        ReadWriteFileLock.HeldLock held = lock.lockForWrite();
        held.close();
        held.close();

        try (ReadWriteFileLock.HeldLock ignored = lock.lockForWrite()) {
            assertNotNull(ignored);
        }
    }

    private static void assertCloseFromDifferentThreadFails(ReadWriteFileLock.HeldLock lock) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> result = executor.submit(() -> {
                lock.close();
                return null;
            });
            ExecutionException exception = assertThrows(ExecutionException.class, () -> result.get(1, TimeUnit.SECONDS));
            assertTrue(exception.getCause() instanceof IllegalMonitorStateException);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

}
