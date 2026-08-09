package net.mezzdev.readwritefilelock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static net.mezzdev.readwritefilelock.ProcessLockClient.Command.HOLD_READ;
import static net.mezzdev.readwritefilelock.ProcessLockClient.Command.HOLD_WRITE;
import static net.mezzdev.readwritefilelock.ProcessLockClient.Command.TRY_READ;
import static net.mezzdev.readwritefilelock.ProcessLockClient.Command.TRY_WRITE;
import static net.mezzdev.readwritefilelock.ProcessLockClient.Result.BUSY;
import static net.mezzdev.readwritefilelock.ProcessLockClient.Result.LOCKED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
        // Setup: two paths identify the same file but one contains redundant path segments.
        Path normalized = tempDir.resolve("cache.lock");
        Path withRedundantSegments = tempDir.resolve("nested").resolve("..").resolve("cache.lock");

        // Operation: look up a lock through each spelling of the path.
        ReadWriteFileLock first = ReadWriteFileLock.forFile(normalized);
        ReadWriteFileLock second = ReadWriteFileLock.forFile(withRedundantSegments);

        // Assertions: normalized paths share one classloader-local lock instance.
        assertSame(first, second);
    }

    @Test
    void forFileReusesLockInstanceForSymlinkedParentWhenSupported() throws Exception {
        // Setup: a real directory and a symbolic link both lead to the same lock-file location.
        Path realDirectory = Files.createDirectory(tempDir.resolve("real"));
        Path symlinkDirectory = tempDir.resolve("link");
        try {
            Files.createSymbolicLink(symlinkDirectory, realDirectory);
        } catch (UnsupportedOperationException | IOException | SecurityException e) {
            abort("Symbolic links are not available in this test environment.");
        }

        // Operation: look up the lock through the real and symlinked parent directories.
        ReadWriteFileLock first = ReadWriteFileLock.forFile(realDirectory.resolve("cache.lock"));
        ReadWriteFileLock second = ReadWriteFileLock.forFile(symlinkDirectory.resolve("cache.lock"));

        // Assertions: real-path normalization maps both paths to the same lock instance.
        assertSame(first, second);
    }

    @Test
    void allowsMultipleReadLocksInSameJvm() throws Exception {
        // Setup: one lock will be acquired repeatedly for reading in this thread.
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(tempDir.resolve("read.lock"));

        // Operation: acquire blocking, immediate, and timed read locks together.
        try (
                ReadWriteFileLock.HeldLock first = lock.lockForRead();
                ReadWriteFileLock.HeldLock second = lock.lockForRead();
                ReadWriteFileLock.HeldLock third = lock.tryLockForRead();
                ReadWriteFileLock.HeldLock timed = lock.tryLockForRead(1L, TimeUnit.SECONDS)
        ) {
            // Assertions: all reads coexist while a write remains unavailable.
            assertNotNull(third);
            assertNotNull(timed);
            assertNull(lock.tryLockForWrite());
        }

        // Operation: request a write after every read lock has closed.
        try (ReadWriteFileLock.HeldLock write = lock.tryLockForWrite()) {
            // Assertions: the final read release makes the write lock available.
            assertNotNull(write);
        }
    }

    @Test
    void allowsReentrantWriteLocksInSameThread() throws Exception {
        // Setup: one thread owns the lock throughout all reentrant acquisitions.
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(tempDir.resolve("reentrant-write.lock"));

        // Operation: nest blocking, timed, read-under-write, and write-after-read acquisitions.
        try (
                ReadWriteFileLock.HeldLock first = lock.lockForWrite();
                ReadWriteFileLock.HeldLock second = lock.lockForWrite();
                ReadWriteFileLock.HeldLock timed = lock.tryLockForWrite(1L, TimeUnit.SECONDS)
        ) {
            // Assertions: timed write reentrancy succeeds.
            assertNotNull(timed);
            try (
                    ReadWriteFileLock.HeldLock blockingRead = lock.lockForRead();
                    ReadWriteFileLock.HeldLock immediateRead = lock.tryLockForRead();
                    ReadWriteFileLock.HeldLock timedRead = lock.tryLockForRead(1L, TimeUnit.SECONDS)
            ) {
                // Assertions: the write owner may use every read acquisition mode.
                assertNotNull(immediateRead);
                assertNotNull(timedRead);
                try (
                        ReadWriteFileLock.HeldLock blockingAfterRead = lock.lockForWrite();
                        ReadWriteFileLock.HeldLock immediateAfterRead = lock.tryLockForWrite();
                        ReadWriteFileLock.HeldLock timedAfterRead = lock.tryLockForWrite(1L, TimeUnit.SECONDS)
                ) {
                    // Assertions: read-under-write is not mistaken for an unsupported lock upgrade.
                    assertNotNull(immediateAfterRead);
                    assertNotNull(timedAfterRead);
                }
            }
        }

        // Operation: acquire once more after the nested locks close.
        try (ReadWriteFileLock.HeldLock write = lock.tryLockForWrite()) {
            // Assertions: balanced reentrant closes leave the lock reusable.
            assertNotNull(write);
        }
    }

    @Test
    void allowsReadLockWhileCurrentThreadHoldsWriteLock() throws Exception {
        // Setup: a write lock and process probe use the same lock file.
        Path lockFile = tempDir.resolve("write-then-read.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        // Operation: acquire a read lock while the current thread owns the write lock.
        ReadWriteFileLock.HeldLock read;
        try (ReadWriteFileLock.HeldLock write = lock.lockForWrite()) {
            read = lock.tryLockForRead(1L, TimeUnit.SECONDS);

            // Assertions: write ownership permits read reentrancy.
            assertNotNull(read);
        }

        // Operation: probe for a process write before and after the surviving read closes.
        try (ReadWriteFileLock.HeldLock ignored = read) {
            // Assertions: the read remains effective after the enclosing write closes.
            assertEquals(BUSY, ProcessLockClient.run(TRY_WRITE, lockFile));
        }
        assertEquals(LOCKED, ProcessLockClient.run(TRY_WRITE, lockFile));
    }

    @Test
    void lockForWriteFailsFastWhenCurrentThreadHoldsReadLock() throws Exception {
        // Setup: the current thread holds only a read lock.
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(tempDir.resolve("upgrade.lock"));

        // Operation: request blocking, immediate, and timed write upgrades.
        try (ReadWriteFileLock.HeldLock ignored = lock.lockForRead()) {
            // Assertions: blocking rejects the upgrade and try methods report it as unavailable.
            assertThrows(IllegalStateException.class, lock::lockForWrite);
            assertNull(lock.tryLockForWrite());
            assertNull(lock.tryLockForWrite(1L, TimeUnit.SECONDS));
        }
    }

    @Test
    void tryLockForReadReturnsNullWhenWriteLockIsHeldByAnotherThread() throws Exception {
        // Setup: the main thread holds a write lock and another thread will try to read.
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(tempDir.resolve("try-read.lock"));
        @SuppressWarnings("resource")
        ExecutorService executor = Executors.newSingleThreadExecutor();

        // Operation: request a non-blocking read from the other thread.
        try (ReadWriteFileLock.HeldLock ignored = lock.lockForWrite()) {
            Future<ReadWriteFileLock.HeldLock> result = executor.submit(() -> lock.tryLockForRead());

            // Assertions: same-classloader write contention returns null promptly.
            assertNull(result.get(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void readLockWaitsForWriteLock() throws Exception {
        // Setup: a worker will hold a read lock once the main thread releases its write lock.
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(tempDir.resolve("waiting.lock"));
        @SuppressWarnings("resource")
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            CountDownLatch taskStarted = new CountDownLatch(1);
            CountDownLatch readAcquired = new CountDownLatch(1);
            CountDownLatch releaseRead = new CountDownLatch(1);
            Future<?> readFuture;

            // Operation: start blocking read acquisition while the write lock is held.
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

                // Assertions: the read does not complete while the write remains held.
                assertTrue(taskStarted.await(1, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> readFuture.get(100, TimeUnit.MILLISECONDS));
            }

            // Assertions: releasing the write lets the read acquire and finish normally.
            assertTrue(readAcquired.await(1, TimeUnit.SECONDS));
            releaseRead.countDown();
            readFuture.get(1, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void timedLocksWaitForSameClassloaderContention() throws Exception {
        // Setup: cover a read blocked by a write and a write blocked by a read in one classloader.
        // Operation: perform immediate and timed attempts while each conflicting lock is held.
        // Assertions: the helper verifies waiting, timeout, and recovery for both lock directions.
        assertTimedLockExpiresForSameClassloaderContention(
                false,
                true,
                "timed-local-write-read.lock"
        );
        assertTimedLockExpiresForSameClassloaderContention(
                true,
                false,
                "timed-local-read-write.lock"
        );
    }

    @Test
    void sameClassloaderLockWaitsAreInterruptibleAndRecoverable() throws Exception {
        // Setup: cover blocking and timed local waits behind an existing write lock.
        // Operation: interrupt each waiting thread before the lock becomes available.
        // Assertions: the helper verifies interruption status and subsequent lock recovery.
        assertSameClassloaderLockWaitIsInterruptible(false, "interrupt-local-blocking.lock");
        assertSameClassloaderLockWaitIsInterruptible(true, "interrupt-local-timed.lock");
    }

    @Test
    void blockingLocksWaitAcrossIsolatedClassloaders() throws Exception {
        // Setup: isolated classloaders contend in both write-to-read and read-to-write directions.
        // Operation: release the first classloader's lock while the second is waiting.
        // Assertions: the helper verifies the second lock blocks first and then acquires promptly.
        assertBlockingLocksWaitAcrossIsolatedClassloaders("lockForWrite", "lockForRead", "write-read.lock");
        assertBlockingLocksWaitAcrossIsolatedClassloaders("lockForRead", "lockForWrite", "read-write.lock");
    }

    @Test
    void timedLocksReturnNullAcrossIsolatedClassloaders() throws Exception {
        // Setup: isolated classloaders contend in both write-to-read and read-to-write directions.
        // Operation: request immediate and timed locks from the second classloader.
        // Assertions: the helper verifies both attempts return null while contention remains.
        assertTimedLockReturnsNullAcrossIsolatedClassloaders(
                "lockForWrite",
                "tryLockForRead",
                "timed-write-read.lock"
        );
        assertTimedLockReturnsNullAcrossIsolatedClassloaders(
                "lockForRead",
                "tryLockForWrite",
                "timed-read-write.lock"
        );
    }

    @Test
    void timedLockAcquiresAfterIsolatedClassloaderReleases() throws Exception {
        // Setup: one isolated classloader holds a write lock and another prepares a timed write attempt.
        Path lockFile = tempDir.resolve("timed-release.lock");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch taskStarted = new CountDownLatch(1);

        try (
                IsolatedLibraryClassLoader firstClassloader = new IsolatedLibraryClassLoader();
                IsolatedLibraryClassLoader secondClassloader = new IsolatedLibraryClassLoader()
        ) {
            Closeable firstLock = invokeLock(newIsolatedLock(firstClassloader, lockFile), "lockForWrite");
            Object secondLock = newIsolatedLock(secondClassloader, lockFile);
            Future<Boolean> secondFuture = executor.submit(() -> {
                taskStarted.countDown();
                Closeable heldLock = invokeTimedLock(secondLock, "tryLockForWrite", 1L, TimeUnit.SECONDS);
                if (heldLock == null) {
                    return false;
                }
                try (Closeable ignored = heldLock) {
                    return true;
                }
            });

            try {
                // Assertions: the timed acquisition remains pending while the first lock is held.
                assertTrue(taskStarted.await(1, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> secondFuture.get(100, TimeUnit.MILLISECONDS));

                // Operation: release the first classloader's lock during the timeout.
                firstLock.close();
                firstLock = null;

                // Assertions: the release notification wakes the timed acquisition promptly.
                assertTrue(secondFuture.get(500, TimeUnit.MILLISECONDS));
            } finally {
                if (firstLock != null) {
                    firstLock.close();
                }
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void blockingLockWaitAcrossIsolatedClassloadersIsInterruptible() throws Exception {
        // Setup: one isolated classloader holds a write lock while another thread prepares to wait.
        Path lockFile = tempDir.resolve("interrupted-classloader.lock");
        AtomicReference<Throwable> result = new AtomicReference<>();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch taskFinished = new CountDownLatch(1);

        try (
                IsolatedLibraryClassLoader firstClassloader = new IsolatedLibraryClassLoader();
                IsolatedLibraryClassLoader secondClassloader = new IsolatedLibraryClassLoader();
                Closeable firstLock = invokeLock(newIsolatedLock(firstClassloader, lockFile), "lockForWrite")
        ) {
            Object secondLock = newIsolatedLock(secondClassloader, lockFile);
            Thread waitingThread = new Thread(() -> {
                taskStarted.countDown();
                try (Closeable ignored = invokeLock(secondLock, "lockForWrite")) {
                    result.set(new AssertionError("Lock acquisition unexpectedly succeeded."));
                } catch (Throwable e) {
                    result.set(e);
                    interruptRestored.set(Thread.currentThread().isInterrupted());
                } finally {
                    taskFinished.countDown();
                }
            }, "isolated-file-lock-waiter");

            // Operation: start the blocked acquisition and interrupt it while contention remains.
            waitingThread.start();
            assertTrue(taskStarted.await(1, TimeUnit.SECONDS));
            assertFalse(taskFinished.await(100, TimeUnit.MILLISECONDS));
            waitingThread.interrupt();
            waitingThread.join(TimeUnit.SECONDS.toMillis(1));

            // Assertions: the waiter exits with interruption reported and its status preserved.
            assertFalse(waitingThread.isAlive());
            assertInstanceOf(InterruptedIOException.class, result.get(), String.valueOf(result.get()));
            assertTrue(interruptRestored.get());
        }
    }

    @Test
    void tryLocksReturnNullWhenOperatingSystemFileLockIsHeldForWriting() throws Exception {
        // Setup: a raw exclusive FileLock represents another lock owner.
        Path lockFile = tempDir.resolve("try-read-os.lock");
        Files.createFile(lockFile);
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        // Operation: request immediate read and write locks while the raw lock is held.
        try (
                FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
                FileLock ignored = channel.lock(0L, Long.MAX_VALUE, false)
        ) {
            // Assertions: both non-blocking modes report operating-system contention as unavailable.
            assertNull(lock.tryLockForRead());
            assertNull(lock.tryLockForWrite());
        }

        // Operation: retry a read after releasing the raw file lock.
        try (ReadWriteFileLock.HeldLock read = lock.tryLockForRead()) {
            // Assertions: the failed attempts did not poison later acquisition.
            assertNotNull(read);
        }
    }

    @Test
    void writeLockHoldsOperatingSystemFileLock() throws Exception {
        // Setup: acquire a library write lock and open a second raw channel to its lock file.
        Path lockFile = tempDir.resolve("operating-system.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        // Operation: request an overlapping raw exclusive lock in the same JVM.
        try (
                ReadWriteFileLock.HeldLock ignored = lock.lockForWrite();
                FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.READ, StandardOpenOption.WRITE)
        ) {
            // Assertions: the library write lock owns the operating-system file-lock range.
            assertThrows(
                    OverlappingFileLockException.class,
                    () -> channel.tryLock(0L, Long.MAX_VALUE, false)
            );
        }
    }

    @Test
    void readLockHoldsOperatingSystemFileLockUntilLastLocalReadLockCloses() throws Exception {
        // Setup: two local readers share the file lock while another process probes for writing.
        Path lockFile = tempDir.resolve("last-read.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        // Operation: close the first reader while leaving the second reader held.
        try (ReadWriteFileLock.HeldLock second = lock.lockForRead()) {
            try (ReadWriteFileLock.HeldLock first = lock.lockForRead()) {
                assertNotNull(first);
            }

            // Assertions: the shared process file lock survives until the last local reader closes.
            assertEquals(BUSY, ProcessLockClient.run(TRY_WRITE, lockFile));
        }

        // Assertions: closing the final reader releases the process file lock.
        assertEquals(LOCKED, ProcessLockClient.run(TRY_WRITE, lockFile));
    }

    @ParameterizedTest(name = "holder={0}, attemptedRead={1}")
    @CsvSource({
            "HOLD_WRITE, true",
            "HOLD_READ, false"
    })
    void blockingLockWaitsForContendingProcess(
            ProcessLockClient.Command holdCommand,
            boolean attemptedRead
    ) throws Exception {
        // Setup: another process holds a lock that conflicts with the requested read or write mode.
        // Operation: start blocking local acquisition, then release the process lock.
        // Assertions: asynchronous waiting completes after release in both lock directions.
        String lockFileName = "blocking-process-" + holdCommand + ".lock";
        assertBlockingLockWaitsForProcess(holdCommand, attemptedRead, lockFileName);
    }

    @ParameterizedTest(name = "holder={0}, attemptedRead={1}")
    @CsvSource({
            "HOLD_WRITE, true",
            "HOLD_READ, false"
    })
    void timedLockWaitsForContendingProcessAndRecoversAfterTimeout(
            ProcessLockClient.Command holdCommand,
            boolean attemptedRead
    ) throws Exception {
        // Setup: another process holds a lock that conflicts with the requested read or write mode.
        // Operation: perform immediate and timed local attempts before retrying after release.
        // Assertions: the deadline expires cleanly and recovery succeeds in both lock directions.
        String lockFileName = "timed-process-timeout-" + holdCommand + ".lock";
        assertTimedLockExpiresForProcess(holdCommand, attemptedRead, lockFileName);
    }

    @ParameterizedTest(name = "timed={0}, holder={1}, attemptedRead={2}")
    @CsvSource({
            "false, HOLD_WRITE, true",
            "false, HOLD_READ, false",
            "true, HOLD_WRITE, true",
            "true, HOLD_READ, false"
    })
    void crossProcessLockWaitIsInterruptibleAndRecoverable(
            boolean timed,
            ProcessLockClient.Command holdCommand,
            boolean attemptedRead
    ) throws Exception {
        // Setup: another process holds a lock that conflicts with the requested read or write mode.
        // Operation: interrupt a blocking or timed acquisition before the process releases its lock.
        // Assertions: interruption returns promptly, preserves status, and leaves the lock reusable.
        String lockFileName = "interrupt-process-" + timed + "-" + holdCommand + ".lock";
        assertCrossProcessLockWaitIsInterruptible(timed, holdCommand, attemptedRead, lockFileName);
    }

    @ParameterizedTest(name = "holder={0}, attemptedRead={1}")
    @CsvSource({
            "HOLD_WRITE, true",
            "HOLD_READ, false"
    })
    void timedLockAcquiresAfterContendingProcessReleases(
            ProcessLockClient.Command holdCommand,
            boolean attemptedRead
    ) throws Exception {
        // Setup: another process holds a lock that conflicts with a timed read or write acquisition.
        // Operation: release the process lock while the timed acquisition remains pending.
        // Assertions: asynchronous completion acquires before the deadline in both lock directions.
        String lockFileName = "timed-process-release-" + holdCommand + ".lock";
        assertTimedLockAcquiresAfterProcessReleases(holdCommand, attemptedRead, lockFileName);
    }

    @Test
    void pendingProcessLockKeepsOtherReadAttemptsTimedAndInterruptible() throws Exception {
        // Setup: another process owns the write lock while one local reader begins a blocking wait.
        Path lockFile = tempDir.resolve("pending-process-read.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);
        @SuppressWarnings("resource")
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch firstReaderStarted = new CountDownLatch(1);

        // Operation: make immediate, timed, and interruptible reads queue behind the pending reader.
        try (ProcessLockClient.HeldProcessLock ignored = ProcessLockClient.hold(HOLD_WRITE, lockFile)) {
            Future<?> firstReader = executor.submit(() -> {
                firstReaderStarted.countDown();
                try (ReadWriteFileLock.HeldLock acquired = lock.lockForRead()) {
                    return null;
                }
            });

            // Assertions: the first reader remains pending on the process lock.
            assertTrue(firstReaderStarted.await(1, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> firstReader.get(100L, TimeUnit.MILLISECONDS));

            // Assertions: later reads stay non-blocking, timed, and interruptible as requested.
            assertNull(lock.tryLockForRead());
            assertNull(lock.tryLockForRead(150L, TimeUnit.MILLISECONDS));
            assertPendingReadIsInterruptible(lock, false);
            assertPendingReadIsInterruptible(lock, true);

            firstReader.cancel(true);
            assertThrows(CancellationException.class, firstReader::get);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }

        // Operation: retry after process contention and the pending local acquisition are gone.
        try (ReadWriteFileLock.HeldLock recovered = lock.tryLockForRead(1L, TimeUnit.SECONDS)) {
            // Assertions: cancellation and interruption leave shared process-lock state reusable.
            assertNotNull(recovered);
        }
    }

    @Test
    void readLocksCoordinateAcrossProcesses() throws Exception {
        // Setup: this process holds a library read lock while a child process probes both modes.
        Path lockFile = tempDir.resolve("process-read.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        // Operation: request child-process read and write locks while the local read is held.
        try (ReadWriteFileLock.HeldLock ignored = lock.lockForRead()) {
            // Assertions: another reader may join, but a writer cannot.
            assertEquals(LOCKED, ProcessLockClient.run(TRY_READ, lockFile));
            assertEquals(BUSY, ProcessLockClient.run(TRY_WRITE, lockFile));
        }

        // Assertions: the writer can acquire after the local read closes.
        assertEquals(LOCKED, ProcessLockClient.run(TRY_WRITE, lockFile));
    }

    @Test
    void writeLocksCoordinateAcrossProcesses() throws Exception {
        // Setup: this process holds a library write lock while a child process probes both modes.
        Path lockFile = tempDir.resolve("process-write.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        // Operation: request child-process read and write locks while the local write is held.
        try (ReadWriteFileLock.HeldLock ignored = lock.lockForWrite()) {
            // Assertions: the exclusive write blocks both process lock modes.
            assertEquals(BUSY, ProcessLockClient.run(TRY_READ, lockFile));
            assertEquals(BUSY, ProcessLockClient.run(TRY_WRITE, lockFile));
        }

        // Assertions: both modes can acquire after the local write closes.
        assertEquals(LOCKED, ProcessLockClient.run(TRY_READ, lockFile));
        assertEquals(LOCKED, ProcessLockClient.run(TRY_WRITE, lockFile));
    }

    @Test
    void closeFromDifferentThreadDoesNotReleaseReadLock() throws Exception {
        // Setup: the current thread owns a read lock that another thread will try to close.
        Path lockFile = tempDir.resolve("wrong-thread-read.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        // Operation: invoke close from the non-owner thread, then probe the process write lock.
        try (ReadWriteFileLock.HeldLock read = lock.lockForRead()) {
            // Assertions: wrong-thread close fails and leaves the read lock held.
            assertCloseFromDifferentThreadFails(read);
            assertEquals(BUSY, ProcessLockClient.run(TRY_WRITE, lockFile));
        }

        // Operation: request a write after the owner closes the read lock.
        try (ReadWriteFileLock.HeldLock write = lock.tryLockForWrite()) {
            // Assertions: the owner can release the lock normally after the failed close.
            assertNotNull(write);
        }
    }

    @Test
    void closeFromDifferentThreadDoesNotReleaseWriteLock() throws Exception {
        // Setup: the current thread owns a write lock that another thread will try to close.
        Path lockFile = tempDir.resolve("wrong-thread-write.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        // Operation: invoke close from the non-owner thread, then probe the process read lock.
        try (ReadWriteFileLock.HeldLock write = lock.lockForWrite()) {
            // Assertions: wrong-thread close fails and leaves the write lock held.
            assertCloseFromDifferentThreadFails(write);
            assertEquals(BUSY, ProcessLockClient.run(TRY_READ, lockFile));
        }

        // Operation: request a read after the owner closes the write lock.
        try (ReadWriteFileLock.HeldLock read = lock.tryLockForRead()) {
            // Assertions: the owner can release the lock normally after the failed close.
            assertNotNull(read);
        }
    }

    @Test
    void failedFileSystemAcquisitionDoesNotPoisonLock() throws Exception {
        // Setup: a regular file occupies the path where the lock's parent directory must be.
        Path parent = tempDir.resolve("not-a-directory");
        Files.createFile(parent);
        Path lockFile = parent.resolve("recoverable.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        // Operation: acquire once against the invalid path, then repair the filesystem and retry.
        // Assertions: the initial filesystem failure is surfaced to the caller.
        assertThrows(IOException.class, lock::lockForWrite);

        Files.delete(parent);
        Files.createDirectory(parent);
        try (ReadWriteFileLock.HeldLock recovered = lock.lockForWrite()) {
            // Assertions: failed acquisition released all local state and the retry creates the lock file.
            assertTrue(Files.exists(lockFile));
        }
    }

    @Test
    void createsParentDirectoriesAndLockFile() throws Exception {
        // Setup: neither the lock file nor its immediate parent directory exists.
        Path lockFile = tempDir.resolve("locks").resolve("nested.lock");
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        // Operation: acquire a write lock for the nested path.
        try (ReadWriteFileLock.HeldLock ignored = lock.lockForWrite()) {
            // Assertions: acquisition creates the required parent directory and lock file.
            assertTrue(Files.exists(lockFile));
        }
    }

    @Test
    void lockCloseIsIdempotent() throws Exception {
        // Setup: acquire one write lock that will be explicitly closed twice.
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(tempDir.resolve("idempotent.lock"));

        // Operation: close the same held lock twice, then acquire the lock again.
        ReadWriteFileLock.HeldLock held = lock.lockForWrite();
        held.close();
        held.close();

        try (ReadWriteFileLock.HeldLock ignored = lock.lockForWrite()) {
            // Assertions: duplicate close is harmless and releases state exactly once.
            assertNotNull(ignored);
        }
    }

    private void assertTimedLockExpiresForSameClassloaderContention(
            boolean holderRead,
            boolean attemptedRead,
            String lockFileName
    ) throws Exception {
        // Setup: one local thread holds the conflicting lock while a worker prepares timed attempts.
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(tempDir.resolve(lockFileName));
        @SuppressWarnings("resource")
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch taskStarted = new CountDownLatch(1);

        // Operation: perform a zero-timeout attempt followed by a finite timed attempt.
        try (ReadWriteFileLock.HeldLock ignored = acquireBlocking(lock, holderRead)) {
            Future<Boolean> result = executor.submit(() -> {
                taskStarted.countDown();
                try (ReadWriteFileLock.HeldLock immediate = acquireTimed(
                        lock,
                        attemptedRead,
                        0L,
                        TimeUnit.NANOSECONDS
                )) {
                    if (immediate != null) {
                        return true;
                    }
                }
                ReadWriteFileLock.HeldLock acquired = acquireTimed(
                        lock,
                        attemptedRead,
                        250L,
                        TimeUnit.MILLISECONDS
                );
                if (acquired == null) {
                    return false;
                }
                try (ReadWriteFileLock.HeldLock held = acquired) {
                    return true;
                }
            });

            // Assertions: the timed call waits for its deadline and returns without acquiring.
            assertTrue(taskStarted.await(1, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> result.get(75L, TimeUnit.MILLISECONDS));
            assertFalse(result.get(1L, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }

        // Operation: retry after the conflicting local lock has closed.
        try (ReadWriteFileLock.HeldLock recovered = acquireTimed(lock, attemptedRead, 1L, TimeUnit.SECONDS)) {
            // Assertions: timeout leaves the lock state reusable.
            assertNotNull(recovered);
        }
    }

    private void assertSameClassloaderLockWaitIsInterruptible(boolean timed, String lockFileName) throws Exception {
        // Setup: a write lock blocks a worker's local read acquisition.
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(tempDir.resolve(lockFileName));
        AtomicReference<Throwable> result = new AtomicReference<>();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch taskFinished = new CountDownLatch(1);

        // Operation: start blocking or timed acquisition and interrupt it while the write remains held.
        try (ReadWriteFileLock.HeldLock ignored = lock.lockForWrite()) {
            Thread waitingThread = new Thread(() -> {
                taskStarted.countDown();
                try (ReadWriteFileLock.HeldLock unexpected = timed
                        ? lock.tryLockForRead(5L, TimeUnit.SECONDS)
                        : lock.lockForRead()) {
                    result.set(new AssertionError(
                            unexpected == null
                                    ? "Timed out instead of being interrupted."
                                    : "Lock acquisition unexpectedly succeeded."
                    ));
                } catch (Throwable e) {
                    result.set(e);
                    interruptRestored.set(Thread.currentThread().isInterrupted());
                } finally {
                    taskFinished.countDown();
                }
            }, timed ? "timed-local-lock-waiter" : "blocking-local-lock-waiter");

            waitingThread.start();
            assertTrue(taskStarted.await(1, TimeUnit.SECONDS));
            assertFalse(taskFinished.await(100, TimeUnit.MILLISECONDS));
            waitingThread.interrupt();
            waitingThread.join(TimeUnit.SECONDS.toMillis(1));

            // Assertions: the waiter exits with interruption reported and its status preserved.
            assertFalse(waitingThread.isAlive());
            assertInstanceOf(InterruptedIOException.class, result.get(), String.valueOf(result.get()));
            assertTrue(interruptRestored.get());
        }

        // Operation: retry after interruption and release of the original write lock.
        try (ReadWriteFileLock.HeldLock recovered = lock.tryLockForRead(1L, TimeUnit.SECONDS)) {
            // Assertions: interrupted acquisition did not poison local or process lock state.
            assertNotNull(recovered);
        }
    }

    private void assertCrossProcessLockWaitIsInterruptible(
            boolean timed,
            ProcessLockClient.Command holdCommand,
            boolean attemptedRead,
            String lockFileName
    ) throws Exception {
        // Setup: a child process holds a conflicting lock while a local worker prepares to wait.
        Path lockFile = tempDir.resolve(lockFileName);
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);
        AtomicReference<Throwable> result = new AtomicReference<>();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch taskFinished = new CountDownLatch(1);

        Thread waitingThread = new Thread(() -> {
            taskStarted.countDown();
            try (ReadWriteFileLock.HeldLock unexpected = timed
                    ? acquireTimed(lock, attemptedRead, 5L, TimeUnit.SECONDS)
                    : acquireBlocking(lock, attemptedRead)) {
                result.set(new AssertionError(
                        unexpected == null
                                ? "Timed out instead of being interrupted."
                                : "Lock acquisition unexpectedly succeeded."
                ));
            } catch (Throwable e) {
                result.set(e);
                interruptRestored.set(Thread.currentThread().isInterrupted());
            } finally {
                taskFinished.countDown();
            }
        }, timed ? "timed-process-lock-waiter" : "blocking-process-lock-waiter");
        Thread interruptingThread = new Thread(waitingThread::interrupt, "process-lock-interrupter");
        interruptingThread.setDaemon(true);
        ProcessLockClient.HeldProcessLock processLock = ProcessLockClient.hold(holdCommand, lockFile);

        // Operation: start blocking or timed acquisition and interrupt it during process contention.
        try {
            waitingThread.start();
            assertTrue(taskStarted.await(1, TimeUnit.SECONDS));
            assertFalse(taskFinished.await(100, TimeUnit.MILLISECONDS));
            interruptingThread.start();

            // Assertions: the waiter exits while the process still holds the lock.
            // Interrupting from a separate thread prevents JDK-8152085 from deadlocking the test
            // controller if synchronous FileChannel.lock() is accidentally reintroduced.
            assertTrue(taskFinished.await(1L, TimeUnit.SECONDS));
            interruptingThread.join(TimeUnit.SECONDS.toMillis(1L));
            assertFalse(interruptingThread.isAlive(), "Thread.interrupt() did not return promptly.");
            assertInstanceOf(InterruptedIOException.class, result.get(), String.valueOf(result.get()));
            assertTrue(interruptRestored.get());
        } finally {
            try {
                processLock.close();
            } finally {
                interruptingThread.join(TimeUnit.SECONDS.toMillis(1L));
                waitingThread.join(TimeUnit.SECONDS.toMillis(1L));
            }
        }

        assertFalse(waitingThread.isAlive(), "Interrupted lock acquisition did not terminate.");

        // Operation: retry after interruption and release of the process lock.
        try (ReadWriteFileLock.HeldLock recovered = acquireTimed(
                lock,
                attemptedRead,
                1L,
                TimeUnit.SECONDS
        )) {
            // Assertions: interrupted process contention leaves the lock reusable.
            assertNotNull(recovered);
        }
    }

    private void assertTimedLockAcquiresAfterProcessReleases(
            ProcessLockClient.Command holdCommand,
            boolean attemptedRead,
            String lockFileName
    ) throws Exception {
        // Setup: a child process holds the conflicting lock while a timed worker prepares to acquire.
        Path lockFile = tempDir.resolve(lockFileName);
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch taskStarted = new CountDownLatch(1);
        ProcessLockClient.HeldProcessLock processLock = ProcessLockClient.hold(holdCommand, lockFile);

        // Operation: start timed acquisition, verify it is pending, and release the child lock.
        try {
            Future<Boolean> result = executor.submit(() -> {
                taskStarted.countDown();
                ReadWriteFileLock.HeldLock acquired = acquireTimed(
                        lock,
                        attemptedRead,
                        2L,
                        TimeUnit.SECONDS
                );
                if (acquired == null) {
                    return false;
                }
                try (ReadWriteFileLock.HeldLock ignored = acquired) {
                    return true;
                }
            });

            assertTrue(taskStarted.await(1L, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> result.get(100L, TimeUnit.MILLISECONDS));

            processLock.close();
            processLock = null;

            // Assertions: releasing the process lock completes the pending timed acquisition.
            assertTrue(result.get(2L, TimeUnit.SECONDS));
        } finally {
            if (processLock != null) {
                processLock.close();
            }
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1L, TimeUnit.SECONDS));
        }
    }

    private static void assertPendingReadIsInterruptible(ReadWriteFileLock lock, boolean timed) throws Exception {
        // Setup: a worker will queue behind process-lock state already occupied by another reader.
        AtomicReference<Throwable> result = new AtomicReference<>();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch taskFinished = new CountDownLatch(1);
        Thread waitingThread = new Thread(() -> {
            taskStarted.countDown();
            try (ReadWriteFileLock.HeldLock unexpected = timed
                    ? lock.tryLockForRead(5L, TimeUnit.SECONDS)
                    : lock.lockForRead()) {
                result.set(new AssertionError(
                        unexpected == null
                                ? "Timed out instead of being interrupted."
                                : "Lock acquisition unexpectedly succeeded."
                ));
            } catch (Throwable e) {
                result.set(e);
                interruptRestored.set(Thread.currentThread().isInterrupted());
            } finally {
                taskFinished.countDown();
            }
        }, timed ? "timed-process-state-waiter" : "blocking-process-state-waiter");

        // Operation: start the queued read and interrupt it before process-lock state becomes available.
        waitingThread.start();
        assertTrue(taskStarted.await(1, TimeUnit.SECONDS));
        assertFalse(taskFinished.await(100, TimeUnit.MILLISECONDS));
        waitingThread.interrupt();
        waitingThread.join(TimeUnit.SECONDS.toMillis(1));

        // Assertions: the queued waiter exits interruptibly and preserves its status.
        assertFalse(waitingThread.isAlive());
        assertInstanceOf(InterruptedIOException.class, result.get(), String.valueOf(result.get()));
        assertTrue(interruptRestored.get());
    }

    private void assertBlockingLockWaitsForProcess(
            ProcessLockClient.Command holdCommand,
            boolean attemptedRead,
            String lockFileName
    ) throws Exception {
        // Setup: a child process holds the conflicting lock while a local worker prepares to acquire.
        Path lockFile = tempDir.resolve(lockFileName);
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch taskStarted = new CountDownLatch(1);

        // Operation: start blocking acquisition, then release the child process's lock.
        try (ProcessLockClient.HeldProcessLock processLock = ProcessLockClient.hold(holdCommand, lockFile)) {
            Future<Boolean> result = executor.submit(() -> {
                taskStarted.countDown();
                try (ReadWriteFileLock.HeldLock ignored = acquireBlocking(lock, attemptedRead)) {
                    return true;
                }
            });

            // Assertions: the acquisition remains pending while process contention exists.
            assertTrue(taskStarted.await(1, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> result.get(100L, TimeUnit.MILLISECONDS));

            processLock.close();

            // Assertions: asynchronous blocking acquisition completes after the process lock is released.
            assertTrue(result.get(2L, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    private void assertTimedLockExpiresForProcess(
            ProcessLockClient.Command holdCommand,
            boolean attemptedRead,
            String lockFileName
    ) throws Exception {
        // Setup: a child process holds the conflicting lock throughout the timed attempts.
        Path lockFile = tempDir.resolve(lockFileName);
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        // Operation: perform a zero-timeout attempt followed by a finite timed attempt.
        try (ProcessLockClient.HeldProcessLock ignored = ProcessLockClient.hold(holdCommand, lockFile)) {
            try (ReadWriteFileLock.HeldLock immediate = acquireTimed(
                    lock,
                    attemptedRead,
                    0L,
                    TimeUnit.NANOSECONDS
            )) {
                // Assertions: zero timeout performs one attempt and returns null immediately.
                assertNull(immediate);
            }

            long startNanos = System.nanoTime();
            try (ReadWriteFileLock.HeldLock acquired = acquireTimed(
                    lock,
                    attemptedRead,
                    150L,
                    TimeUnit.MILLISECONDS
            )) {
                // Assertions: finite timeout also returns null while contention remains.
                assertNull(acquired);
            }
            long elapsedNanos = System.nanoTime() - startNanos;

            // Assertions: the finite attempt waited rather than behaving as a non-blocking call.
            assertTrue(
                    elapsedNanos >= TimeUnit.MILLISECONDS.toNanos(75L),
                    "Timed acquisition returned without waiting for the contending process."
            );
        }

        // Operation: retry after the child process releases its lock.
        try (ReadWriteFileLock.HeldLock recovered = acquireTimed(lock, attemptedRead, 1L, TimeUnit.SECONDS)) {
            // Assertions: timed process contention leaves the lock reusable.
            assertNotNull(recovered);
        }
    }

    private static ReadWriteFileLock.HeldLock acquireBlocking(
            ReadWriteFileLock lock,
            boolean read
    ) throws IOException {
        return read ? lock.lockForRead() : lock.lockForWrite();
    }

    private static ReadWriteFileLock.HeldLock acquireTimed(
            ReadWriteFileLock lock,
            boolean read,
            long timeout,
            TimeUnit unit
    ) throws IOException {
        return read ? lock.tryLockForRead(timeout, unit) : lock.tryLockForWrite(timeout, unit);
    }

    private static void assertCloseFromDifferentThreadFails(ReadWriteFileLock.HeldLock lock) throws Exception {
        // Setup: a worker thread will close a HeldLock owned by the calling thread.
        @SuppressWarnings("resource")
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            // Operation: invoke close on the non-owner and observe the worker's Future.
            Future<?> result = executor.submit(() -> {
                lock.close();
                return null;
            });
            ExecutionException exception = assertThrows(ExecutionException.class, () -> result.get(1, TimeUnit.SECONDS));

            // Assertions: ownership validation fails with the same exception as a JVM lock.
            assertInstanceOf(IllegalMonitorStateException.class, exception.getCause());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    private void assertBlockingLocksWaitAcrossIsolatedClassloaders(
            String firstMethod,
            String secondMethod,
            String lockFileName
    ) throws Exception {
        // Setup: two isolated classloaders use the same lock file and conflicting acquisition modes.
        Path lockFile = tempDir.resolve(lockFileName);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch secondAcquired = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);

        try (
                IsolatedLibraryClassLoader firstClassloader = new IsolatedLibraryClassLoader();
                IsolatedLibraryClassLoader secondClassloader = new IsolatedLibraryClassLoader()
        ) {
            Object firstLock = newIsolatedLock(firstClassloader, lockFile);
            Object secondLock = newIsolatedLock(secondClassloader, lockFile);
            Closeable firstHeldLock = invokeLock(firstLock, firstMethod);

            // Operation: start blocking acquisition through the second classloader.
            Future<?> secondFuture = executor.submit(() -> {
                taskStarted.countDown();
                try (Closeable ignored = invokeLock(secondLock, secondMethod)) {
                    secondAcquired.countDown();
                    if (!releaseSecond.await(1, TimeUnit.SECONDS)) {
                        throw new TimeoutException("Timed out waiting to release isolated lock.");
                    }
                }
                return null;
            });

            try {
                // Assertions: JVM overlap is treated as contention while the first lock remains held.
                assertTrue(taskStarted.await(1, TimeUnit.SECONDS));
                assertFalse(secondAcquired.await(100, TimeUnit.MILLISECONDS));

                // Operation: release through the first classloader.
                firstHeldLock.close();
                firstHeldLock = null;

                // Assertions: the JVM-wide notification wakes the second classloader promptly.
                assertTrue(secondAcquired.await(500, TimeUnit.MILLISECONDS));
                releaseSecond.countDown();
                secondFuture.get(1, TimeUnit.SECONDS);
            } finally {
                releaseSecond.countDown();
                if (firstHeldLock != null) {
                    firstHeldLock.close();
                }
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    private void assertTimedLockReturnsNullAcrossIsolatedClassloaders(
            String firstMethod,
            String timedMethod,
            String lockFileName
    ) throws Exception {
        // Setup: one isolated classloader holds a lock that conflicts with the second classloader.
        Path lockFile = tempDir.resolve(lockFileName);
        try (
                IsolatedLibraryClassLoader firstClassloader = new IsolatedLibraryClassLoader();
                IsolatedLibraryClassLoader secondClassloader = new IsolatedLibraryClassLoader();
                Closeable ignored = invokeLock(newIsolatedLock(firstClassloader, lockFile), firstMethod)
        ) {
            Object secondLock = newIsolatedLock(secondClassloader, lockFile);
            Method method = secondLock.getClass().getMethod(timedMethod, long.class, TimeUnit.class);

            // Operation: perform zero-timeout and finite timed attempts through the second classloader.
            // Assertions: both return null rather than exposing OverlappingFileLockException.
            assertNull(method.invoke(secondLock, 0L, TimeUnit.NANOSECONDS));
            assertNull(method.invoke(secondLock, 100L, TimeUnit.MILLISECONDS));
        }
    }

    private static Object newIsolatedLock(ClassLoader classloader, Path path) throws Exception {
        Class<?> lockClass = classloader.loadClass(ReadWriteFileLock.class.getName());
        return lockClass.getMethod("forFile", Path.class).invoke(null, path);
    }

    private static Closeable invokeLock(Object lock, String methodName) throws Exception {
        try {
            return (Closeable) lock.getClass().getMethod(methodName).invoke(lock);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new AssertionError(cause);
        }
    }

    private static Closeable invokeTimedLock(
            Object lock,
            String methodName,
            long timeout,
            TimeUnit unit
    ) throws Exception {
        try {
            Method method = lock.getClass().getMethod(methodName, long.class, TimeUnit.class);
            return (Closeable) method.invoke(lock, timeout, unit);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new AssertionError(cause);
        }
    }

    private static final class IsolatedLibraryClassLoader extends URLClassLoader {
        private static final String LIBRARY_PACKAGE = ReadWriteFileLock.class.getPackage().getName() + ".";

        private IsolatedLibraryClassLoader() {
            super(
                    new URL[]{ReadWriteFileLock.class.getProtectionDomain().getCodeSource().getLocation()},
                    ReadWriteFileLockTest.class.getClassLoader()
            );
        }

        @Override
        protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith(LIBRARY_PACKAGE)) {
                Class<?> loadedClass = findLoadedClass(name);
                if (loadedClass == null) {
                    try {
                        loadedClass = findClass(name);
                    } catch (ClassNotFoundException ignored) {
                        // Fall through to regular parent-first loading.
                    }
                }

                if (loadedClass != null) {
                    if (resolve) {
                        resolveClass(loadedClass);
                    }
                    return loadedClass;
                }
            }

            return super.loadClass(name, resolve);
        }
    }

}
