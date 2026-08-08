package net.mezzdev.readwritefilelock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InterruptedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LockFileTest {
    @TempDir
    Path tempDir;

    @Test
    void exclusiveLockPreventsOverlappingFileLocksInSameJvm() throws Exception {
        // Setup: one LockFile holds an exclusive lock in this JVM.
        Path lockFile = tempDir.resolve("exclusive.lock");
        LockFile lock = new LockFile(lockFile);

        // Operation: use a separate FileChannel to request overlapping exclusive and shared locks.
        try (
                LockFile.OpenFileLock ignored = lock.lock(false);
                FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.READ, StandardOpenOption.WRITE)
        ) {
            // Assertions: Java rejects both overlapping lock modes in the same JVM.
            assertThrows(
                    OverlappingFileLockException.class,
                    () -> channel.tryLock(0L, Long.MAX_VALUE, false)
            );
            assertThrows(
                    OverlappingFileLockException.class,
                    () -> channel.tryLock(0L, Long.MAX_VALUE, true)
            );
        }
    }

    @Test
    void tryLockReturnsNullForOverlappingFileLocksInSameJvm() throws Exception {
        // Setup: the file is exclusively locked through this LockFile.
        LockFile lock = new LockFile(tempDir.resolve("try-overlap.lock"));

        // Operation: immediately request both exclusive and shared locks through the same LockFile.
        try (LockFile.OpenFileLock ignored = lock.lock(false)) {
            // Assertions: non-blocking acquisition reports both forms of contention as unavailable.
            assertNull(lock.tryLock(false));
            assertNull(lock.tryLock(true));
        }
    }

    @Test
    void directSharedLocksDoNotOverlapInSameJvm() throws Exception {
        // Setup: a direct shared file lock is already held in this JVM.
        LockFile lock = new LockFile(tempDir.resolve("shared-overlap.lock"));

        // Operation: immediately request another shared lock for the same file.
        try (LockFile.OpenFileLock ignored = lock.lock(true)) {
            // Assertions: same-JVM overlap is reported as contention even though both locks are shared.
            assertNull(lock.tryLock(true));
        }
    }

    @Test
    @Timeout(value = 5L, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void timedLockFallsBackForNonCooperatingSameJvmHolder() throws Exception {
        // Setup: a raw FileChannel holds the lock and cannot send this library's release notification.
        Path lockFile = tempDir.resolve("non-cooperating.lock");
        Files.createFile(lockFile);
        LockFile lock = new LockFile(lockFile);
        @SuppressWarnings("resource")
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch taskStarted = new CountDownLatch(1);

        // Operation: start a timed LockFile acquisition while the raw file lock is held.
        try (
                FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
                FileLock directLock = channel.lock(0L, Long.MAX_VALUE, false)
        ) {
            Future<Boolean> result = executor.submit(() -> {
                taskStarted.countDown();
                LockFile.OpenFileLock heldLock = lock.tryLock(false, TimeUnit.SECONDS.toNanos(2L));
                if (heldLock == null) {
                    return false;
                }
                try (LockFile.OpenFileLock ignored = heldLock) {
                    return true;
                }
            });

            // Assertions: acquisition waits instead of failing on the same-JVM overlap.
            assertTrue(taskStarted.await(1, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> result.get(100, TimeUnit.MILLISECONDS));

            // Operation: release the non-cooperating lock without sending a notification.
            directLock.release();

            // Assertions: the fallback retry notices the release and acquires within its timeout.
            assertTrue(result.get(1500, TimeUnit.MILLISECONDS));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest(name = "timed={0}")
    @ValueSource(booleans = {false, true})
    void overlapReleasedBeforeWaitStartsDoesNotLoseNotification(boolean timed) throws Exception {
        // Setup: hold the first lock and prepare a blocking or timed waiter through a second LockFile.
        String lockFileName = timed ? "timed-lost-notification.lock" : "blocking-lost-notification.lock";
        LockFile first = new LockFile(tempDir.resolve(lockFileName));
        LockFile second = new LockFile(tempDir.resolve(lockFileName));
        Object overlapMonitor = first.getOverlapMonitor();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch acquired = new CountDownLatch(1);
        LockFile.OpenFileLock firstLock = first.lock(false);
        Thread waitingThread = null;

        try {
            // Operation: use the monitor as a barrier after the waiter observes OverlappingFileLockException.
            // The waiter cannot enter waitForJvmOverlap's synchronized block until this block exits.
            // close() can re-enter the monitor on this thread, so it releases and notifies before the
            // waiter starts waiting. This forces the exact release-before-wait race under test.
            synchronized (overlapMonitor) {
                waitingThread = startLockingThread(second, timed, acquired, failure);
                awaitBlockedOnOverlapMonitor(waitingThread);
                firstLock.close();
                firstLock = null;
            }

            // Assertions: the guarded retry observes the release and the waiter exits successfully.
            assertTrue(acquired.await(1L, TimeUnit.SECONDS));
            waitingThread.join(TimeUnit.SECONDS.toMillis(1L));

            assertFalse(waitingThread.isAlive());
            assertNull(failure.get(), String.valueOf(failure.get()));
        } finally {
            if (firstLock != null) {
                firstLock.close();
            }
            if (waitingThread != null && waitingThread.isAlive()) {
                waitingThread.interrupt();
                waitingThread.join(TimeUnit.SECONDS.toMillis(1L));
            }
        }
    }

    @Test
    void blockingLockWaitsNativelyWhenProcessWinsJvmOverlapHandoff() throws Exception {
        // Setup: one same-JVM lock is held while a second LockFile prepares to wait for it.
        Path lockFile = tempDir.resolve("jvm-process-handoff.lock");
        LockFile first = new LockFile(lockFile);
        LockFile second = new LockFile(lockFile);
        Object overlapMonitor = first.getOverlapMonitor();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch acquired = new CountDownLatch(1);
        LockFile.OpenFileLock firstLock = first.lock(false);
        ProcessLockClient.HeldProcessLock processLock = null;
        Thread waitingThread = null;

        try {
            // Operation: use the monitor as a barrier after the waiter observes OverlappingFileLockException.
            // close() re-enters the monitor on this thread, but the waiter cannot perform its guarded retry
            // until this block exits. This gives the child process the file lock before the retry runs.
            synchronized (overlapMonitor) {
                waitingThread = startLockingThread(second, false, acquired, failure);
                awaitBlockedOnOverlapMonitor(waitingThread);

                firstLock.close();
                firstLock = null;
                processLock = ProcessLockClient.hold(ProcessLockClient.Command.HOLD_WRITE, lockFile);
            }

            // Assertions: the blocking waiter stays blocked while the process owns the file lock.
            assertFalse(acquired.await(100L, TimeUnit.MILLISECONDS));

            // Operation: release the process lock so native FileChannel.lock() can complete.
            processLock.close();
            processLock = null;

            // Assertions: the waiter acquires cleanly after the cross-process handoff.
            assertTrue(acquired.await(2L, TimeUnit.SECONDS));
            waitingThread.join(TimeUnit.SECONDS.toMillis(1L));

            assertFalse(waitingThread.isAlive());
            assertNull(failure.get(), String.valueOf(failure.get()));
        } finally {
            if (firstLock != null) {
                firstLock.close();
            }
            if (processLock != null) {
                processLock.close();
            }
            if (waitingThread != null && waitingThread.isAlive()) {
                waitingThread.interrupt();
                waitingThread.join(TimeUnit.SECONDS.toMillis(1L));
            }
        }
    }

    @ParameterizedTest(name = "timed={0}")
    @ValueSource(booleans = {false, true})
    void preInterruptedLockFailsWithoutPoisoningLock(boolean timed) throws Exception {
        // Setup: a worker will enter blocking or timed acquisition with its interrupt status already set.
        String lockFileName = timed ? "pre-interrupted-timed.lock" : "pre-interrupted-blocking.lock";
        LockFile lock = new LockFile(tempDir.resolve(lockFileName));
        AtomicReference<Throwable> result = new AtomicReference<>();
        AtomicBoolean interruptPreserved = new AtomicBoolean();
        Thread thread = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                LockFile.OpenFileLock heldLock = timed
                        ? lock.tryLock(false, TimeUnit.SECONDS.toNanos(1L))
                        : lock.lock(false);
                if (heldLock == null) {
                    result.set(new AssertionError("Lock timed out instead of reporting interruption."));
                    return;
                }
                try (LockFile.OpenFileLock ignored = heldLock) {
                    result.set(new AssertionError("Pre-interrupted lock acquisition unexpectedly succeeded."));
                }
            } catch (Throwable e) {
                result.set(e);
            } finally {
                interruptPreserved.set(Thread.currentThread().isInterrupted());
            }
        }, timed ? "pre-interrupted-timed-lock" : "pre-interrupted-blocking-lock");

        // Operation: run the pre-interrupted acquisition to completion.
        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(1L));

        // Assertions: acquisition fails interruptibly, preserves status, and leaves the lock usable.
        assertFalse(thread.isAlive());
        assertInstanceOf(InterruptedIOException.class, result.get(), String.valueOf(result.get()));
        assertTrue(interruptPreserved.get());
        try (LockFile.OpenFileLock recovered = lock.lock(false)) {
            assertNotNull(recovered);
        }
    }

    @Test
    void closeReleasesFileLock() throws Exception {
        // Setup: acquire and close an exclusive LockFile lock.
        Path lockFile = tempDir.resolve("release.lock");
        LockFile lock = new LockFile(lockFile);

        // Operation: close the library lock, then request the same range through a raw FileChannel.
        try (LockFile.OpenFileLock ignored = lock.lock(false)) {
            // Assertions: acquisition creates the lock file.
            assertTrue(Files.exists(lockFile));
        }

        try (
                FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
                FileLock fileLock = channel.tryLock(0L, Long.MAX_VALUE, false)
        ) {
            // Assertions: closing the library lock released the operating-system lock.
            assertNotNull(fileLock);
        }
    }

    private static Thread startLockingThread(
            LockFile lock,
            boolean timed,
            CountDownLatch acquired,
            AtomicReference<Throwable> failure
    ) {
        Thread thread = new Thread(() -> {
            try {
                LockFile.OpenFileLock heldLock = timed
                        ? lock.tryLock(false, TimeUnit.SECONDS.toNanos(2L))
                        : lock.lock(false);
                if (heldLock == null) {
                    throw new AssertionError("Timed lock unexpectedly expired.");
                }
                try (LockFile.OpenFileLock ignored = heldLock) {
                    acquired.countDown();
                }
            } catch (Throwable e) {
                failure.set(e);
            }
        }, timed ? "timed-overlap-waiter" : "blocking-overlap-waiter");
        thread.start();
        return thread;
    }

    @SuppressWarnings("BusyWait")
    private static void awaitBlockedOnOverlapMonitor(Thread thread) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        while (
                thread.isAlive()
                        && thread.getState() != Thread.State.BLOCKED
                        && System.nanoTime() < deadlineNanos
        ) {
            Thread.sleep(1L);
        }

        assertTrue(thread.isAlive(), "Locking thread exited before reaching the overlap monitor.");
        assertSame(Thread.State.BLOCKED, thread.getState(), "Locking thread did not block on the overlap monitor: " + thread.getState());
    }

}
