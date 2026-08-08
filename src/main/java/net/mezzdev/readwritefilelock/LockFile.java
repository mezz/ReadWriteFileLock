package net.mezzdev.readwritefilelock;

import org.jspecify.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Represents a file used for inter-process locking.
 */
final class LockFile {
    /**
     * Prefix for the interned monitor name. Interning gives isolated copies of
     * this class a JVM-wide monitor for the same normalized lock-file path.
     */
    private static final String OVERLAP_MONITOR_PREFIX = "net.mezzdev.readwritefilelock.LockFile:";

    /**
     * Initial delay between file-lock attempts during timed cross-process contention.
     */
    private static final long INITIAL_RETRY_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(10L);

    /**
     * Maximum delay between file-lock attempts during timed cross-process contention.
     */
    private static final long MAX_RETRY_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(100L);

    /**
     * Maximum time to await a same-JVM release notification before retrying.
     * The fallback permits progress when the overlapping lock belongs to code
     * that does not use this library and therefore cannot send a notification.
     */
    private static final long OVERLAP_NOTIFICATION_FALLBACK_NANOS = TimeUnit.SECONDS.toNanos(1L);

    private final Path path;

    /**
     * JVM-wide monitor used to wait for overlapping locks on {@link #path}.
     */
    private final Object overlapMonitor;

    LockFile(Path path) {
        this.path = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        this.overlapMonitor = (OVERLAP_MONITOR_PREFIX + this.path).intern();
    }

    /**
     * Returns the JVM-wide monitor used for same-JVM overlap notifications.
     *
     * @return the overlap monitor for this lock-file path
     */
    Object getOverlapMonitor() {
        return overlapMonitor;
    }

    /**
     * Locks this file and closes the file channel if the lock cannot be acquired.
     *
     * @param shared whether the lock should be shared or exclusive
     * @return the held file lock
     * @throws IOException when opening or locking the file fails, or the thread is interrupted
     */
    OpenFileLock lock(boolean shared) throws IOException {
        while (true) {
            checkInterrupted();

            try {
                return lockBlockingOnce(shared);
            } catch (OverlappingFileLockException e) {
                OpenFileLock lock = waitForJvmOverlap(shared, OVERLAP_NOTIFICATION_FALLBACK_NANOS);
                if (lock != null) {
                    return lock;
                }
            }
        }
    }

    private OpenFileLock lockBlockingOnce(boolean shared) throws IOException {
        FileChannel channel = openChannel();
        try {
            return new OpenFileLock(path, channel.lock(0L, Long.MAX_VALUE, shared), overlapMonitor);
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }
    }

    /**
     * Tries to lock this file and closes the file channel if the lock is not acquired.
     *
     * @param shared whether the lock should be shared or exclusive
     * @return the held file lock, or {@code null} when the file is already locked
     * @throws IOException when opening or locking the file fails
     */
    @Nullable
    OpenFileLock tryLock(boolean shared) throws IOException {
        try {
            return tryLockOnce(shared);
        } catch (OverlappingFileLockException e) {
            return null;
        }
    }

    private @Nullable OpenFileLock tryLockOnce(boolean shared) throws IOException {
        FileChannel channel = openChannel();
        FileLock lock;
        try {
            lock = channel.tryLock(0L, Long.MAX_VALUE, shared);
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }

        if (lock == null) {
            channel.close();
            return null;
        }
        return new OpenFileLock(path, lock, overlapMonitor);
    }

    /**
     * Tries to lock this file, waiting up to the given timeout.
     *
     * @param shared whether the lock should be shared or exclusive
     * @param timeoutNanos the maximum time to wait, in nanoseconds
     * @return the held file lock, or {@code null} when the timeout expires
     * @throws IOException when opening or locking the file fails, or the thread is interrupted
     */
    @Nullable
    OpenFileLock tryLock(boolean shared, long timeoutNanos) throws IOException {
        long startNanos = System.nanoTime();
        long retryDelayNanos = INITIAL_RETRY_DELAY_NANOS;
        boolean firstAttempt = true;

        while (true) {
            if (!firstAttempt && remainingNanos(startNanos, timeoutNanos) == 0L) {
                return null;
            }
            firstAttempt = false;

            checkInterrupted();

            try {
                OpenFileLock lock = tryLockOnce(shared);
                if (lock != null) {
                    return lock;
                }
            } catch (OverlappingFileLockException e) {
                long remainingNanos = remainingNanos(startNanos, timeoutNanos);
                if (remainingNanos == 0L) {
                    return null;
                }

                OpenFileLock lock = waitForJvmOverlap(
                        shared,
                        Math.min(OVERLAP_NOTIFICATION_FALLBACK_NANOS, remainingNanos)
                );
                if (lock != null) {
                    return lock;
                }
                continue;
            }

            long remainingNanos = remainingNanos(startNanos, timeoutNanos);
            if (remainingNanos == 0L) {
                return null;
            }

            waitForRetry(Math.min(retryDelayNanos, remainingNanos));
            retryDelayNanos = nextRetryDelayNanos(retryDelayNanos);
        }
    }

    private @Nullable OpenFileLock waitForJvmOverlap(boolean shared, long waitNanos) throws IOException {
        synchronized (overlapMonitor) {
            checkInterrupted();

            // Re-attempt while holding the monitor so a release notification cannot be lost
            // between observing JVM contention and starting to wait.
            try {
				return tryLockOnce(shared);
			} catch (OverlappingFileLockException e) {
                waitForOverlapNotification(waitNanos);
                return null;
            }
        }
    }

    private static long nextRetryDelayNanos(long retryDelayNanos) {
        return Math.min(retryDelayNanos * 2L, MAX_RETRY_DELAY_NANOS);
    }

    private static long remainingNanos(long startNanos, long timeoutNanos) {
        if (timeoutNanos <= 0L) {
            return 0L;
        }

        long elapsedNanos = System.nanoTime() - startNanos;
        if (elapsedNanos <= 0L) {
            return timeoutNanos;
        }
        return elapsedNanos >= timeoutNanos ? 0L : timeoutNanos - elapsedNanos;
    }

    private void checkInterrupted() throws InterruptedIOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException("Interrupted while waiting for file lock: " + path);
        }
    }

    private void waitForRetry(long delayNanos) throws InterruptedIOException {
        long delayMillis = TimeUnit.NANOSECONDS.toMillis(delayNanos);
        int nanoAdjustment = (int) (delayNanos - TimeUnit.MILLISECONDS.toNanos(delayMillis));
        try {
            Thread.sleep(delayMillis, nanoAdjustment);
        } catch (InterruptedException e) {
            throw interruptedIOException(e);
        }
    }

    private void waitForOverlapNotification(long delayNanos) throws InterruptedIOException {
        long delayMillis = TimeUnit.NANOSECONDS.toMillis(delayNanos);
        int nanoAdjustment = (int) (delayNanos - TimeUnit.MILLISECONDS.toNanos(delayMillis));
        try {
            overlapMonitor.wait(delayMillis, nanoAdjustment);
        } catch (InterruptedException e) {
            throw interruptedIOException(e);
        }
    }

    private InterruptedIOException interruptedIOException(InterruptedException cause) {
        Thread.currentThread().interrupt();
        InterruptedIOException exception = new InterruptedIOException(
                "Interrupted while waiting for file lock: " + path
        );
        exception.initCause(cause);
        return exception;
    }

    private FileChannel openChannel() throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        return FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
    }

    @Override
    public String toString() {
        return path.toString();
    }

    static final class OpenFileLock implements Closeable {
        private final Path path;
        private final FileLock lock;

        /**
         * Monitor notified after releasing {@link #lock}, including when closing its channel fails.
         */
        private final Object overlapMonitor;

        private OpenFileLock(Path path, FileLock lock, Object overlapMonitor) {
            this.path = path;
            this.lock = lock;
            this.overlapMonitor = overlapMonitor;
        }

        @Override
        public void close() throws IOException {
            try {
                try {
                    lock.release();
                } finally {
                    lock.channel().close();
                }
            } finally {
                synchronized (overlapMonitor) {
                    overlapMonitor.notifyAll();
                }
            }
        }

        @Override
        public String toString() {
            return path.toString();
        }
    }
}
