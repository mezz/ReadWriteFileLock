package net.mezzdev.readwritefilelock;

import org.jspecify.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
        boolean firstAttempt = true;

        while (true) {
            checkInterrupted();

            try {
                if (firstAttempt) {
                    firstAttempt = false;
                    OpenFileLock lock = tryLockOnce(shared);
                    if (lock != null) {
                        return lock;
                    }
                }

                return lockBlockingOnce(shared);
            } catch (OverlappingFileLockException e) {
                firstAttempt = false;
                OpenFileLock lock = waitForJvmOverlap(shared, OVERLAP_NOTIFICATION_FALLBACK_NANOS);
                if (lock != null) {
                    return lock;
                }
            }
        }
    }

    private OpenFileLock lockBlockingOnce(boolean shared) throws IOException {
        return lockAsynchronously(shared, null);
    }

    /**
     * Acquires the operating-system lock without entering {@link FileChannel#lock(long, long, boolean)}.
     * <p>
     * A contended {@code FileChannel.lock()} cannot be interrupted on Windows; see
     * <a href="https://bugs.openjdk.org/browse/JDK-8152085">JDK-8152085</a>.
     * {@link AsynchronousFileChannel} uses overlapped I/O on Windows, allowing this
     * thread to wait interruptibly on the returned future instead.
     *
     * @param shared whether the lock should be shared or exclusive
     * @param timeoutNanos the timeout in nanoseconds, or {@code null} to wait indefinitely
     * @return the held file lock, or {@code null} when the timeout expires
     * @throws IOException when opening or locking the file fails, or the thread is interrupted
     */
    private @Nullable OpenFileLock lockAsynchronously(boolean shared, @Nullable Long timeoutNanos) throws IOException {
        AsynchronousFileChannel channel = openAsynchronousChannel();
        try {
            Future<FileLock> pendingLock = channel.lock(0L, Long.MAX_VALUE, shared);
            FileLock lock;
            try {
                lock = timeoutNanos == null
                        ? pendingLock.get()
                        : pendingLock.get(timeoutNanos, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                InterruptedIOException failure = interruptedIOException(e);
                closeAfterFailure(channel, failure);
                throw failure;
            } catch (ExecutionException e) {
                IOException failure = lockFailure(e);
                closeAfterFailure(channel, failure);
                throw failure;
            } catch (TimeoutException e) {
                channel.close();
                return null;
            }

            return new OpenFileLock(path, channel, lock, overlapMonitor);
        } catch (RuntimeException | Error e) {
            closeAfterFailure(channel, e);
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
        return new OpenFileLock(path, channel, lock, overlapMonitor);
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
        boolean firstAttempt = true;

        while (true) {
            checkInterrupted();

            try {
                if (firstAttempt) {
                    firstAttempt = false;
                    OpenFileLock lock = tryLockOnce(shared);
                    if (lock != null) {
                        return lock;
                    }
                }

                long remainingNanos = remainingNanos(startNanos, timeoutNanos);
                if (remainingNanos == 0L) {
                    return null;
                }
                return lockAsynchronously(shared, remainingNanos);
            } catch (OverlappingFileLockException e) {
                firstAttempt = false;
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
            }
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

    private IOException lockFailure(ExecutionException exception) {
        Throwable cause = exception.getCause();
        if (cause instanceof IOException) {
            return (IOException) cause;
        }
        return new IOException("Failed to acquire file lock: " + path, cause);
    }

    private static void closeAfterFailure(Channel channel, Throwable failure) {
        try {
            channel.close();
        } catch (IOException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private FileChannel openChannel() throws IOException {
        createParentDirectories();

        return FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
    }

    private AsynchronousFileChannel openAsynchronousChannel() throws IOException {
        createParentDirectories();

        return AsynchronousFileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
    }

    private void createParentDirectories() throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
    }

    @Override
    public String toString() {
        return path.toString();
    }

    static final class OpenFileLock implements Closeable {
        private final Path path;
        private final Channel channel;
        private final FileLock lock;

        /**
         * Monitor notified after releasing {@link #lock}, including when closing its channel fails.
         */
        private final Object overlapMonitor;

        private OpenFileLock(Path path, Channel channel, FileLock lock, Object overlapMonitor) {
            this.path = path;
            this.channel = channel;
            this.lock = lock;
            this.overlapMonitor = overlapMonitor;
        }

        @Override
        public void close() throws IOException {
            try {
                try {
                    lock.release();
                } finally {
                    channel.close();
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
