package net.mezzdev.readwritefilelock;

import org.jspecify.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A read/write lock backed by a file lock, so it works across processes.
 * <p>
 * This lock has two layers. The JVM lock coordinates threads using this class
 * from the same classloader. The file lock coordinates separate processes and
 * isolated classloaders in the same process.
 * <p>
 * Java does not allow overlapping file locks on the same file in one JVM
 * process. To support multiple readers using the same classloader, read locks
 * share one file lock and keep a holder count for it. Isolated classloaders
 * coordinate overlapping locks through JVM-wide release notifications, with
 * a periodic fallback for non-cooperating same-JVM lock holders.
 */
public final class ReadWriteFileLock {
    /**
     * Classloader-local registry that shares JVM lock state for each normalized path.
     */
    private static final ConcurrentMap<Path, ReadWriteFileLock> LOCKS = new ConcurrentHashMap<>();

    /**
     * The file used to acquire locks between processes.
     */
    private final LockFile processLockFile;

    /**
     * The read/write lock used to coordinate threads using this classloader.
     */
    private final ReentrantReadWriteLock jvmLock = new ReentrantReadWriteLock(true);

    /**
     * Guards process lock state and serializes interruptible file lock acquisition.
     */
    private final ReentrantLock processLockStateLock = new ReentrantLock(true);

    /**
     * The read file lock currently held by readers using this classloader.
     */
    private LockFile.@Nullable OpenFileLock readProcessLock;

    /**
     * The write file lock currently held through this classloader.
     */
    private LockFile.@Nullable OpenFileLock writeProcessLock;

    /**
     * The number of readers using this classloader's {@link #readProcessLock}.
     */
    private int readProcessLockHolders;

    /**
     * The number of local locks using this classloader's {@link #writeProcessLock}.
     */
    private int writeProcessLockHolders;

    /**
     * Returns the read/write file lock for a lock file.
     * <p>
     * The path is converted to an absolute normalized path before lookup.
     * Existing paths and existing parent directories are also resolved to their
     * real paths when possible. Calls for the same normalized path return the
     * same lock instance in this classloader, which avoids Java's overlapping
     * file lock limitation for its callers.
     *
     * @param path the lock file path
     * @return the read/write file lock for the path
     */
    public static ReadWriteFileLock forFile(Path path) {
        Path normalizedLockFile = normalizeLockFile(path);
        return LOCKS.computeIfAbsent(normalizedLockFile, ReadWriteFileLock::new);
    }

    private static Path normalizeLockFile(Path path) {
        Path absolutePath = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        try {
            return absolutePath.toRealPath();
        } catch (NoSuchFileException e) {
            Path parent = absolutePath.getParent();
            if (parent != null) {
                try {
                    return parent.toRealPath().resolve(absolutePath.getFileName()).normalize();
                } catch (IOException ignored) {
                    return absolutePath;
                }
            }
        } catch (IOException ignored) {
            return absolutePath;
        }
        return absolutePath;
    }

    private ReadWriteFileLock(Path path) {
        this.processLockFile = new LockFile(path);
    }

    /**
     * Locks for reading, waiting until the lock is available.
     * <p>
     * Multiple threads and processes may hold read locks at the same time.
     *
     * @return the held lock
     * @throws IOException when locking fails or the thread is interrupted
     */
    public HeldLock lockForRead() throws IOException {
        return lock(true);
    }

    /**
     * Tries to lock for reading without waiting.
     * <p>
     * Multiple threads and processes may hold read locks at the same time.
     *
     * @return the held lock, or {@code null} when the lock is not immediately available
     * @throws IOException when locking fails
     */
    public @Nullable HeldLock tryLockForRead() throws IOException {
        return tryLock(true);
    }

    /**
     * Tries to lock for reading, waiting up to the given timeout.
     * <p>
     * Multiple threads and processes may hold read locks at the same time.
     * The timeout covers both the classloader-local lock and the file lock.
     * A non-positive timeout performs only an immediate attempt.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout
     * @return the held lock, or {@code null} when the timeout expires
     * @throws IOException when locking fails or the thread is interrupted
     */
    public @Nullable HeldLock tryLockForRead(long timeout, TimeUnit unit) throws IOException {
        return tryLock(true, timeout, unit);
    }

    /**
     * Locks for writing, waiting until the lock is available.
     *
     * @return the held lock
     * @throws IOException when locking fails or the thread is interrupted
     */
    public HeldLock lockForWrite() throws IOException {
        return lock(false);
    }

    /**
     * Tries to lock for writing without waiting.
     *
     * @return the held lock, or {@code null} when the lock is not immediately available
     * @throws IOException when locking fails
     */
    public @Nullable HeldLock tryLockForWrite() throws IOException {
        return tryLock(false);
    }

    /**
     * Tries to lock for writing, waiting up to the given timeout.
     * <p>
     * The timeout covers both the classloader-local lock and the file lock.
     * A non-positive timeout performs only an immediate attempt.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout
     * @return the held lock, or {@code null} when the timeout expires
     * @throws IOException when locking fails or the thread is interrupted
     */
    public @Nullable HeldLock tryLockForWrite(long timeout, TimeUnit unit) throws IOException {
        return tryLock(false, timeout, unit);
    }

    private HeldLock lock(boolean read) throws IOException {
        if (!read && jvmLock.getReadHoldCount() > 0 && !jvmLock.isWriteLockedByCurrentThread()) {
            throw new IllegalStateException("Cannot acquire a write lock while the current thread holds a read lock.");
        }

        Lock localLock = read ? jvmLock.readLock() : jvmLock.writeLock();
        try {
            localLock.lockInterruptibly();
        } catch (InterruptedException e) {
            throw interruptedIOException(e);
        }

        LockMode lockMode = null;
        try {
            lockMode = read ? acquireReadLock() : acquireWriteLock();
            return new HeldLock(localLock, lockMode);
        } finally {
            if (lockMode == null) {
                localLock.unlock();
            }
        }
    }

    private @Nullable HeldLock tryLock(boolean read) throws IOException {
        Lock localLock = read ? jvmLock.readLock() : jvmLock.writeLock();
        if (!localLock.tryLock()) {
            return null;
        }

        LockMode lockMode = null;
        try {
            lockMode = read ? tryAcquireReadLock() : tryAcquireWriteLock();
            if (lockMode == null) {
                return null;
            }

            return new HeldLock(localLock, lockMode);
        } finally {
            if (lockMode == null) {
                localLock.unlock();
            }
        }
    }

    private @Nullable HeldLock tryLock(boolean read, long timeout, TimeUnit unit) throws IOException {
        Objects.requireNonNull(unit, "unit");
        if (!read && jvmLock.getReadHoldCount() > 0 && !jvmLock.isWriteLockedByCurrentThread()) {
            return null;
        }

        Timeout lockTimeout = new Timeout(Math.max(0L, unit.toNanos(timeout)));
        Lock localLock = read ? jvmLock.readLock() : jvmLock.writeLock();
        try {
            if (!localLock.tryLock(lockTimeout.remainingNanos(), TimeUnit.NANOSECONDS)) {
                return null;
            }
        } catch (InterruptedException e) {
            throw interruptedIOException(e);
        }

        LockMode lockMode = null;
        try {
            lockMode = read ? tryAcquireReadLock(lockTimeout) : tryAcquireWriteLock(lockTimeout);
            if (lockMode == null) {
                return null;
            }

            return new HeldLock(localLock, lockMode);
        } finally {
            if (lockMode == null) {
                localLock.unlock();
            }
        }
    }

    private LockMode acquireReadLock() throws IOException {
        lockProcessStateInterruptibly();
        try {
            if (writeProcessLock != null) {
                writeProcessLockHolders++;
                return LockMode.WRITE;
            }

            if (readProcessLock == null) {
                readProcessLock = processLockFile.lock(true);
            }
            readProcessLockHolders++;
            return LockMode.READ;
        } finally {
            processLockStateLock.unlock();
        }
    }

    private @Nullable LockMode tryAcquireReadLock() throws IOException {
        if (!processLockStateLock.tryLock()) {
            return null;
        }
        try {
            if (writeProcessLock != null) {
                writeProcessLockHolders++;
                return LockMode.WRITE;
            }

            if (readProcessLock == null) {
                LockFile.OpenFileLock processLock = processLockFile.tryLock(true);
                if (processLock == null) {
                    return null;
                }
                readProcessLock = processLock;
            }
            readProcessLockHolders++;
            return LockMode.READ;
        } finally {
            processLockStateLock.unlock();
        }
    }

    private @Nullable LockMode tryAcquireReadLock(Timeout timeout) throws IOException {
        if (!tryLockProcessState(timeout)) {
            return null;
        }
        try {
            if (writeProcessLock != null) {
                writeProcessLockHolders++;
                return LockMode.WRITE;
            }

            if (readProcessLock == null) {
                LockFile.OpenFileLock processLock = processLockFile.tryLock(true, timeout.remainingNanos());
                if (processLock == null) {
                    return null;
                }
                readProcessLock = processLock;
            }
            readProcessLockHolders++;
            return LockMode.READ;
        } finally {
            processLockStateLock.unlock();
        }
    }

    private LockMode acquireWriteLock() throws IOException {
        lockProcessStateInterruptibly();
        try {
            if (writeProcessLock == null) {
                writeProcessLock = processLockFile.lock(false);
            }
            writeProcessLockHolders++;
            return LockMode.WRITE;
        } finally {
            processLockStateLock.unlock();
        }
    }

    private @Nullable LockMode tryAcquireWriteLock() throws IOException {
        if (!processLockStateLock.tryLock()) {
            return null;
        }
        try {
            if (writeProcessLock == null) {
                LockFile.OpenFileLock processLock = processLockFile.tryLock(false);
                if (processLock == null) {
                    return null;
                }
                writeProcessLock = processLock;
            }
            writeProcessLockHolders++;
            return LockMode.WRITE;
        } finally {
            processLockStateLock.unlock();
        }
    }

    private @Nullable LockMode tryAcquireWriteLock(Timeout timeout) throws IOException {
        if (!tryLockProcessState(timeout)) {
            return null;
        }
        try {
            if (writeProcessLock == null) {
                LockFile.OpenFileLock processLock = processLockFile.tryLock(false, timeout.remainingNanos());
                if (processLock == null) {
                    return null;
                }
                writeProcessLock = processLock;
            }
            writeProcessLockHolders++;
            return LockMode.WRITE;
        } finally {
            processLockStateLock.unlock();
        }
    }

    private void lockProcessStateInterruptibly() throws IOException {
        try {
            processLockStateLock.lockInterruptibly();
        } catch (InterruptedException e) {
            throw interruptedIOException(e);
        }
    }

    private boolean tryLockProcessState(Timeout timeout) throws IOException {
        try {
            return processLockStateLock.tryLock(timeout.remainingNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            throw interruptedIOException(e);
        }
    }

    private InterruptedIOException interruptedIOException(InterruptedException cause) {
        Thread.currentThread().interrupt();
        InterruptedIOException exception = new InterruptedIOException(
                "Interrupted while waiting for file lock: " + processLockFile
        );
        exception.initCause(cause);
        return exception;
    }

    private void releaseProcessLock(LockMode lockMode) throws IOException {
        switch (lockMode) {
            case READ:
                releaseReadLock();
                break;
            case WRITE:
                releaseWriteLock();
                break;
            default:
                throw new AssertionError(lockMode);
        }
    }

    private void releaseReadLock() throws IOException {
        processLockStateLock.lock();
        try {
            readProcessLockHolders--;
            if (readProcessLockHolders < 0) {
                readProcessLockHolders = 0;
                throw new IllegalStateException("Read file lock holder count became negative.");
            }

            if (readProcessLockHolders == 0) {
                LockFile.OpenFileLock lockToClose = readProcessLock;
                if (lockToClose == null) {
                    throw new IllegalStateException("No read file lock is held.");
                }
                readProcessLock = null;
                lockToClose.close();
            }
        } finally {
            processLockStateLock.unlock();
        }
    }

    private void releaseWriteLock() throws IOException {
        processLockStateLock.lock();
        try {
            writeProcessLockHolders--;
            if (writeProcessLockHolders < 0) {
                writeProcessLockHolders = 0;
                throw new IllegalStateException("Write file lock holder count became negative.");
            }

            if (writeProcessLockHolders == 0) {
                LockFile.OpenFileLock lockToClose = writeProcessLock;
                if (lockToClose == null) {
                    throw new IllegalStateException("No write file lock is held.");
                }
                writeProcessLock = null;
                lockToClose.close();
            }
        } finally {
            processLockStateLock.unlock();
        }
    }

    private static final class Timeout {
        private final long startNanos = System.nanoTime();
        private final long timeoutNanos;

        private Timeout(long timeoutNanos) {
            this.timeoutNanos = timeoutNanos;
        }

        private long remainingNanos() {
            if (timeoutNanos == 0L) {
                return 0L;
            }

            long elapsedNanos = System.nanoTime() - startNanos;
            if (elapsedNanos <= 0L) {
                return timeoutNanos;
            }
            return elapsedNanos >= timeoutNanos ? 0L : timeoutNanos - elapsedNanos;
        }
    }

    private enum LockMode {
        READ,
        WRITE
    }

    /**
     * A held read/write file lock.
     * <p>
     * Close the lock to release it. Closing a lock more than once has no effect.
     * A lock must be closed by the same thread that acquired it.
     */
    public final class HeldLock implements Closeable {
        private final Lock localLock;
        private final LockMode lockMode;
        private final Thread ownerThread;
        private boolean closed;

        private HeldLock(
                Lock localLock,
                LockMode lockMode
        ) {
            this.localLock = localLock;
            this.lockMode = lockMode;
            this.ownerThread = Thread.currentThread();
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            if (Thread.currentThread() != ownerThread) {
                throw new IllegalMonitorStateException("HeldLock must be closed by the same thread that acquired it.");
            }
            closed = true;

            try {
                releaseProcessLock(lockMode);
            } finally {
                localLock.unlock();
            }
        }
    }
}
