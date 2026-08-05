package net.mezzdev.readwritefilelock;

import org.jspecify.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A read/write lock backed by a file lock, so it works across processes.
 * <p>
 * This lock has two layers. The JVM lock coordinates threads in this process.
 * The file lock coordinates separate processes.
 * <p>
 * Java does not allow overlapping file locks on the same file in one JVM
 * process. To support multiple readers in this process, read locks share one
 * file lock and keep a holder count for it.
 */
public final class ReadWriteFileLock {
    private static final ConcurrentMap<Path, ReadWriteFileLock> LOCKS = new ConcurrentHashMap<>();

    /**
     * The file used to acquire locks between processes.
     */
    private final LockFile processLockFile;

    /**
     * The read/write lock used to coordinate threads in this JVM.
     */
    private final ReentrantReadWriteLock jvmLock = new ReentrantReadWriteLock(true);

    /**
     * The read file lock currently held by readers in this JVM.
     */
    private LockFile.@Nullable OpenFileLock readProcessLock;

    /**
     * The write file lock currently held by this JVM.
     */
    private LockFile.@Nullable OpenFileLock writeProcessLock;

    /**
     * The number of readers in this JVM using {@link #readProcessLock}.
     */
    private int readProcessLockHolders;

    /**
     * The number of local locks in this JVM using {@link #writeProcessLock}.
     */
    private int writeProcessLockHolders;

    /**
     * Returns the read/write file lock for a lock file.
     * <p>
     * The path is converted to an absolute normalized path before lookup.
     * Existing paths and existing parent directories are also resolved to their
     * real paths when possible. Calls for the same normalized path return the
     * same lock instance in this JVM, which avoids Java's overlapping file lock
     * limitation.
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
        Lock readLock = jvmLock.readLock();
        if (!readLock.tryLock()) {
            return null;
        }

        LockMode lockMode = null;
        try {
            lockMode = tryAcquireReadLock();
            if (lockMode == null) {
                return null;
            }

            return new HeldLock(readLock, lockMode);
        } finally {
            if (lockMode == null) {
                readLock.unlock();
            }
        }
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
        Lock writeLock = jvmLock.writeLock();
        if (!writeLock.tryLock()) {
            return null;
        }

        LockMode lockMode = null;
        try {
            lockMode = tryAcquireWriteLock();
            if (lockMode == null) {
                return null;
            }

            return new HeldLock(writeLock, lockMode);
        } finally {
            if (lockMode == null) {
                writeLock.unlock();
            }
        }
    }

    private HeldLock lock(boolean read) throws IOException {
        if (!read && jvmLock.getReadHoldCount() > 0 && !jvmLock.isWriteLockedByCurrentThread()) {
            throw new IllegalStateException("Cannot acquire a write lock while the current thread holds a read lock.");
        }

        Lock localLock = read ? jvmLock.readLock() : jvmLock.writeLock();
        try {
            localLock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for file lock.", e);
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

    private LockMode acquireReadLock() throws IOException {
        synchronized (this) {
            if (writeProcessLock != null) {
                writeProcessLockHolders++;
                return LockMode.WRITE;
            }

            if (readProcessLock == null) {
                readProcessLock = processLockFile.lock(true);
            }
            readProcessLockHolders++;
            return LockMode.READ;
        }
    }

    private @Nullable LockMode tryAcquireReadLock() throws IOException {
        synchronized (this) {
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
        }
    }

    private LockMode acquireWriteLock() throws IOException {
        synchronized (this) {
            if (writeProcessLock == null) {
                writeProcessLock = processLockFile.lock(false);
            }
            writeProcessLockHolders++;
            return LockMode.WRITE;
        }
    }

    private @Nullable LockMode tryAcquireWriteLock() throws IOException {
        synchronized (this) {
            if (writeProcessLock == null) {
                LockFile.OpenFileLock processLock = processLockFile.tryLock(false);
                if (processLock == null) {
                    return null;
                }
                writeProcessLock = processLock;
            }
            writeProcessLockHolders++;
            return LockMode.WRITE;
        }
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
        synchronized (this) {
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
        }
    }

    private void releaseWriteLock() throws IOException {
        synchronized (this) {
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
