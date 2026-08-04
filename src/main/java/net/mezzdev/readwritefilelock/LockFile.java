package net.mezzdev.readwritefilelock;

import org.jspecify.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Represents a file used for inter-process locking.
 */
final class LockFile {
    private final Path path;

    LockFile(Path path) {
        this.path = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
    }

    /**
     * Locks this file and closes the file channel if the lock cannot be acquired.
     *
     * @param shared whether the lock should be shared or exclusive
     * @return the held file lock
     * @throws IOException when opening or locking the file fails
     */
    OpenFileLock lock(boolean shared) throws IOException {
        var channel = openChannel();
        try {
            return new OpenFileLock(path, channel.lock(0L, Long.MAX_VALUE, shared));
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
        var channel = openChannel();
        FileLock lock;
        try {
            lock = channel.tryLock(0L, Long.MAX_VALUE, shared);
        } catch (OverlappingFileLockException e) {
            channel.close();
            return null;
        } catch (IOException e) {
            channel.close();
            throw e;
        }

        if (lock == null) {
            channel.close();
            return null;
        }
        return new OpenFileLock(path, lock);
    }

    private FileChannel openChannel() throws IOException {
        var parent = path.getParent();
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

        private OpenFileLock(Path path, FileLock lock) {
            this.path = path;
            this.lock = lock;
        }

        @Override
        public void close() throws IOException {
            try {
                lock.release();
            } finally {
                lock.channel().close();
            }
        }

        @Override
        public String toString() {
            return path.toString();
        }
    }
}
