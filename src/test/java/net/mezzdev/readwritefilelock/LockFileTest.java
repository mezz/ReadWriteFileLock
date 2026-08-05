package net.mezzdev.readwritefilelock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LockFileTest {
    @TempDir
    Path tempDir;

    @Test
    void constructorRejectsNullPath() {
        assertThrows(NullPointerException.class, () -> new LockFile(null));
    }

    @Test
    void toStringReturnsAbsoluteNormalizedPath() {
        Path path = tempDir.resolve("nested").resolve("..").resolve("lock.file");

        assertEquals(path.toAbsolutePath().normalize().toString(), new LockFile(path).toString());
    }

    @Test
    void lockCreatesParentDirectoriesAndLockFile() throws Exception {
        Path lockFile = tempDir.resolve("locks").resolve("nested.lock");
        LockFile lock = new LockFile(lockFile);

        try (LockFile.OpenFileLock ignored = lock.lock(false)) {
            assertTrue(Files.exists(lockFile));
        }
    }

    @Test
    void exclusiveLockPreventsOverlappingFileLocksInSameJvm() throws Exception {
        Path lockFile = tempDir.resolve("exclusive.lock");
        LockFile lock = new LockFile(lockFile);

        try (
                LockFile.OpenFileLock ignored = lock.lock(false);
                FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.READ, StandardOpenOption.WRITE)
        ) {
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
        LockFile lock = new LockFile(tempDir.resolve("try-overlap.lock"));

        try (LockFile.OpenFileLock ignored = lock.lock(false)) {
            assertNull(lock.tryLock(false));
            assertNull(lock.tryLock(true));
        }
    }

    @Test
    void directSharedLocksDoNotOverlapInSameJvm() throws Exception {
        LockFile lock = new LockFile(tempDir.resolve("shared-overlap.lock"));

        try (LockFile.OpenFileLock ignored = lock.lock(true)) {
            assertNull(lock.tryLock(true));
        }
    }

    @Test
    void closeReleasesFileLock() throws Exception {
        Path lockFile = tempDir.resolve("release.lock");
        LockFile lock = new LockFile(lockFile);

        try (LockFile.OpenFileLock ignored = lock.lock(false)) {
            assertTrue(Files.exists(lockFile));
        }

        try (
                FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
                java.nio.channels.FileLock fileLock = channel.tryLock(0L, Long.MAX_VALUE, false)
        ) {
            assertNotNull(fileLock);
        }
    }

    @Test
    void openFileLockToStringReturnsLockFilePath() throws Exception {
        Path lockFile = tempDir.resolve("to-string.lock");
        LockFile lock = new LockFile(lockFile);

        try (LockFile.OpenFileLock heldLock = lock.lock(false)) {
            assertEquals(lockFile.toAbsolutePath().normalize().toString(), heldLock.toString());
        }
    }
}
