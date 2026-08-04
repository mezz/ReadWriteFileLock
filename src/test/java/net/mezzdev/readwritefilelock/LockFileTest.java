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
        var path = tempDir.resolve("nested").resolve("..").resolve("lock.file");

        assertEquals(path.toAbsolutePath().normalize().toString(), new LockFile(path).toString());
    }

    @Test
    void lockCreatesParentDirectoriesAndLockFile() throws Exception {
        var lockFile = tempDir.resolve("locks").resolve("nested.lock");
        var lock = new LockFile(lockFile);

        try (var ignored = lock.lock(false)) {
            assertTrue(Files.exists(lockFile));
        }
    }

    @Test
    void exclusiveLockPreventsOverlappingFileLocksInSameJvm() throws Exception {
        var lockFile = tempDir.resolve("exclusive.lock");
        var lock = new LockFile(lockFile);

        try (
                var ignored = lock.lock(false);
                var channel = FileChannel.open(lockFile, StandardOpenOption.READ, StandardOpenOption.WRITE)
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
        var lock = new LockFile(tempDir.resolve("try-overlap.lock"));

        try (var ignored = lock.lock(false)) {
            assertNull(lock.tryLock(false));
            assertNull(lock.tryLock(true));
        }
    }

    @Test
    void directSharedLocksDoNotOverlapInSameJvm() throws Exception {
        var lock = new LockFile(tempDir.resolve("shared-overlap.lock"));

        try (var ignored = lock.lock(true)) {
            assertNull(lock.tryLock(true));
        }
    }

    @Test
    void closeReleasesFileLock() throws Exception {
        var lockFile = tempDir.resolve("release.lock");
        var lock = new LockFile(lockFile);

        try (var ignored = lock.lock(false)) {
            assertTrue(Files.exists(lockFile));
        }

        try (
                var channel = FileChannel.open(lockFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
                var fileLock = channel.tryLock(0L, Long.MAX_VALUE, false)
        ) {
            assertNotNull(fileLock);
        }
    }

    @Test
    void openFileLockToStringReturnsLockFilePath() throws Exception {
        var lockFile = tempDir.resolve("to-string.lock");
        var lock = new LockFile(lockFile);

        try (var heldLock = lock.lock(false)) {
            assertEquals(lockFile.toAbsolutePath().normalize().toString(), heldLock.toString());
        }
    }
}
