package net.mezzdev.readwritefilelock;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

final class ProcessLockClient {
    private static final Duration PROCESS_TIMEOUT = Duration.ofSeconds(5);

    private ProcessLockClient() {
    }

    static Result run(Command command, Path lockFile) throws Exception {
        var javaExecutable = Path.of(
                System.getProperty("java.home"),
                "bin",
                isWindows() ? "java.exe" : "java"
        );
        var process = new ProcessBuilder(
                javaExecutable.toString(),
                "-cp",
                System.getProperty("java.class.path"),
                ProcessLockClient.class.getName(),
                command.argument(),
                lockFile.toString()
        )
                .redirectErrorStream(true)
                .start();

        try {
            if (!process.waitFor(PROCESS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                throw new AssertionError("Timed out waiting for process lock client.");
            }

            var output = readProcessOutput(process);
            if (process.exitValue() != 0) {
                throw new AssertionError(output);
            }
            return Result.parse(output);
        } finally {
            process.destroyForcibly();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: ProcessLockClient <command> <lockFile>");
        }

        var command = Command.parse(args[0]);
        var lockFile = Path.of(args[1]);
        var lock = ReadWriteFileLock.forFile(lockFile);

        switch (command) {
            case TRY_READ -> tryRead(lock);
            case TRY_WRITE -> tryWrite(lock);
            case HOLD_READ -> holdRead(lock);
            case HOLD_WRITE -> holdWrite(lock);
            default -> throw new IllegalArgumentException("Unknown command: " + command);
        }
    }

    private static void tryRead(ReadWriteFileLock lock) throws Exception {
        var heldLock = lock.tryLockForRead();
        if (heldLock == null) {
            System.out.println(Result.BUSY);
            return;
        }

        try (heldLock) {
            System.out.println(Result.LOCKED);
        }
    }

    private static void tryWrite(ReadWriteFileLock lock) throws Exception {
        var heldLock = lock.tryLockForWrite();
        if (heldLock == null) {
            System.out.println(Result.BUSY);
            return;
        }

        try (heldLock) {
            System.out.println(Result.LOCKED);
        }
    }

    private static void holdRead(ReadWriteFileLock lock) throws Exception {
        try (var ignored = lock.lockForRead()) {
            System.out.println("LOCKED");
            System.out.flush();
            System.in.read();
        }
    }

    private static void holdWrite(ReadWriteFileLock lock) throws Exception {
        try (var ignored = lock.lockForWrite()) {
            System.out.println("LOCKED");
            System.out.flush();
            System.in.read();
        }
    }

    private static String readProcessOutput(Process process) throws IOException {
        try (
                var input = new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8);
                var reader = new BufferedReader(input)
        ) {
            var output = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (!output.isEmpty()) {
                    output.append(System.lineSeparator());
                }
                output.append(line);
            }
            return output.toString();
        }
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").startsWith("Windows");
    }

    enum Command {
        TRY_READ("try-read"),
        TRY_WRITE("try-write"),
        HOLD_READ("hold-read"),
        HOLD_WRITE("hold-write");

        private final String argument;

        Command(String argument) {
            this.argument = argument;
        }

        String argument() {
            return argument;
        }

        static Command parse(String argument) {
            for (var command : values()) {
                if (command.argument.equals(argument)) {
                    return command;
                }
            }
            throw new IllegalArgumentException("Unknown command: " + argument);
        }
    }

    enum Result {
        LOCKED,
        BUSY;

        static Result parse(String output) {
            var trimmedOutput = output.strip();
            try {
                return valueOf(trimmedOutput);
            } catch (IllegalArgumentException e) {
                throw new AssertionError("Unexpected process lock client output: " + trimmedOutput, e);
            }
        }
    }
}
