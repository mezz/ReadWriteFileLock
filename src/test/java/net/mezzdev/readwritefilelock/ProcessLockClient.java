package net.mezzdev.readwritefilelock;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

final class ProcessLockClient {
    private static final Duration PROCESS_TIMEOUT = Duration.ofSeconds(5);

    private ProcessLockClient() {
    }

    static Result run(Command command, Path lockFile) throws Exception {
        Process process = start(command, lockFile);

        try {
            if (!process.waitFor(PROCESS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                throw new AssertionError("Timed out waiting for process lock client.");
            }

            String output = readProcessOutput(process);
            if (process.exitValue() != 0) {
                throw new AssertionError(output);
            }
            return Result.parse(output);
        } finally {
            process.destroyForcibly();
        }
    }

    static HeldProcessLock hold(Command command, Path lockFile) throws Exception {
        if (command != Command.HOLD_READ && command != Command.HOLD_WRITE) {
            throw new IllegalArgumentException("Expected a hold command, got: " + command);
        }

        Process process = start(command, lockFile);
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8)
        );
        FutureTask<String> firstLine = new FutureTask<>(reader::readLine);
        Thread readerThread = new Thread(firstLine, "process-lock-client-output");
        readerThread.setDaemon(true);
        readerThread.start();

        try {
            String output = firstLine.get(PROCESS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (!"LOCKED".equals(output)) {
                throw new AssertionError("Process lock client did not acquire its lock: " + output);
            }
            return new HeldProcessLock(process, reader);
        } catch (Exception | Error e) {
            process.destroyForcibly();
            reader.close();
            throw e;
        }
    }

    private static Process start(Command command, Path lockFile) throws IOException {
        Path javaExecutable = Paths.get(
                System.getProperty("java.home"),
                "bin",
                isWindows() ? "java.exe" : "java"
        );
		return new ProcessBuilder(
				javaExecutable.toString(),
				"-cp",
				System.getProperty("java.class.path"),
				ProcessLockClient.class.getName(),
				command.argument(),
				lockFile.toString()
		)
				.redirectErrorStream(true)
				.start();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: ProcessLockClient <command> <lockFile>");
        }

        Command command = Command.parse(args[0]);
        Path lockFile = Paths.get(args[1]);
        ReadWriteFileLock lock = ReadWriteFileLock.forFile(lockFile);

        switch (command) {
            case TRY_READ:
                tryRead(lock);
                break;
            case TRY_WRITE:
                tryWrite(lock);
                break;
            case HOLD_READ:
                holdRead(lock);
                break;
            case HOLD_WRITE:
                holdWrite(lock);
                break;
            default:
                throw new IllegalArgumentException("Unknown command: " + command);
        }
    }

    private static void tryRead(ReadWriteFileLock lock) throws Exception {
        ReadWriteFileLock.HeldLock heldLock = lock.tryLockForRead();
        if (heldLock == null) {
            System.out.println(Result.BUSY);
            return;
        }

        try (ReadWriteFileLock.HeldLock ignored = heldLock) {
            System.out.println(Result.LOCKED);
        }
    }

    private static void tryWrite(ReadWriteFileLock lock) throws Exception {
        ReadWriteFileLock.HeldLock heldLock = lock.tryLockForWrite();
        if (heldLock == null) {
            System.out.println(Result.BUSY);
            return;
        }

        try (ReadWriteFileLock.HeldLock ignored = heldLock) {
            System.out.println(Result.LOCKED);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void holdRead(ReadWriteFileLock lock) throws Exception {
        try (ReadWriteFileLock.HeldLock ignored = lock.lockForRead()) {
            System.out.println("LOCKED");
            System.out.flush();
            System.in.read();
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void holdWrite(ReadWriteFileLock lock) throws Exception {
        try (ReadWriteFileLock.HeldLock ignored = lock.lockForWrite()) {
            System.out.println("LOCKED");
            System.out.flush();
            System.in.read();
        }
    }

    private static String readProcessOutput(Process process) throws IOException {
        try (
                InputStreamReader input = new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8);
                BufferedReader reader = new BufferedReader(input)
        ) {
            return readProcessOutput(reader);
        }
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").startsWith("Windows");
    }

    static final class HeldProcessLock implements AutoCloseable {
        private final Process process;
        private final BufferedReader reader;
        private boolean closed;

        private HeldProcessLock(Process process, BufferedReader reader) {
            this.process = process;
            this.reader = reader;
        }

        @Override
        public void close() throws Exception {
            if (closed) {
                return;
            }
            closed = true;

            try {
                try (OutputStream processInput = process.getOutputStream()) {
                    processInput.write(0);
                    processInput.flush();
                }

                if (!process.waitFor(PROCESS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    throw new AssertionError("Timed out releasing process lock client.");
                }

                String output = readProcessOutput(reader);
                if (process.exitValue() != 0) {
                    throw new AssertionError(output);
                }
            } finally {
                process.destroyForcibly();
                reader.close();
            }
        }
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
            for (Command command : values()) {
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
            String trimmedOutput = output.trim();
            try {
                return valueOf(trimmedOutput);
            } catch (IllegalArgumentException e) {
                throw new AssertionError("Unexpected process lock client output: " + trimmedOutput, e);
            }
        }
    }

    private static String readProcessOutput(BufferedReader reader) throws IOException {
        StringBuilder output = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            if (output.length() > 0) {
                output.append(System.lineSeparator());
            }
            output.append(line);
        }
        return output.toString();
    }
}
