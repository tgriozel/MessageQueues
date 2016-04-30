package namespace.model;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/*
 * File version of the MessageQueue.
 * It strives to deliver messages in FIFO order, but does not guarantee it.
 * Then, the only operation we have to make thread safe is the ID computation.
 * This message queue can be shared with other processes.
 * The other operations including files are made "process-safe" by lock() and unlock().
 * Each FileQueue corresponds to a file.
 * Each line is a message preceeded by its handle.
 */

public class FileQueue implements MessageQueue {
    private Map<String, Thread> timeoutThreads;
    private Thread confWatchingThread;
    private File queue;
    private File lock;
    private File timeoutFile;
    private char separator = '|';
    private int visibilityTimeout = 10;
    private long processId = 0;
    private AtomicLong messageId = new AtomicLong(0);

    public class ConfWatchingRunnable implements Runnable {
        private String confDirName;

        public ConfWatchingRunnable(String confDirName) {
            this.confDirName = confDirName;
        }

        // This will watch the timeout file and ensure that timeout is shared among processes
        @Override
        public void run() {
            Path path = Paths.get(confDirName);
            FileSystem fs = path.getFileSystem();
            try (WatchService service = fs.newWatchService()) {
                path.register(service, StandardWatchEventKinds.ENTRY_MODIFY);
                while (true) {
                    WatchKey watchKey = service.take();
                    for(WatchEvent<?> watchEvent : watchKey.pollEvents()) {
                        Path changed = (Path) watchEvent.context();
                        if (changed.endsWith("timeout"))
                            readVisibilityTimeout();
                    }
                    watchKey.reset();
                }
            }
            catch (Exception e) {}
        }
    }

    public FileQueue(String queueId, String dirName) {
        timeoutThreads = new ConcurrentHashMap<>();
        queue = new File(dirName + queueId);
        lock = new File(dirName + ".lock");

        // Let's make sure the configuration directory exists
        new File(dirName + ".conf").mkdir();

        timeoutFile = new File(dirName + ".conf/timeout");
        if(timeoutFile.exists() && timeoutFile.isFile())
            readVisibilityTimeout();
        else
            setVisibilityTimeout(visibilityTimeout);

        lock();
        File processFile = new File(dirName + ".conf/process");
        if(processFile.exists() && processFile.isFile()) {
            List<String> lines = readLines(processFile, false);
            if (!lines.isEmpty())
                processId = Long.valueOf(lines.get(0)) + 1;
        }
        writeLines(processFile, Collections.singletonList(String.valueOf(processId)), false, false);
        unlock();

        confWatchingThread = new Thread(new ConfWatchingRunnable(dirName));
        confWatchingThread.start();
    }

    private void lock() {
        // File system lock, leveraging the atomicity of the POSIX mkdir
        while (!lock.mkdir()) {
            try { Thread.sleep(50); }
            catch(InterruptedException e) {}
        }
    }

    private void unlock() {
        lock.delete();
    }

    private boolean writeLines(File file, List<String> lines, boolean lock, boolean append) {
        if (lock)
            lock();
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(new BufferedWriter(new FileWriter(file, append)));
            for (String line : lines)
                writer.println(line);
        }
        catch (IOException e) {
            return false;
        }
        finally {
            if (writer != null) {
                writer.close();
            }
            if(lock)
                unlock();
        }
        return true;
    }

    private List<String> readLines(File file, boolean lock) {
        if (lock)
            lock();
        Scanner scanner = null;
        try {
            scanner = new Scanner(file);
            List<String> result = new ArrayList<>();
            while (scanner.hasNext())
                result.add(scanner.nextLine());
            return result;
        }
        catch(IOException e) {
            return Collections.emptyList();
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
            if (lock)
                unlock();
        }
    }

    private void readVisibilityTimeout() {
        // The timeoutFile file will always contain a timeout
        visibilityTimeout = Integer.parseInt(readLines(timeoutFile, false).get(0));
    }

    @Override
    public void setVisibilityTimeout(int visibilityTimeout) {
        this.visibilityTimeout = visibilityTimeout;
        String line = String.valueOf(visibilityTimeout);
        writeLines(timeoutFile, Collections.singletonList(line), true, false);
    }

    @Override
    public void push(Message message) {
        message.setHandle(processId + "_" + messageId.getAndIncrement());
        String line = message.getHandle() + separator + message.getBody();
        writeLines(queue, Collections.singletonList(line), true, true);
    }

    // The consuming process also handles the visibility timeout, this is potentially a problem
    @Override
    public Message pull() {
        lock();
        List<String> allLines = readLines(queue, false);
        if (allLines == null || allLines.isEmpty()) {
            unlock();
            return null;
        }
        String line = allLines.get(allLines.size() - 1);
        writeLines(queue, allLines.subList(0, allLines.size() - 1), false, false);
        unlock();

        int delimiterPos = line.indexOf(separator);
        String handle = line.substring(0, delimiterPos);
        String body = line.substring(delimiterPos + 1);
        Message message = new Message(handle, body);
        Thread t = new Thread(new delayedInsert(this, message, visibilityTimeout));
        timeoutThreads.put(handle, t);
        t.start();
        return message;
    }

    @Override
    public void delete(String handle) {
        Thread thread = timeoutThreads.get(handle);
        if (thread == null)
            return;
        thread.interrupt();
        timeoutThreads.remove(handle);
    }

    @Override
    public void close() {
        confWatchingThread.interrupt();
        for (Thread thread : timeoutThreads.values())
            thread.interrupt();
        queue.delete();
    }

    private class delayedInsert implements Runnable {
        private FileQueue queue;
        private Message message;
        private int timeout;

        public delayedInsert(FileQueue queue, Message message, int timeout) {
            this.queue = queue;
            this.message = message;
            this.timeout = timeout;
        }

        @Override
        public void run() {
            try { Thread.sleep(timeout * 1000); }
            catch (InterruptedException e) { return; }
            queue.push(message);
        }
    }
}
