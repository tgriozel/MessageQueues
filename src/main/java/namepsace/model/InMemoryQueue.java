package namespace.model;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/*
 * In memory version of the MessageQueue.
 * It strives to deliver messages in FIFO order, but does not guarantee it.
 * Then, the only operation we have to make thread safe is the ID computation.
 */

public class InMemoryQueue implements MessageQueue {
    private Deque<Message> internalDequeue;
    private Map<String, Thread> timeoutThreads;
    private int visibilityTimeout = 10;
    private AtomicLong messageId = new AtomicLong(0);

    public InMemoryQueue() {
        internalDequeue = new ConcurrentLinkedDeque<>();
        timeoutThreads = new ConcurrentHashMap<>();
    }

    @Override
    public void setVisibilityTimeout(int visibilityTimeout) {
        this.visibilityTimeout = visibilityTimeout;
    }

    @Override
    public void push(Message message) {
        message.setHandle(String.valueOf(messageId.getAndIncrement()));
        internalDequeue.addFirst(message);
    }

    @Override
    public Message pull() {
        Message message = internalDequeue.pollLast();
        if(message == null)
            return null;
        Thread t = new Thread(new delayedInsert(internalDequeue, message, visibilityTimeout));
        timeoutThreads.put(message.getHandle(), t);
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
        for (Thread thread : timeoutThreads.values())
            thread.interrupt();
        internalDequeue.clear();
    }

    private class delayedInsert implements Runnable {
        private Deque<Message> dequeue;
        private Message message;
        private int timeout;

        public delayedInsert(Deque<Message> dequeue, Message message, int timeout) {
            this.dequeue = dequeue;
            this.message = message;
            this.timeout = timeout;
        }

        @Override
        public void run() {
            try { Thread.sleep(timeout * 1000); }
            catch (InterruptedException e) { return; }
            dequeue.addLast(message);
        }
    }
}
