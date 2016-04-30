package namespace;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import namespace.model.Message;
import namespace.model.MessageQueue;
import namespace.service.QueueService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class QueueTest {
    protected static QueueService service;
    private static final int shortTimeoutInSeconds = 1;
    private static final int longTimeoutInSeconds = 10;
    private static final String testQueueId = "testQueue";
    private static final String testMsg1 = "This is test message 1";
    private static final String testMsg2 = "This is test message 2";

    @BeforeClass
    public static void testInit() {
        // Method has to be static (because of BeforeClass) but cannot be abstract
        Assert.assertTrue(service != null);
    }

    @AfterClass
    public static void testTeardown() {
        service.shutdown();
    }

    private class OneMessageConsumer implements Callable<Void> {
        MessageQueue queue;

        public OneMessageConsumer(MessageQueue queue) {
            this.queue = queue;
        }

        @Override
        public Void call() {
            Message message;
            do {
                message = queue.pull();
            } while(message == null);
            queue.delete(message.getHandle());
            return null;
        }
    }

    private class OneMessageProducer implements Callable<Void> {
        MessageQueue queue;

        public OneMessageProducer(MessageQueue queue) {
            this.queue = queue;
        }

        @Override
        public Void call() {
            queue.push(new Message(testMsg1));
            return null;
        }
    }

    @Test
    public void testRegularSendAndReceive() {
        MessageQueue queue = service.getQueue(testQueueId);
        queue.setVisibilityTimeout(longTimeoutInSeconds);

        queue.push(new Message(testMsg1));
        queue.push(new Message(testMsg2));

        Message msg1 = queue.pull();
        Message msg2 = queue.pull();
        Assert.assertTrue(msg1 != null && msg2 != null);
        queue.delete(msg1.getHandle());
        queue.delete(msg2.getHandle());

        Assert.assertTrue(!msg1.getBody().equals(msg2.getBody()));
        Assert.assertTrue(testMsg1.equals(msg1.getBody()) || (testMsg1.equals(msg2.getBody())));
        Assert.assertTrue(testMsg2.equals(msg1.getBody()) || (testMsg2.equals(msg2.getBody())));
    }

    @Test
    public void testCorrectMessageDeletion() {
        MessageQueue queue = service.getQueue(testQueueId);
        queue.setVisibilityTimeout(longTimeoutInSeconds);

        queue.push(new Message(testMsg1));
        Message msg = queue.pull();
        Assert.assertTrue(msg != null);

        queue.delete(msg.getHandle());
        Assert.assertTrue(queue.pull() == null);
    }

    @Test
    public void testMessageVisibilityTimeout() {
        MessageQueue queue = service.getQueue(testQueueId);
        queue.setVisibilityTimeout(shortTimeoutInSeconds);

        queue.push(new Message(testMsg1));
        Assert.assertTrue(queue.pull() != null);
        Assert.assertTrue(queue.pull() == null);

        // Let's wait for the timeout to expire
        try { Thread.sleep(shortTimeoutInSeconds * 1000 * 2); }
        catch (InterruptedException e) { e.printStackTrace(); }
        Message msg = queue.pull();
        Assert.assertTrue(msg != null);
        queue.delete(msg.getHandle());
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException {
        MessageQueue queue = service.getQueue(testQueueId);
        queue.setVisibilityTimeout(longTimeoutInSeconds);

        // Create as many producers as consumers
        int loopCount = 100;
        List<Callable<Void>> callables = new ArrayList<>(loopCount * 2);
        for (short i = 0; i < loopCount; ++i) {
            callables.add(new OneMessageConsumer(queue));
            callables.add(new OneMessageProducer(queue));
        }

        // Call them all and wait a bit so they can finish
        ExecutorService executor = Executors.newWorkStealingPool();
        executor.invokeAll(callables);
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // Let's check that all threads finished and queue is now empty
        Assert.assertTrue(queue.pull() == null);
    }
}
