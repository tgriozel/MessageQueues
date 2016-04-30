package namespace.service;

import java.util.HashMap;
import java.util.Map;

import namespace.model.MessageQueue;
import namespace.model.InMemoryQueue;

public class InMemoryQueueService implements QueueService {
    private Map<String, InMemoryQueue> idToQueue;

    public InMemoryQueueService() {
        idToQueue = new HashMap<>();
    }

    @Override
    public MessageQueue getQueue(String queueId) {
        InMemoryQueue queue = idToQueue.get(queueId);
        if (queue == null) {
            queue = new InMemoryQueue();
            idToQueue.put(queueId, queue);
        }
        return queue;
    }

    @Override
    public MessageQueue deleteQueue(String queueId) {
        InMemoryQueue queue = idToQueue.get(queueId);
        if (queue != null) {
            queue.close();
            idToQueue.remove(queueId);
        }
        return queue;
    }

    @Override
    public void shutdown() {
        for (InMemoryQueue queue : idToQueue.values()) {
            queue.close();
        }
    }
}
