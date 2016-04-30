package namespace.service;

import java.util.HashMap;
import java.util.Map;

import namespace.model.MessageQueue;
import namespace.model.FileQueue;

public class FileQueueService implements QueueService {
    private Map<String, FileQueue> idToQueue;
    private String queuesDirName = "./";

    public FileQueueService() {
        idToQueue = new HashMap<>();
    }

    public FileQueueService(String queuesDirName) {
        this();
        this.queuesDirName = queuesDirName;
    }

    @Override
    public MessageQueue getQueue(String queueId) {
        FileQueue queue = idToQueue.get(queueId);
        if (queue == null) {
            queue = new FileQueue(queueId, queuesDirName);
            idToQueue.put(queueId, queue);
        }
        return queue;
    }

    @Override
    public MessageQueue deleteQueue(String queueId) {
        FileQueue queue = idToQueue.get(queueId);
        if (queue != null) {
            queue.close();
            idToQueue.remove(queueId);
        }
        return queue;
    }

    @Override
    public void shutdown() {
        for (FileQueue queue : idToQueue.values()) {
            queue.close();
        }
    }
}
