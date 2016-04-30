package namespace.service;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.sqs.AmazonSQSClient;
import namespace.model.MessageQueue;
import namespace.model.SqsQueue;

public class SqsQueueService implements QueueService {
    private AmazonSQSClient sqs;
    private Map<String, SqsQueue> idToQueue;

    public SqsQueueService(AmazonSQSClient sqs) {
        idToQueue = new HashMap<>();
        this.sqs = sqs;
    }

    @Override
    public MessageQueue getQueue(String queueId) {
        SqsQueue queue = idToQueue.get(queueId);
        if (queue == null) {
            queue = new SqsQueue(queueId, sqs);
            idToQueue.put(queueId, queue);
        }
        return queue;
    }

    @Override
    public MessageQueue deleteQueue(String queueId) {
        SqsQueue queue = idToQueue.get(queueId);
        if (queue != null) {
            queue.close();
            idToQueue.remove(queueId);
        }
        return queue;
    }

    @Override
    public void shutdown() {
        for (SqsQueue queue : idToQueue.values()) {
            queue.close();
        }
    }
}
