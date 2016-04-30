package namespace.service;

import namespace.model.MessageQueue;

public interface QueueService {
    public MessageQueue getQueue(String queueId);
    public MessageQueue deleteQueue(String queueId);
    public void shutdown();
}
