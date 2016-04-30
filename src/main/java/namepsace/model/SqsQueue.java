package namespace.model;

import java.util.List;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;

/*
 * Amazon SQS version of the MessageQueue.
 * It strives to deliver messages in FIFO order, but does not guarantee it.
 * Correct AWS credentials need to be configured on the system in order to use this.
 */

public class SqsQueue implements MessageQueue {
    private AmazonSQSClient sqs;
    private String queueUrl;

    public SqsQueue(String queueId, AmazonSQSClient sqs) {
        this.sqs = sqs;
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueId);
        queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
    }

    @Override
    public void setVisibilityTimeout(int visibilityTimeout) {
        // Sets the visibility timeout for the whole queue (in seconds)
        sqs.setQueueAttributes(new SetQueueAttributesRequest().withQueueUrl(queueUrl)
                .addAttributesEntry("VisibilityTimeout", Long.toString(visibilityTimeout)));
    }

    @Override
    public void push(namespace.model.Message message) {
        sqs.sendMessage(new SendMessageRequest(queueUrl, message.getBody()));
    }

    @Override
    public namespace.model.Message pull() {
        // By default, MaxNumberOfMessages is set to 1
        ReceiveMessageResult result = sqs.receiveMessage(queueUrl);
        List<com.amazonaws.services.sqs.model.Message> messages = result.getMessages();
        if (messages.isEmpty())
            return null;
        else {
            com.amazonaws.services.sqs.model.Message message = messages.get(0);
            return new namespace.model.Message(message.getReceiptHandle(), message.getBody());
        }
    }

    @Override
    public void delete(String handle) {
        sqs.deleteMessage(new DeleteMessageRequest(queueUrl, handle));
    }

    @Override
    public void close() {
        sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
    }
}
