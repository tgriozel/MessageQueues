package namespace.model;

public interface MessageQueue {
    void setVisibilityTimeout(int visibilityTimeout);
    void push(Message message);
    Message pull();
    void delete(String handle);
    void close();
}
