package namespace.model;

/*
  * TODO: try another implementation with a switch for the message body type
  * This switch would have values "text" or "binary" and this would determine
  * If messages are treated as regular text (String) or some binary data that
  * we could choose to encode / decode with base64.
  */

public class Message {
    private String handle;
    private String body;

    public Message(String body) {
        this.body = body;
    }

    public Message(String handle, String body) {
        this.handle = handle;
        this.body = body;
    }

    public String getHandle() {
        return handle;
    }

    public void setHandle(String handle) {
        this.handle = handle;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }
}
