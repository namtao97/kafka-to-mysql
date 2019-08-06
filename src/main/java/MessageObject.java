public class MessageObject {
    private String msg;
    private long time;

    public MessageObject(String message, long time) {
        this.msg = message;
        this.time = time;
    }

    public MessageObject() {

    }

    public String getMessage() {
        return this.msg;
    }

    public long getTime() {
        return this.time;
    }

    public void setMessage(String message) {
        this.msg = message;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "Message{" +
                "msg=\'" + this.msg + "\'" +
                ", time=" + this.time +
                "}";
    }
}
