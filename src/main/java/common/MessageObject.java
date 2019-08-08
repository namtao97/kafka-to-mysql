package common;

import java.security.SecureRandom;
import java.util.Date;
import java.util.Random;

public class MessageObject {

    private static final String CHAR_LOWER = "abcdefghijklmnopqrstuvwxyz";
    private static final String CHAR_UPPER = CHAR_LOWER.toUpperCase();
    private static final String DATA = CHAR_LOWER + CHAR_UPPER;

    private String message;
    private long time;


    public MessageObject(String message, long time) {
        this.message = message;
        this.time = time;
    }

    public MessageObject() {}


    public String getMessage() {
        return this.message;
    }


    public long getTime() {
        return this.time;
    }


    public void setMessage(String message) {
        this.message = message;
    }


    public void setTime(long time) {
        this.time = time;
    }


    public static MessageObject getRandomMessage(int length) {
        return new MessageObject(generateRandomString(length), new Date().getTime() / 1000L);
    }


    private static String generateRandomString(int length) {
        if (length < 1) throw new IllegalArgumentException();

        StringBuilder sb = new StringBuilder(length);
        Random random = new SecureRandom();

        for (int i = 0; i < length; i++) {
            int id = random.nextInt(DATA.length());
            char randomChar = DATA.charAt(id);
            sb.append(randomChar);
        }

        return sb.toString();
    }


    @Override
    public String toString() {
        return "Message{" +
                "message=\'" + this.message + "\'" +
                ", time=" + this.time +
                "}";
    }
}
