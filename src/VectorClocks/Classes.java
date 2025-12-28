package VectorClocks;

public class Classes {
    static class Message {
        final int senderId;
        final String payload;
        final int[] vectorClock;

        Message(int senderId, String payload, int[] vectorClock) {
            this.senderId = senderId;
            this.payload = payload;
            this.vectorClock = vectorClock;
        }
    }

    static class LogEntry {
        final String text;
        final int[] vectorClock;

        LogEntry(String text, int[] vectorClock) {
            this.text = text;
            this.vectorClock = vectorClock;
        }
    }
}
