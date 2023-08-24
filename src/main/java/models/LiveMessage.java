package models;

public class LiveMessage {

    private double value;
    private String receivedTime;

    public LiveMessage(double value, String receivedTime) {
        this.value = value;
        this.receivedTime = receivedTime;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public String getReceivedTime() {
        return receivedTime;
    }

    public void setReceivedTime(String receivedTime) {
        this.receivedTime = receivedTime;
    }

    @Override
    public String toString() {
        return "LiveMessage{" +
                "value=" + value +
                ", receivedTime='" + receivedTime + '\'' +
                '}';
    }
}
