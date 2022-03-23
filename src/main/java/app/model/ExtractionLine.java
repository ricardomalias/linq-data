package app.model;

import java.util.List;

public class ExtractionLine {

    private final String lineNumber;
    private final List<Double> gigabytes;
    private final int dropped;
    private final int underneath;
    private final int percentage;

    public ExtractionLine(String lineNumber, List<Double> gigabytes, int dropped, int underneath, int percentage) {
        this.lineNumber = lineNumber;
        this.gigabytes = gigabytes;
        this.dropped = dropped;
        this.underneath = underneath;
        this.percentage = percentage;
    }

    public String getLineNumber() {
        return lineNumber;
    }

    public List<Double> getGigabytes() {
        return gigabytes;
    }

    public int getDropped() {
        return dropped;
    }

    public int getUnderneath() {
        return underneath;
    }

    public int getPercentage() {
        return percentage;
    }
}
