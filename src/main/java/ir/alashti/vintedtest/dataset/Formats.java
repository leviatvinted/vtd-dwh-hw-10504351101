package ir.alashti.vintedtest.dataset;

public enum Formats {
    CSV("csv", ","),
    TSV("tsv", "\t");

    String name;
    String splitter;

    Formats(String name, String splitter) {
        this.name = name;
        this.splitter = splitter;
    }

    public String getSplitter() {
        return splitter;
    }
}
