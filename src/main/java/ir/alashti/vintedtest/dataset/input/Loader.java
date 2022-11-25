package ir.alashti.vintedtest.dataset.input;

import ir.alashti.vintedtest.dataset.Dataset;
import ir.alashti.vintedtest.dataset.Row;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class Loader<V> implements Callable<Dataset<Row>> {
    Path path;
    List<String> header;
    boolean hasHeader;
    String splitter;

    public Loader(Path path, List<String> header, boolean hasHeader, String splitter) {
        this.path = path;
        this.header = header;
        this.hasHeader = hasHeader;
        this.splitter = splitter;
    }

    @Override
    public Dataset<Row> call() {
        Dataset<Row> dataset = new Dataset<>();
        if (header != null) {
            hasHeader = true;
            dataset.setHeader(header);
        }
        try {
            List<String> lines = Files.readAllLines(path);
            List<String> split = new ArrayList<>();
            if (hasHeader) {
                split = new ArrayList<>(Arrays.asList(lines.remove(0).split(splitter)));
                if (header == null)
                    dataset.setHeader(split);
                else
                    updateFileSchema(dataset.getHeader(), split);
            }
            List<String> finalSplit = split;
            List<Row> rows = lines.parallelStream()
                    .map(d -> Row.deserialize(d, finalSplit, splitter))
                    .collect(Collectors.toList());
            dataset.getRows().addAll(rows);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        return dataset;
    }

    private static void updateFileSchema(List<String> schemaHeader, List<String> header) {
        for (int i = 0; i < schemaHeader.size(); i++) {
            String column = schemaHeader.get(i);
            if (!column.equals(header.get(i))) {
                header.add(i, null);
            }
        }
    }
}
