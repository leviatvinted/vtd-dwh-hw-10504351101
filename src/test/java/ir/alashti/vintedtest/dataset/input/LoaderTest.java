package ir.alashti.vintedtest.dataset.input;

import ir.alashti.vintedtest.dataset.Dataset;
import ir.alashti.vintedtest.dataset.Formats;
import ir.alashti.vintedtest.dataset.Row;
import ir.alashti.vintedtest.dataset.parallel.ParallelDataset;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class LoaderTest {
    ParallelDataset<Row> ds;
    ParallelDataset<Row> dsWithoutSchema;

    @BeforeEach
    void setUp() {
        List<String> clicksHeader = new ArrayList<>();
        clicksHeader.add("date");
        clicksHeader.add("screen");
        clicksHeader.add("user_id");
        clicksHeader.add("click_target");
        ds = LoaderManager.textLoaderManager("data/clicks", clicksHeader, true, Formats.CSV.getSplitter());
        dsWithoutSchema = LoaderManager.textLoaderManager("data/clicks", null, true, Formats.CSV.getSplitter());
    }

    @Test
    void call() {
        int count = 0;
        for (Dataset<Row> rowDataset : ds.getParallelDataset()) {
            count += rowDataset.select("screen").filter(d -> ((Row) d).getValues().get(0) == null).count();
        }
        Assertions.assertEquals(63, count);
        count = 0;
        for (Dataset<Row> rowDataset : dsWithoutSchema.getParallelDataset()) {
            count += rowDataset.select("screen").filter(d -> {
                try {
                    return ((Row) d).getValues().get(0) != null;
                } catch (Exception ex) {
                    return false;
                }
            }).count();
        }
        Assertions.assertEquals(37, count);
    }
}