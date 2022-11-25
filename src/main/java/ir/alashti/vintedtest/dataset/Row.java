package ir.alashti.vintedtest.dataset;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Row {
    List<Object> values = new ArrayList<>();

    public Row() {
    }

    public Row(String[] split) {
        values = new ArrayList<>(Arrays.asList(split));
    }

    public Row(List<Object> values) {
        this.values = values;
    }

    public List<Object> getValues() {
        return values;
    }

    public void add(int index, Object element) {
        getValues().add(index, element);
    }

    public void add(Object element) {
        getValues().add(element);
    }

    public static Row deserialize(String line, List<String> header, String splitter) {
        String[] split = line.split(splitter);
        Row row = new Row();
        int j = 0;
        for (int i = 0; i < header.size(); i++) {
            if (header.get(i) == null)
                row.add(null);
            else
                row.add(split[j++]);
        }
        return row;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        Iterator<Object> iterator = getValues().iterator();
        while (iterator.hasNext()) {
            Object next = iterator.next();
            builder.append(next);
            if (iterator.hasNext())
                builder.append(",");
        }
        return builder.toString();
    }
}
