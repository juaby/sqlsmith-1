package org.libsmith.sql;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
 * @created 26.02.2015 13:27
 */
public class SQLInsertValues implements SQLFragment {

    public static final SQLFragment DEFAULT = SQLFragment.Impl.of("DEFAULT");

    private StringBuilder stringBuilder = new StringBuilder();
    private List<Object> parameters = new ArrayList<>();
    private final String whitespace;
    private final boolean inside;

    public SQLInsertValues() {
        this("\n    ", true);
    }

    public SQLInsertValues(String whitespace, boolean inside) {
        this.whitespace = whitespace;
        this.inside = inside;
    }

    public SQLInsertValues values(@Nonnull Object ... values) {
        if (stringBuilder.length() > 0) {
            if (inside) {
                stringBuilder.append("),").append(whitespace).append("(");
            }
            else {
                stringBuilder.append(",");
            }
        }
        if (!inside) {
            stringBuilder.append(whitespace).append("(");
        }
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                stringBuilder.append(", ");
            }
            Object value = values[i];
            if (value instanceof SQLFragment) {
                SQLFragment sqlFragment = (SQLFragment) value;
                stringBuilder.append(sqlFragment.getFragment());
                parameters.addAll(sqlFragment.getParameters());
            }
            else {
                stringBuilder.append("?");
                parameters.add(value instanceof SQLValue ? ((SQLValue) value).getSQLValueEntity() : value);
            }
        }
        if (!inside) {
            stringBuilder.append(")");
        }
        return this;
    }

    @Override
    public @Nonnull String getFragment() {
        if (stringBuilder.length() == 0) {
            throw new IllegalStateException();
        }
        return stringBuilder.toString();
    }

    @Override
    public @Nonnull List<Object> getParameters() {
        return parameters;
    }

    public static SQLInsertValues sequential(@Nonnull Iterable<?> ... columnSequence) {
        SQLInsertValues insertValues = new SQLInsertValues();
        if (columnSequence.length == 0) {
            throw new IllegalArgumentException();
        }
        Iterator<?>[] iterators = new Iterator[columnSequence.length];
        for (int i = 0; i < columnSequence.length; i++) {
            iterators[i] = columnSequence[i].iterator();
        }
        for(;;) {
            Object[] row = new Object[columnSequence.length];
            for (int i = 0; i < row.length; i++) {
                if (!iterators[i].hasNext()) {
                    return insertValues;
                }
                row[i] = iterators[i].next();
            }
            insertValues.values(row);
        }
    }

    public static <T> Iterable<T> staticSequence(final T value) {
        return () -> new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public T next() {
                return value;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
