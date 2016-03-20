package org.libsmith.sql;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

/**
 * @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
 * @created 14.02.2015 21:00
 */
public class SQLSelect implements SQLFragment {
    public interface SelectHint extends SQLFragment
    { }

    public enum MySQLHint implements SelectHint {
        DISTINCT,
        HIGH_PRIORITY,
        STRAIGHT_JOIN,
        SQL_SMALL_RESULT,
        SQL_BIG_RESULT,
        SQL_BUFFER_RESULT,
        SQL_CACHE,
        SQL_NO_CACHE,
        SQL_CALC_FOUND_ROWS;

        public static final ExtendedResultSet.AttachmentKey<Integer> FOUND_ROWS_ATTACHMENT_KEY = new ExtendedResultSet.AttachmentKey<>();

        @Override
        public @Nonnull String getFragment() {
            return name();
        }

        @Override
        public @Nonnull List<Object> getParameters() {
            return Collections.emptyList();
        }
    }

    private final Map<Object, List<String>> columnsByQualifierMap = new HashMap<>();
    private List<Object> qualifiers;

    public SQLSelect()
    { }

    public SQLSelect(Object ... qualifiers) {
        qualifiers(qualifiers);
    }

    public SQLSelect columns(@Nonnull String ... columns) {
        return columns(true, columns);
    }

    public SQLSelect columns(@Nonnull Boolean clause, @Nonnull String ... columns) {
        return Boolean.TRUE.equals(clause) ? columns((Object) null, columns) : this;
    }

    public SQLSelect columns(@Nullable Object qualifier, @Nonnull String ... columns) {
        for (String column : columns) {
            List<String> list = columnsByQualifierMap.get(qualifier);
            if (list == null) {
                list = new ArrayList<>();
                columnsByQualifierMap.put(qualifier, list);
            }
            list.add(column);
        }
        return this;
    }

    public <T extends Collection<?>> SQLSelect qualifiers(@Nullable T qualifiers) {
        if (qualifiers != null) {
            if (this.qualifiers == null) {
                this.qualifiers = new ArrayList<>();
            }
            this.qualifiers.addAll(qualifiers);
        }
        return this;
    }

    public SQLSelect qualifiers(@Nullable Object ... qualifiers) {
        return qualifiers(qualifiers == null ? null : Arrays.asList(qualifiers));
    }

    protected boolean contains(SelectHint selectHint) {
        return qualifiers != null && qualifiers.contains(selectHint);
    }

    @Override
    public @Nonnull String getFragment() {
        Set<String> columns = new HashSet<>();
        List<String> nullColumns = columnsByQualifierMap.get(null);
        if (nullColumns != null) {
            columns.addAll(nullColumns);
        }

        final StringBuilder sb = new StringBuilder();
        if (qualifiers != null) {
            for (Object qualifier : qualifiers) {
                if (qualifier instanceof SelectHint) {
                    sb.append(((SelectHint) qualifier).getFragment()).append(" ");
                }
                List<String> qualifierColumns = columnsByQualifierMap.get(qualifier);
                if (qualifierColumns != null)  {
                    columns.addAll(qualifierColumns);
                }
            }
        }
        int i = 0;
        for (String column : columns) {
            if (i++ > 0) {
                sb.append(", ");
            }
            sb.append(column);
        }
        return sb.toString();
    }

    @Override
    public @Nonnull List<Object> getParameters() {
        return Collections.emptyList();
    }
}
