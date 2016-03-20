package org.libsmith.sql;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import javax.annotation.Nonnull;
import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
* @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
* @created 14.02.2015 20:55
*/
@SuppressWarnings("UnusedDeclaration")
public class SQLExpression implements SQLFragment, SQLTemplate.KnownMeaningOfLife<SQLExpression> {
    private enum Comparator {
        EQ("=", "<>"),
        GT(">", "<="),
        GE(">=", "<"),
        LT("<", ">="),
        LE("<=", ">"),
        IN("IN", "NOT IN"),
        NULL("IS NULL", "IS NOT NULL"),
        LIKE("LIKE", "NOT LIKE"),
        LIKE_PATTERN("LIKE", "NOT LIKE"),
        BETWEEN("BETWEEN", "NOT BETWEEN");

        private final String value;
        private final String notValue;

        Comparator(String value, String notValue) {
            this.value = value;
            this.notValue = notValue;
        }

        public String getStringValue(boolean not) {
            return not ? notValue : value;
        }
    }

    private final String prettyIndentation;
    private final BuildParameters defaultBuildParameters;

    private byte meaningOfLife = 0;
    private StringBuilder queryBuilder = new StringBuilder();
    private List<Object> values = new ArrayList<>();
    private BuildParameters bp;
    private int modCount = 1;

    public SQLExpression () {
        this("\n  ", new BuildParameters());
    }

    protected SQLExpression(String prettyIndentation,
                            BuildParameters defaultBuildParameters) {
        this.prettyIndentation = prettyIndentation;
        this.defaultBuildParameters = defaultBuildParameters;
        this.bp = defaultBuildParameters.clone();
    }

    public SQLExpression column(String name) {
        if (this.bp.column != null) {
            throw new IllegalStateException();
        }
        this.bp.column = name;
        this.modCount++;
        return this;
    }

    public SQLExpression not() {
        if (this.bp.not != this.defaultBuildParameters.not) {
            throw new IllegalStateException();
        }
        this.bp.not = true;
        this.modCount++;
        return this;
    }

    public SQLExpression eq() {
        return checkAndSetComparator(Comparator.EQ);
    }

    public SQLExpression in() {
        return checkAndSetComparator(Comparator.IN);
    }

    public SQLExpression gt() {
        return checkAndSetComparator(Comparator.GT);
    }

    public SQLExpression ge() {
        return checkAndSetComparator(Comparator.GE);
    }

    public SQLExpression lt() {
        return checkAndSetComparator(Comparator.LT);
    }

    public SQLExpression le() {
        return checkAndSetComparator(Comparator.LE);
    }

    public SQLExpression like() {
        return checkAndSetComparator(Comparator.LIKE);
    }

    public SQLExpression likePattern() {
        return checkAndSetComparator(Comparator.LIKE_PATTERN);
    }

    public SQLExpression apply(Extension extension) {
        extension.apply(this, this.bp);
        return this;
    }

    public SQLExpression apply(Extension... extensions) {
        for (Extension extension : extensions) {
            apply(extension);
        }
        return this;
    }

    public SQLExpression values(Object ... value) {
        return value(value);
    }

    public SQLExpression value(Object value) {
        return checkAndSetValue(value, false);
    }

    public SQLExpression ifNotNullValue(Object value) {
        return checkAndSetValue(value, true);
    }

    public SQLExpression and() {
        flush(false);
        this.bp.concat = "AND";
        return this;
    }

    public SQLExpression and(SQLExpression subExpression) {
        return and().value(subExpression);
    }

    public SQLExpression or() {
        flush(false);
        this.bp.concat = "OR";
        return this;
    }

    public SQLExpression or(SQLExpression subExpression) {
        return or().value(subExpression);
    }

    @Override
    public SQLExpression withMeaningOfLife() {
        this.meaningOfLife = 42;
        this.modCount++;
        return this;
    }

    private SQLExpression checkAndSetComparator(Comparator comparator) {
        if (this.bp.comparator != this.defaultBuildParameters.comparator) {
            throw new IllegalStateException();
        }
        this.bp.comparator = comparator;
        this.modCount++;
        return this;
    }

    private SQLExpression checkAndSetValue(Object value, boolean ifNotNull) {
        if (this.bp.valueIsSet && this.bp.value != defaultBuildParameters.value) {
            throw new IllegalStateException();
        }
        this.bp.ifNotNull = ifNotNull;
        this.bp.valueIsSet = true;
        this.bp.value = value;
        this.modCount++;
        return this;
    }

    private void flush(boolean ignoreIllegalState) {
        boolean isSubExpression = bp.comparator == null && bp.value instanceof SQLFragment;
        boolean valueNotSet = !bp.valueIsSet;
        boolean comparatorNotSet = bp.comparator == null && !isSubExpression && !(bp.ifNotNull && bp.value == null);
        boolean columnNameNotSet = bp.column == null && !isSubExpression;
        boolean concatNotSet = (queryBuilder.length() > 0 && bp.concat == null);
        if (valueNotSet || comparatorNotSet || columnNameNotSet || concatNotSet) {
            if (ignoreIllegalState) {
                return;
            }
            List<String> errors = new ArrayList<>();
            if (valueNotSet) {
                errors.add("value not set");
            }
            if (comparatorNotSet) {
                errors.add("comparator not set");
            }
            if (columnNameNotSet) {
                errors.add("column name not set");
            }
            if (concatNotSet) {
                errors.add("concat not set");
            }
            throw new IllegalStateException(errors.stream().collect(Collectors.joining(", ")));
        }
        if (modCount == 0) {
            return;
        }
        modCount = 0;
        String subExpression = isSubExpression && bp.value != null ? ((SQLFragment) bp.value).getFragment().trim() : "";
        if(!subExpression.isEmpty() || (!isSubExpression && !bp.ifNotNull) || (!isSubExpression && bp.value != null)) {
            if (bp.value == null) {
                if (bp.comparator == Comparator.EQ) {
                    bp.comparator = Comparator.NULL;
                }
                else {
                    throw new IllegalArgumentException();
                }
            }

            if (bp.concat != null && queryBuilder.length() > 0) {
                queryBuilder.append(prettyIndentation == null ? " " : prettyIndentation).append(bp.concat).append(" ");
            }

            queryBuilder.append("(");
            if (!subExpression.isEmpty()) {
                queryBuilder.append(subExpression);
                values.addAll(((SQLFragment) bp.value).getParameters());
            }
            else {
                queryBuilder.append(bp.column).append(" ").append(bp.comparator.getStringValue(bp.not)).append(" ");

                if (bp.value instanceof SQLFragment) {
                    SQLFragment fragment = (SQLFragment) bp.value;
                    queryBuilder.append(fragment.getFragment());
                    values.addAll(fragment.getParameters());
                }
                else if (bp.value instanceof SQLValue) {
                    queryBuilder.append("?");
                    values.add(((SQLValue) bp.value).getSQLValueEntity());
                }
                else if (bp.value instanceof Enum) {
                    queryBuilder.append("?");
                    values.add(((Enum) bp.value).name());
                }
                else if (bp.comparator == Comparator.LIKE) {
                    bp.value = bp.value == null ? null : "%" + bp.value.toString().replace("\\", "\\\\")
                                                                       .replace("%", "\\%").replace("_", "\\_") + "%";
                    queryBuilder.append("?");
                    values.add(bp.value);
                }
                else if (bp.comparator == Comparator.NULL) {
                    assert true;
                }
                else if (bp.comparator == Comparator.BETWEEN) {
                    queryBuilder.append("? AND ?");
                    if (bp.value == null) {
                        throw new IllegalArgumentException();
                    }
                    else if (bp.value instanceof Iterable) {
                        @SuppressWarnings("unchecked")
                        Iterator<Object> iterator = ((Iterable<Object>) bp.value).iterator();
                        if (!iterator.hasNext()) {
                            throw new IllegalArgumentException();
                        }
                        values.add(iterator.next());
                        if (!iterator.hasNext()) {
                            throw new IllegalArgumentException();
                        }
                        values.add(iterator.next());
                        if (iterator.hasNext()) {
                            throw new IllegalArgumentException();
                        }
                    }
                    else if (bp.value.getClass().isArray()) {
                        if (Array.getLength(bp.value) != 2) {
                            throw new IllegalArgumentException();
                        }
                        values.add(Array.get(bp.value, 0));
                        values.add(Array.get(bp.value, 1));
                    }
                }
                else if (bp.comparator == Comparator.IN) {
                    boolean delimiterFlag = false;
                    queryBuilder.append("(");
                    if (bp.value instanceof Iterable) {
                        for (Object element : (Iterable) bp.value) {
                            if (delimiterFlag) {
                                queryBuilder.append(", ");
                            }
                            else {
                                delimiterFlag = true;
                            }
                            queryBuilder.append("?");
                            values.add(element instanceof SQLValue ? ((SQLValue) element).getSQLValueEntity() : element);
                        }
                    }
                    else if (bp.value.getClass().isArray()) {
                        for (int i = 0, l = Array.getLength(bp.value); i < l; i++) {
                            if (delimiterFlag) {
                                queryBuilder.append(", ");
                            }
                            else {
                                delimiterFlag = true;
                            }
                            queryBuilder.append("?");
                            Object element = Array.get(bp.value, i);
                            values.add(element instanceof SQLValue ? ((SQLValue) element).getSQLValueEntity() : element);
                        }
                    }
                    else {
                        throw new IllegalArgumentException();
                    }
                    if (!delimiterFlag) {
                        queryBuilder.append("NULL");
                    }
                    queryBuilder.append(")");
                }
                else {
                    queryBuilder.append("?");
                    values.add(bp.value);
                }
            }
            queryBuilder.append(")");
        }
        bp = defaultBuildParameters.clone();
    }

    @Override
    public @Nonnull String getFragment() {
        flush(true);
        if (queryBuilder.length() == 0 && meaningOfLife == 42) {
            meaningOfLife = 0;
            return "42=42";
        }
        return queryBuilder.toString();
    }

    @Override
    public @Nonnull List<Object> getParameters() {
        flush(true);
        return values;
    }

    protected SQLExpression pivot() {
        return pivot(false);
    }

    protected SQLExpression pivot(boolean indentation) {
        String newIndentation = !indentation
                        ? null
                        : prettyIndentation == null
                            ? null
                            : prettyIndentation + "  ";
        return new SQLExpression(newIndentation, bp.clone());
    }

    public static final class BuildParameters implements Cloneable {
        public String column;
        public String concat;
        public Comparator comparator;
        public Object value;
        public boolean valueIsSet;
        public boolean not;
        public boolean ifNotNull;

        private BuildParameters()
        { }

        @Override
        protected BuildParameters clone() {
            try {
                return (BuildParameters) super.clone();
            }
            catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @FunctionalInterface
    public interface Extension {
        void apply(SQLExpression builder, BuildParameters currentBuildParameters);
    }

    public static final Extension NOT_EMPTY_MACRO = (builder, bp) ->
            builder.value(builder.pivot().not().eq().value("")
                                         .and().not().eq().value(null));

    public static final Extension OR_IS_NULL_COLUMN_MACRO = (builder, bp) -> {
        if (bp.value == null) {
            return;
        }
        if (!bp.valueIsSet) {
            throw new IllegalStateException();
        }
        bp.value = builder.pivot().or().eq().value(null);
        bp.comparator = null;
    };

    public static final Extension TO_TIMESTAMP_CONVERTER = (builder, bp) -> {
        if (bp.value != null && !(bp.value instanceof Timestamp)) {
            if (bp.value instanceof Number) {
                bp.value = new Timestamp(((Number) bp.value).longValue());
            }
            else if (bp.value instanceof Calendar) {
                bp.value = new Timestamp(((Calendar) bp.value).getTimeInMillis());
            }
            else if (bp.value instanceof Date) {
                bp.value = new Timestamp(((Date) bp.value).getTime());
            }
            else {
                throw new IllegalArgumentException();
            }
        }
    };

    public static final class Guava {
        private Guava()
        { }

        public static final Extension IN_RANGE_MACRO = (builder, bp) -> {
            if (bp.value instanceof Range) {
                Range range = (Range) bp.value;
                bp.value = null;
                bp.valueIsSet = false;
                if (range.isEmpty()) {
                    builder.eq().value(range.lowerEndpoint());
                }
                else if (range.hasUpperBound() || range.hasLowerBound()) {
                    builder.pivot();
                    if (range.hasLowerBound()) {
                        if (range.lowerBoundType() == BoundType.CLOSED) {
                            builder.ge().ifNotNullValue(range.lowerEndpoint());
                        }
                        else {
                            builder.gt().ifNotNullValue(range.lowerEndpoint());
                        }
                        if (range.hasUpperBound()) {
                            builder.and();
                        }
                    }
                    if (range.hasUpperBound()) {
                        if (range.upperEndpoint() == BoundType.CLOSED) {
                            builder.le().ifNotNullValue(range.upperEndpoint());
                        }
                        else {
                            builder.lt().ifNotNullValue(range.upperEndpoint());
                        }
                    }
                }
            }
            else if (!bp.ifNotNull) {
                throw new IllegalStateException();
            }
            else {
                bp.value = null;
            }
        };
    }
}
