package org.libsmith.sql;

import org.libsmith.anvil.reflection.GenericReflection;
import org.libsmith.sql.SQLTemplate.GeneratedKeyMapper;
import org.libsmith.sql.SQLTemplate.Mapper;
import org.libsmith.sql.SQLTemplate.OptionalMapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
 * @created 30.05.2015 21:03
 */
public abstract class SQLMappers {
    private SQLMappers()
    { }

    public interface ColumnMapper<T> {
        T map(ExtendedResultSet resultSet, int columnIndex) throws SQLException;
    }

    public static class MapAggregator<K, V> extends SQLTemplate.AbstractSQLRowAggregator<Map<K, V>> {
        private final Mapper<K> keyMapper;
        private final Mapper<V> valueMapper;
        private final Map<K, V> map;

        protected MapAggregator(@Nonnull Mapper<K> keyMapper, @Nonnull Mapper<V> valueMapper, @Nonnull Map<K, V> map) {
            this.keyMapper = keyMapper;
            this.valueMapper = valueMapper;
            this.map = map;
        }

        @Override
        public @Nullable Map<K, V> processRow(@Nonnull ExtendedResultSet resultSet) throws Exception {
            K key = keyMapper.map(resultSet);
            V value = valueMapper.map(resultSet);
            map.put(key, value);
            return null;
        }

        @Override
        public @Nonnull Map<K, V> getResult() throws Exception {
            return map;
        }

        public static <K, V> MapAggregator<K, V> of(@Nonnull Mapper<K> keyMapper, @Nonnull Mapper<V> valueMapper) {
            return new MapAggregator<>(keyMapper, valueMapper, new HashMap<>());
        }

        public static <K, V> MapAggregator<K, V> of(@Nonnull Mapper<K> keyMapper, @Nonnull Mapper<V> valueMapper,
                                                    @Nonnull Map<K, V> map) {
            return new MapAggregator<>(keyMapper, valueMapper, map);
        }
    }

    @SuppressWarnings("unused")
    public static abstract class FlexibleColumnMapper<T> implements ColumnMapper<T> {
        private final OptionalColumnEntityMapper<T> optionalMapperForFirstColumn = new OptionalColumnEntityMapper<>(this, 1);
        private final NotNullColumnEntityMapper<T> notNullMapperForFirstColumn = new NotNullColumnEntityMapper<>(this, 1);

        public NotNullColumnEntityMapper<T> forNotNullColumn(int columnNumber) {
            if (columnNumber == 1) {
                return notNullMapperForFirstColumn;
            }
            if (columnNumber < 1) {
                throw new IllegalArgumentException("Column number must be equals or greater than 1, got " + columnNumber);
            }
            return new NotNullColumnEntityMapper<>(this, columnNumber);
        }

        public NotNullColumnEntityMapper<T> forNotNullColumn(String columnName) {
            return new NotNullColumnEntityMapper<>(this, columnName);
        }

        public OptionalColumnEntityMapper<T> forColumn(int columnNumber) {
            if (columnNumber == 1) {
                return optionalMapperForFirstColumn;
            }
            if (columnNumber < 1) {
                throw new IllegalArgumentException("Column number must be equals or greater than 1, got " + columnNumber);
            }
            return new OptionalColumnEntityMapper<>(this, columnNumber);
        }

        public OptionalColumnEntityMapper<T> forColumn(String columnName) {
            return new OptionalColumnEntityMapper<>(this, columnName);
        }

        public abstract static class ColumnEntityMapper<T> {
            protected final ColumnMapper<T> columnMapper;
            protected final int columnIndex;
            protected final String columnLabel;

            public ColumnEntityMapper(ColumnMapper<T> columnMapper, String columnLabel) {
                this.columnMapper = columnMapper;
                this.columnIndex = -1;
                this.columnLabel = columnLabel;
            }

            public ColumnEntityMapper(ColumnMapper<T> columnMapper, int columnIndex) {
                this.columnMapper = columnMapper;
                this.columnIndex = columnIndex;
                this.columnLabel = null;
            }
        }

        public static class OptionalColumnEntityMapper<T> extends ColumnEntityMapper<T> implements OptionalMapper<T> {

            public OptionalColumnEntityMapper(ColumnMapper<T> columnMapper, String columnLabel) {
                super(columnMapper, columnLabel);
            }

            public OptionalColumnEntityMapper(ColumnMapper<T> columnMapper, int columnIndex) {
                super(columnMapper, columnIndex);
            }

            @Override
            public @Nonnull Optional<T> map(@Nonnull ExtendedResultSet resultSet) throws SQLException {
                int idx = columnIndex;
                if (columnLabel != null) {
                    idx = resultSet.findColumn(columnLabel);
                }
                return Optional.ofNullable(columnMapper.map(resultSet, idx));
            }
        }

        public static class NotNullColumnEntityMapper<T> extends ColumnEntityMapper<T> implements Mapper<T> {

            public NotNullColumnEntityMapper(ColumnMapper<T> columnMapper, String columnLabel) {
                super(columnMapper, columnLabel);
            }

            public NotNullColumnEntityMapper(ColumnMapper<T> columnMapper, int columnIndex) {
                super(columnMapper, columnIndex);
            }

            @Override
            public @Nonnull T map(@Nonnull ExtendedResultSet resultSet) throws SQLException {
                int idx = columnIndex;
                if (columnLabel != null) {
                    idx = resultSet.findColumn(columnLabel);
                }
                T result = columnMapper.map(resultSet, idx);
                if (result == null) {
                    throw new SQLException("NULL value returned for column " + (columnLabel == null ? idx : columnLabel));
                }
                return result;
            }
        }
    }

    public static class SeparatedListMapper<T> extends FlexibleColumnMapper<List<T>> {
        private final Pattern splitPattern;
        private final Class<T> elementType;

        public SeparatedListMapper(Class<T> elementType) {
            this(elementType, null);
        }

        public SeparatedListMapper(Class<T> elementType, Pattern splitPattern) {
            this.splitPattern = splitPattern == null ? Pattern.compile("\\s*,\\s*") : splitPattern;
            this.elementType = elementType;
        }

        @Override
        public List<T> map(ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            String string = resultSet.getString(columnIndex);
            if (string == null) {
                return null;
            }
            String[] split = this.splitPattern.split(string, -1);
            List<T> list = new ArrayList<>(split.length);
            for (String value : split) {
                list.add(convert(value));
            }
            return list;
        }

        protected T convert(String string) {
            if (elementType == String.class) {
                return elementType.cast(string);
            }
            else if (elementType == Integer.class) {
                return string.isEmpty() ? null : elementType.cast(Integer.parseInt(string));
            }
            else if (elementType == Long.class) {
                return string.isEmpty() ? null : elementType.cast(Long.parseLong(string));
            }
            else if (elementType == Short.class) {
                return string.isEmpty() ? null : elementType.cast(Short.parseShort(string));
            }
            else if (elementType == Byte.class) {
                return string.isEmpty() ? null : elementType.cast(Byte.parseByte(string));
            }
            else if (elementType == Double.class) {
                return string.isEmpty() ? null : elementType.cast(Double.parseDouble(string));
            }
            else if (elementType == Float.class) {
                return string.isEmpty() ? null : elementType.cast(Float.parseFloat(string));
            }
            throw new IllegalArgumentException();
        }
    }

    @SuppressWarnings("unused")
    public static class SQLValueEnumMapper<K, T extends Enum & SQLValue<K>> extends FlexibleColumnMapper<T> {
        private final Map<K, T> valuesMap = new HashMap<>();
        private final Class<K> keyClass;
        private final Class<T> type;

        protected SQLValueEnumMapper(Class<T> type) {
            this.type = type;
            this.keyClass = GenericReflection.extractParameterOf(SQLValue.class).atIndex(0).from(type);
            for (T constant : type.getEnumConstants()) {
                this.valuesMap.put(constant.getSQLValueEntity(), constant);
            }
        }

        @Override
        public T map(ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            K key = resultSet.getObject(columnIndex, keyClass);
            if (resultSet.wasNull()) {
                return null;
            }
            T value = valuesMap.get(key);
            if (value == null) {
                throw new SQLException("Unknown value '" + key + "' for sql value of enum " + type);
            }
            return value;
        }

        public static <K, T extends Enum & SQLValue<K>> SQLValueEnumMapper<K, T> of(Class<T> type) {
            return new SQLValueEnumMapper<>(type);
        }
    }

    @SuppressWarnings("unused")
    public static class EnumMapper<T extends Enum> extends FlexibleColumnMapper<T>  {
        private final Map<String, T> valuesMap = new HashMap<>();
        private final Class<T> type;

        protected EnumMapper(Class<T> type) {
            this.type = type;
            for (T constant : type.getEnumConstants()) {
                valuesMap.put(constant.name(), constant);
            }
        }

        @Override
        public T map(ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            String key = resultSet.getString(columnIndex);
            if (key == null) {
                return null;
            }
            T value = valuesMap.get(key);
            if (value == null) {
                throw new SQLException("Unknown value '" + key + "' for name of enum " + type);
            }
            return value;
        }

        public static <T extends Enum> EnumMapper<T> of(Class<T> type) {
            return new EnumMapper<>(type);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static final FlexibleColumnMapper<Boolean> BOOLEAN = new FlexibleColumnMapper<Boolean>() {
        @Override
        public Boolean map(@Nonnull ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            boolean result = resultSet.getBoolean(columnIndex);
            return resultSet.wasNull() ? null : result;
        }
    };

    @SuppressWarnings("UnusedDeclaration")
    public static final FlexibleColumnMapper<Float> FLOAT = new FlexibleColumnMapper<Float>() {
        @Override
        public Float map(@Nonnull ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            float result = resultSet.getFloat(columnIndex);
            return resultSet.wasNull() ? null : result;
        }
    };

    @SuppressWarnings("UnusedDeclaration")
    public static final FlexibleColumnMapper<Integer> INTEGER = new FlexibleColumnMapper<Integer>() {
        @Override
        public Integer map(@Nonnull ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            int result = resultSet.getInt(columnIndex);
            return resultSet.wasNull() ? null : result;
        }
    };

    @SuppressWarnings("UnusedDeclaration")
    public static final FlexibleColumnMapper<Long> LONG = new FlexibleColumnMapper<Long>() {
        @Override
        public Long map(@Nonnull ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            long result = resultSet.getLong(columnIndex);
            return resultSet.wasNull() ? null : result;
        }
    };

    @SuppressWarnings("UnusedDeclaration")
    public static final FlexibleColumnMapper<BigDecimal> BIG_DECIMAL = new FlexibleColumnMapper<BigDecimal>() {
        @Override
        public BigDecimal map(@Nonnull ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            return resultSet.getBigDecimal(columnIndex);
        }
    };

    @SuppressWarnings("UnusedDeclaration")
    public static final FlexibleColumnMapper<Date> DATE = new FlexibleColumnMapper<Date>() {
        @Override
        public Date map(@Nonnull ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            return resultSet.getDate(columnIndex);
        }
    };

    @SuppressWarnings("UnusedDeclaration")
    public static final FlexibleColumnMapper<Time> TIME = new FlexibleColumnMapper<Time>() {
        @Override
        public Time map(@Nonnull ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            return resultSet.getTime(columnIndex);
        }
    };

    @SuppressWarnings("UnusedDeclaration")
    public static final FlexibleColumnMapper<Timestamp> TIMESTAMP = new FlexibleColumnMapper<Timestamp>() {
        @Override
        public Timestamp map(@Nonnull ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            return resultSet.getTimestamp(columnIndex);
        }
    };

    public static final FlexibleColumnMapper<Long> TIMESTAMP_TIME = new FlexibleColumnMapper<Long>() {
        @Override
        public Long map(@Nonnull ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            Timestamp timestamp = resultSet.getTimestamp(columnIndex);
            return timestamp == null ? null : timestamp.getTime();
        }
    };

    @SuppressWarnings("UnusedDeclaration")
    public static final FlexibleColumnMapper<Number> NUMBER = new FlexibleColumnMapper<Number>() {
        @Override
        public Number map(@Nonnull ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            return resultSet.getNumber(columnIndex);
        }
    };

    public static final FlexibleColumnMapper<String> STRING = new FlexibleColumnMapper<String>() {
        @Override
        public String map(ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            return resultSet.getString(columnIndex);
        }
    };

    public static final FlexibleColumnMapper<String> STRING_LOWERCASE = new FlexibleColumnMapper<String>() {
        @Override
        public String map(ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            String value = resultSet.getString(columnIndex);
            return value == null ? null : value.toLowerCase();
        }
    };

    public static final FlexibleColumnMapper<TimeZone> TIMEZONE = new FlexibleColumnMapper<TimeZone>() {
        @Override
        public TimeZone map(ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            String tzString = resultSet.getString(columnIndex);
            return tzString == null ? null : TimeZone.getTimeZone(tzString);
        }
    };

    public static final FlexibleColumnMapper<InetAddress> INET_ADDRESS = new FlexibleColumnMapper<InetAddress>() {
        @Override
        public InetAddress map(ExtendedResultSet resultSet, int columnIndex) throws SQLException {
            String stringValue = resultSet.getString(columnIndex);
            try {
                return stringValue == null ? null : InetAddress.getByName(stringValue);
            }
            catch (UnknownHostException ex) {
                throw new SQLException(ex);
            }
        }
    };

    @SuppressWarnings("UnusedDeclaration")
    public static final GeneratedKeyMapper<Number> NUMBER_KEY = new GeneratedKeyMapper<Number>() {
        @Override
        public @Nonnull Number map(@Nonnull ExtendedResultSet resultSet) throws SQLException {
            Number number = resultSet.getNumber(1);
            if (number == null) {
                throw new SQLException("NULL key returned");
            }
            return number;
        }
    };

    @SuppressWarnings("UnusedDeclaration")
    public static final GeneratedKeyMapper<Integer> INTEGER_KEY = new GeneratedKeyMapper<Integer>() {
        @Override
        public @Nonnull Integer map(@Nonnull ExtendedResultSet resultSet) throws SQLException {
            int result = resultSet.getInt(1);
            if (resultSet.wasNull()) {
                throw new SQLException("NULL key returned");
            }
            return result;
        }
    };

    @SuppressWarnings("UnusedDeclaration")
    public static final Mapper<Map<String, Object>> ROW_TO_MAP = new Mapper<Map<String, Object>>() {
        @Override
        public @Nonnull Map<String, Object> map(@Nonnull ExtendedResultSet resultSet) throws SQLException {
            ResultSetMetaData metaData = resultSet.getMetaData();
            Map<String, Object> row = new HashMap<>();
            for (int i = 0, size = metaData.getColumnCount(); i < size; i++) {
                String columnLabel = metaData.getColumnLabel(i + 1);
                Object value = resultSet.getObject(i + 1);
                row.put(columnLabel, value);
            }
            return row;
        }
    };
}
