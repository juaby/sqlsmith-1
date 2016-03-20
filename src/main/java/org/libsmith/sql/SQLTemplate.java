package org.libsmith.sql;


import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
 * @created 14.02.2015 20:00
 */
public class SQLTemplate {
    private static final Logger LOG = Logger.getLogger(SQLTemplate.class.getName());

    private final Map<String, Object> parameterMap = new HashMap<>();
    private final List<SQLFragment> epilogueList = new ArrayList<>();
    private final String template;
    private final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\B\\\\?:(\\w+)");

    private DataSource dataSource;

    public SQLTemplate(String template, DataSource dataSource) {
        this.template = template;
        this.dataSource = dataSource;
    }

    public int executeQuery() throws SQLException {
        try ( Connection connection = dataSource.getConnection();
              PreparedStatement ps = prepareStatement(connection, false) ) {
            try {
                return ps.executeUpdate();
            }
            catch (SQLException ex) {
                if (!LOG.isLoggable(Level.FINEST)) {
                    LOG.finest(String.format("%08X: %s", unwrap(connection).hashCode(), ps));
                }
                throw ex;
            }
        }
    }

    public <T extends Throwable> void executeQueryAndThrowIfNoUpdate(Class<T> throwable) throws T, SQLException {
        if (executeQuery() == 0) {
            try {
                throw throwable.newInstance();
            }
            catch (InstantiationException | IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public <T> ResultList<T> executeQuery(GeneratedKeyMapper<T> generatedKeyMapper) throws SQLException {
        try ( Connection connection = dataSource.getConnection();
              PreparedStatement ps = prepareStatement(connection, false) ) {
            int count = ps.executeUpdate();
            ResultList<T> resultList = new ResultArrayList<>(count);
            if (count > 0) {
                try (ExtendedResultSet rs = new ExtendedResultSet(ps.getGeneratedKeys())) {
                    while (rs.next()) {
                        T res = generatedKeyMapper.map(rs);
                        resultList.add(res);
                    }
                }
                catch (SQLException ex) {
                    if (!LOG.isLoggable(Level.FINEST)) {
                        LOG.finest(String.format("%08X: %s", unwrap(connection).hashCode(), ps));
                    }
                    throw ex;
                }
            }
            return resultList;
        }
    }

    public <T> ResultListOfOptionals<T> executeQuery(OptionalMapper<T> optionalMapper) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            SQLLimit sqlLimit = getParameterOf(SQLLimit.class);
            SQLSelect sqlSelect = getParameterOf(SQLSelect.class);
            boolean checkFoundRows = sqlSelect != null && sqlSelect.contains(SQLSelect.MySQLHint.SQL_CALC_FOUND_ROWS);
            try (PreparedStatement ps = prepareStatement(connection, false)) {
                try (ExtendedResultSet rs = new ExtendedResultSet(ps.executeQuery())) {
                    Integer foundRows = checkFoundRows ? selectFoundRows(connection) : null;
                    rs.putAttachment(SQLSelect.MySQLHint.FOUND_ROWS_ATTACHMENT_KEY, foundRows);
                    ResultListOfOptionals<T> resultList = new ResultArrayListOfOptionals<>(foundRows,
                                                                     sqlLimit == null ? null : sqlLimit.getOffset(),
                                                                     sqlLimit == null ? null : sqlLimit.getLimit());
                    while (rs.next()) {
                        @SuppressWarnings("unchecked")
                        Optional<T> object = optionalMapper.map(rs);
                        resultList.add(object);
                    }
                    return resultList;
                }
                catch (SQLException ex) {
                    if (!LOG.isLoggable(Level.FINEST)) {
                        LOG.finest(String.format("%08X: %s", unwrap(connection).hashCode(), ps));
                    }
                    throw ex;
                }
            }
        }
    }

    public <T> ResultList<T> executeQuery(Mapper<T> mapper) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            SQLLimit sqlLimit = getParameterOf(SQLLimit.class);
            SQLSelect sqlSelect = getParameterOf(SQLSelect.class);
            boolean checkFoundRows = sqlSelect != null && sqlSelect.contains(SQLSelect.MySQLHint.SQL_CALC_FOUND_ROWS);
            try (PreparedStatement ps = prepareStatement(connection, false)) {
                try (ExtendedResultSet rs = new ExtendedResultSet(ps.executeQuery())) {
                    Integer foundRows = checkFoundRows ? selectFoundRows(connection) : null;
                    rs.putAttachment(SQLSelect.MySQLHint.FOUND_ROWS_ATTACHMENT_KEY, foundRows);
                    ResultList<T> resultList = new ResultArrayList<>(foundRows,
                                                                     sqlLimit == null ? null : sqlLimit.getOffset(),
                                                                     sqlLimit == null ? null : sqlLimit.getLimit());
                    while (rs.next()) {
                        T object = mapper.map(rs);
                        resultList.add(object);
                    }
                    return resultList;
                }
                catch (SQLException ex) {
                    if (!LOG.isLoggable(Level.FINEST)) {
                        LOG.info(String.format("%08X: %s", unwrap(connection).hashCode(), ps));
                    }
                    throw ex;
                }
            }
        }
    }

    public <T, R> T executeQuery(Mapper<R> mapper, RowAggregator<? extends T, ? super R> aggregator) throws SQLException {
        return executeQuery(new MappableRowAggregator<>(mapper, aggregator));
    }

    public <T> T executeQuery(RowAggregator<? extends T, ? super ExtendedResultSet> aggregator) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            SQLSelect sqlSelect = getParameterOf(SQLSelect.class);
            boolean checkFoundRows = sqlSelect != null && sqlSelect.contains(SQLSelect.MySQLHint.SQL_CALC_FOUND_ROWS);
            try (PreparedStatement ps = prepareStatement(connection, false)) {
                @SuppressWarnings({ "unchecked", "rawtypes" })
                SQLRowAggregator<T> sqlRowAggregator =
                        aggregator instanceof SQLRowAggregator ? (SQLRowAggregator) aggregator
                                                               : null;
                T result = sqlRowAggregator != null ? sqlRowAggregator.preQuery(ps) : null;
                if (result != null) {
                    return result;
                }
                try (ExtendedResultSet rs = new ExtendedResultSet(ps.executeQuery())) {
                    if (checkFoundRows) {
                        rs.putAttachment(SQLSelect.MySQLHint.FOUND_ROWS_ATTACHMENT_KEY, selectFoundRows(connection));
                    }
                    result = sqlRowAggregator != null ? sqlRowAggregator.postQuery(rs) : null;
                    if (result != null) {
                        return result;
                    }
                    while (rs.next()) {
                        result = aggregator.processRow(rs);
                        if (result != null) {
                            return result;
                        }
                    }
                    return aggregator.getResult();
                }
                catch (SQLException ex) {
                    if (!LOG.isLoggable(Level.FINEST)) {
                        LOG.info(String.format("%08X: %s", unwrap(connection).hashCode(), ps));
                    }
                    throw ex;
                }
            }
            catch (SQLException | RuntimeException | Error ex) {
                throw ex;
            }
            catch (Throwable ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @SuppressWarnings("unused")
    public SQLTemplate parameter(@Nonnull String parameterName, Object value) {
        parameterMap.put(parameterName, value);
        return this;
    }

    @SuppressWarnings("unused")
    public SQLTemplate epilogue(@Nullable SQLFragment sqlFragment) {
        if (sqlFragment != null) {
            epilogueList.add(sqlFragment);
        }
        return this;
    }

    @SuppressWarnings("unused")
    public SQLTemplate epilogue(@Nonnull Class<? extends SQLFragment> fragmentType, @Nullable List<?> list) {
        if (list != null) {
            list.stream().filter(fragmentType::isInstance)
                         .map(fragmentType::cast)
                         .collect(Collectors.toCollection(() -> epilogueList));
        }
        return this;
    }

    @SuppressWarnings("unused")
    public SQLTemplate epilogue(@Nonnull Class<? extends SQLFragment> fragmentType, @Nullable Object ... list) {
        if (list != null) {
            for (Object o : list) {
                if (fragmentType.isInstance(o)) {
                    epilogueList.add((SQLFragment) o);
                }
            }
        }
        return this;
    }

    private PreparedStatement prepareStatement(Connection connection, boolean fetchGeneratedKey) throws SQLException {
        List<Object> parameters = new ArrayList<>();
        Matcher matcher = PLACEHOLDER_PATTERN.matcher(template);
        StringBuilder sb = new StringBuilder();
        int position = 0;
        while(matcher.find()) {
            sb.append(template, position, matcher.start());
            position = matcher.end();
            if (matcher.group().startsWith("\\")) {
                sb.append(":").append(matcher.group(1));
            }
            else {
                String placeholderName = matcher.group(1);
                if (!parameterMap.containsKey(placeholderName)) {
                    throw new IllegalArgumentException("Parameter with name '" + placeholderName + "' is not set");
                }
                Object rawValue = parameterMap.get(placeholderName);
                @SuppressWarnings("unchecked")
                Iterable<Object> rawValueIterable = rawValue instanceof Iterable
                                                            ? (Iterable<Object>) rawValue
                                                            : Collections.singleton(rawValue);
                boolean nextIteration = false;
                for (Object value : rawValueIterable) {
                    if (nextIteration) {
                        sb.append(", ");
                    }
                    else {
                        nextIteration = true;
                    }
                    if (value instanceof SQLFragment) {
                        if (value instanceof KnownMeaningOfLife) {
                            ((KnownMeaningOfLife) value).withMeaningOfLife();
                        }
                        SQLFragment sqlFragment = (SQLFragment) value;
                        parameters.addAll(sqlFragment.getParameters());
                        sb.append(sqlFragment.getFragment());
                    }
                    else if (value instanceof SQLValue) {
                        if (value instanceof KnownMeaningOfLife) {
                            ((KnownMeaningOfLife) value).withMeaningOfLife();
                        }
                        parameters.add(((SQLValue) value).getSQLValueEntity());
                        sb.append("?");
                    }
                    else if (value instanceof Enum) {
                        parameters.add(((Enum) value).name());
                        sb.append("?");
                    }
                    else {
                        sb.append("?");
                        parameters.add(value);
                    }
                }
            }
        }
        sb.append(template, position, template.length());
        for (SQLFragment sqlFragment : epilogueList) {
            sb.append(" ").append(sqlFragment.getFragment());
            parameters.addAll(sqlFragment.getParameters());
        }
        String query = sb.toString();
        PreparedStatement ps = connection.prepareStatement(query, fetchGeneratedKey ? Statement.RETURN_GENERATED_KEYS
                                                                                    : Statement.NO_GENERATED_KEYS);
        for (int i = 0; i < parameters.size(); i++) {
            try {
                ps.setObject(i + 1, parameters.get(i));
            }
            catch (SQLException ex) {
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.log(Level.FINEST,
                            "Exception catched (and rethrowed) at ps.setObject(#{}, {}), query: {}",
                            new Object[] { i + 1, parameters.get(i), query });
                }
                throw ex;
            }
        }
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(String.format("%08X: %s", unwrap(connection).hashCode(), ps));
        }
        return ps;
    }

    private @Nullable <T> T getParameterOf(Class<T> type) {
        for (Object value : parameterMap.values()) {
            if (type.isInstance(value)) {
                return type.cast(value);
            }
        }
        return null;
    }

    private static @Nonnull Connection unwrap(@Nonnull Connection connection) {
        Connection unwrapped = connection;
        try {
            for (int i = 0; i < 100; i++) {
                Connection c = connection.unwrap(Connection.class);
                if (c == null || c == unwrapped) {
                    break;
                }
                unwrapped = c;
            }
        }
        catch (SQLException ignored)
        { }
        return unwrapped;
    }

    private static @Nullable Integer selectFoundRows(Connection connection) throws SQLException {
        try (PreparedStatement cps = connection.prepareStatement("SELECT FOUND_ROWS()");
             ResultSet crs = cps.executeQuery()) {
            if (crs.next()) {
                return crs.getInt(1);
            }
            return null;
        }
    }

    public int executeInsertAndGetKey() throws SQLException {
        return executeQuery(SQLMappers.INTEGER_KEY).getSingleResultOrThrow(IllegalStateException.class);
    }

    public interface Mapper<T> {
        @Nonnull T map(@Nonnull ExtendedResultSet resultSet) throws SQLException;
    }

    public interface OptionalMapper<T> extends Mapper<Optional<T>>
    { }

    public interface SQLRowAggregator<T> extends RowAggregator<T, ExtendedResultSet> {
        @Nullable T preQuery(@Nonnull PreparedStatement preparedStatement) throws Exception;
        @Nullable T postQuery(@Nonnull ExtendedResultSet resultSet) throws Exception;
        @Override @Nullable T processRow(@Nonnull ExtendedResultSet resultSet) throws Exception;
        @Override @Nonnull T getResult() throws Exception;
    }

    public static abstract class AbstractSQLRowAggregator<T> implements SQLRowAggregator<T> {
        @Override
        public @Nullable T preQuery(@Nonnull PreparedStatement preparedStatement) throws Exception {
            return null;
        }

        @Override
        public @Nullable T postQuery(@Nonnull ExtendedResultSet resultSet) throws Exception {
            return null;
        }

        @Override
        public @Nullable T processRow(@Nonnull ExtendedResultSet resultSet) throws Exception {
            return null;
        }

        @Override
        public @Nonnull T getResult() throws Exception {
            throw new RuntimeException();
        }
    }

    public static abstract class AbstractMappableAggregator<T, R> extends AbstractSQLRowAggregator<T> {
        private final Mapper<R> mapper;

        public AbstractMappableAggregator(Mapper<R> mapper) {
            this.mapper = mapper;
        }

        @Override
        public final @Nullable T processRow(@Nonnull ExtendedResultSet resultSet) throws Exception {
            return processMappedRow(mapper.map(resultSet));
        }

        public abstract @Nullable T processMappedRow(R row) throws Exception;
    }

    public static class MappableRowAggregator<T, R> extends AbstractMappableAggregator<T, R> {
        private final RowAggregator<? extends T, ? super R> aggregator;

        public MappableRowAggregator(Mapper<R> mapper, RowAggregator<? extends T, ? super R> aggregator) {
            super(mapper);
            this.aggregator = aggregator;
        }

        @Override
        public @Nullable T processMappedRow(R row) throws Exception {
            return aggregator.processRow(row);
        }

        @Override
        public @Nonnull T getResult() throws Exception {
            return aggregator.getResult();
        }
    }

    public static abstract class ResultListKeyAggregator<T> extends AbstractSQLRowAggregator<ResultList<T>> {
        private final Object[] keyColumns;
        private final int[] keyIndexes;
        private final Map<Object, T> resultMap;
        private final ResultCollection<?> keyResultCollection;

        public ResultListKeyAggregator(Object ... keyColumns) {
            this(null, keyColumns);
        }

        public ResultListKeyAggregator(@Nullable ResultCollection<?> keyResultCollection, Object ... keyColumns) {
            this.keyColumns = keyColumns;
            this.keyIndexes = new int[keyColumns.length];
            this.resultMap = new LinkedHashMap<>();
            this.keyResultCollection = keyResultCollection;
        }

        @Override
        public @Nullable ResultList<T> postQuery(@Nonnull ExtendedResultSet resultSet) throws Exception {
            for (int i = 0; i < keyColumns.length; i++) {
                if (keyColumns[i] instanceof Number) {
                    keyIndexes[i] = ((Number) keyColumns[i]).intValue();
                }
                else {
                    keyIndexes[i] = resultSet.findColumn(keyColumns[i].toString());
                }
            }
            return null;
        }

        @Override
        public @Nullable ResultList<T> processRow(@Nonnull ExtendedResultSet resultSet) throws Exception {
            Object key;
            if (keyIndexes.length == 0) {
                key = new Object();
            }
            else if (keyIndexes.length == 1) {
                key = resultSet.getObject(keyIndexes[0]);
            }
            else {
                Object[] array = new Object[keyIndexes.length];
                for (int i = 0; i < keyIndexes.length; i++) {
                    array[i] = resultSet.getObject(keyIndexes[i]);
                }
                key = Arrays.asList(array);
            }
            T currentRow = resultMap.get(key);
            T newRow = processRow(resultSet, currentRow);
            if (currentRow != newRow) {
                resultMap.put(key, newRow);
            }
            return null;
        }

        @Override
        public @Nonnull ResultList<T> getResult() throws Exception {
            return keyResultCollection == null ? new ResultArrayList<>(null, null, null, resultMap.values())
                                               : new ResultArrayList<>(keyResultCollection.getFoundRows(),
                                                                       keyResultCollection.getOffset(),
                                                                       keyResultCollection.getLimit(),
                                                                       resultMap.values());
        }

        protected abstract T processRow(@Nonnull ExtendedResultSet resultSet, @Nullable T currentRow) throws SQLException;
    }

    public interface GeneratedKeyMapper<T> {
        @Nonnull T map(@Nonnull ExtendedResultSet resultSet) throws SQLException;
    }

    public interface KnownMeaningOfLife<T> {
        T withMeaningOfLife();
    }

    static abstract class AbstractDataSource implements DataSource {
        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            throw new UnsupportedOperationException();
        }
    }
}
