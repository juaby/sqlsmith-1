package org.libsmith.sql;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
 * @created 09.03.2015 13:17
 */
public class SQLTemplateFactory {
    private final DataSource dataSource;

    public SQLTemplateFactory(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public SQLTemplate template(String query) {
        return new SQLTemplate(query, dataSource);
    }

    public int execute(String query) throws SQLException {
        return new SQLTemplate(query, dataSource).executeQuery();
    }

    public RuntimeException translate(Exception exception) {
        return exception instanceof RuntimeException ? (RuntimeException) exception : new RuntimeException(exception);
    }
}
