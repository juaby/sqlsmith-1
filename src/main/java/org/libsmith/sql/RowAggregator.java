package org.libsmith.sql;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
 * @created 28.04.15 0:41
 */
public interface RowAggregator<T, R> {
    @Nullable T processRow(R row) throws Exception;
    @Nonnull T getResult() throws Exception;
}
