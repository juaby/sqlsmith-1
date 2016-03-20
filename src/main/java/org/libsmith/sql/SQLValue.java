package org.libsmith.sql;

import javax.annotation.Nullable;

/**
 * @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
 * @created 05.03.2015 17:05
 */
public interface SQLValue<T> {
    @Nullable T getSQLValueEntity();
}
