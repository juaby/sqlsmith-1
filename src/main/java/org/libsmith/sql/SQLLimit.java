package org.libsmith.sql;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
 * @created 14.02.2015 21:00
 */
public class SQLLimit implements SQLFragment {
    private final Long offset;
    private final Integer limit;

    public SQLLimit(@Nullable Long offset, @Nullable Integer limit) {
        this.offset = offset;
        this.limit = limit;
    }

    public SQLLimit(@Nullable Integer offset, @Nullable Integer limit) {
        this.offset = offset == null ? null : offset.longValue();
        this.limit = limit;
    }

    @Override
    public @Nonnull String getFragment() {
        int limit = this.limit == null ? Integer.MAX_VALUE : this.limit;
        return offset == null ? Integer.toString(limit) : (offset + ", " + limit);
    }

    @Override
    public @Nonnull List<Object> getParameters () {
        return Collections.emptyList();
    }

    public Integer getLimit() {
        return limit;
    }

    public Long getOffset() {
        return offset;
    }
}
