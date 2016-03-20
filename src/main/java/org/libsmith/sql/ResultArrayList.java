package org.libsmith.sql;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
 * @created 22.01.2015 18:34
 */
public class ResultArrayList<T> extends ArrayList<T> implements ResultList<T> {
    private static final long serialVersionUID = 8425154677980640595L;

    private final Long offset;
    private final Integer limit;
    private final Integer foundRows;

    @SuppressWarnings("unused")
    public ResultArrayList() {
        this(null, null, null);
    }

    @SuppressWarnings("unused")
    public ResultArrayList(Integer foundRows) {
        this(foundRows, null, null);
    }

    public ResultArrayList(Integer foundRows, Long offset, Integer limit) {
        this(foundRows, offset, limit, null);
    }

    public ResultArrayList(Integer foundRows, Long offset, Integer limit, Collection<? extends T> collection) {
        super(collection == null ? Collections.emptyList() : collection);
        this.offset = offset;
        this.limit = limit;
        this.foundRows = foundRows;
    }

    @Override
    public Long getOffset() {
        return offset;
    }

    @Override
    public Integer getLimit() {
        return limit;
    }

    @Override
    public Integer getFoundRows() {
        return foundRows;
    }

    @Override
    public T getSingleResultOr(T defaultValue) throws IllegalStateException {
        int size = size();
        if (size == 1) {
            return get(0);
        }
        else if (size == 0) {
            return defaultValue;
        }
        //noinspection Contract
        throw new IllegalStateException();
    }

    @Override
    public T getFirstResultOr(T defaultValue) {
        return isEmpty() ? defaultValue : get(0);
    }

    @Override
    @SuppressWarnings("unchecked")
    public @Nonnull <E extends Exception> T getSingleResultOrThrow(Class<E> throwableType) throws E {
        int size = size();
        if (size == 1) {
            return get(0);
        }
        else if (size == 0) {
            try {
                throw throwableType.newInstance();
            }
            catch (IllegalAccessException | InstantiationException ex) {
                throw (E) ex;
            }
        }
        throw new IllegalStateException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public @Nonnull <E extends Exception> T getFirstResultOrThrow(Class<E> throwableType) throws E {
        if (isEmpty()) {
            try {
                throw throwableType.newInstance();
            } catch (IllegalAccessException | InstantiationException ex) {
                throw (E) ex;
            }
        }
        return get(0);
    }
}
