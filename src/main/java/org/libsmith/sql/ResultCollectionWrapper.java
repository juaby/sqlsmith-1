package org.libsmith.sql;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

/**
 * @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
 * @created 08.05.2015 21:05
 */
class ResultCollectionWrapper<T> implements ResultCollection<T> {
    private final Collection<T> delegate;
    private final Integer foundRows;
    private final Long offset;
    private final Integer limit;

    ResultCollectionWrapper(Collection<T> delegate, Integer foundRows, Long offset, Integer limit) {
        this.delegate = delegate;
        this.offset = offset;
        this.limit = limit;
        this.foundRows = foundRows;
    }

    @Override
    public Integer getFoundRows() {
        return foundRows;
    }

    @Override
    public Long getOffset() {
        return offset;
    }

    @Override
    public Integer getLimit() {
        return limit;
    }

    protected T getFirst() {
        return iterator().next();
    }

    @Override
    public T getSingleResultOr(T defaultValue) {
        int size = size();
        if (size == 1) {
            return getFirst();
        }
        else if (size == 0) {
            return defaultValue;
        }
        //noinspection Contract
        throw new IllegalStateException();
    }

    @Override
    public T getFirstResultOr(T defaultValue) {
        return isEmpty() ? defaultValue : getFirst();
    }

    @Override
    @SuppressWarnings("unchecked")
    public @Nonnull <E extends Exception> T getSingleResultOrThrow(Class<E> throwableType) throws E {
        int size = size();
        if (size == 1) {
            return getFirst();
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
            }
            catch (IllegalAccessException | InstantiationException ex) {
                throw (E) ex;
            }
        }
        return getFirst();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ResultCollectionWrapper)) return false;
        ResultCollectionWrapper<?> that = (ResultCollectionWrapper<?>) o;
        return Objects.equals(delegate, that.delegate) &&
                Objects.equals(foundRows, that.foundRows) &&
                Objects.equals(offset, that.offset) &&
                Objects.equals(limit, that.limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate, foundRows, offset, limit);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return delegate.contains(o);
    }

    @Override
    public @Nonnull Iterator<T> iterator() {
        return delegate.iterator();
    }

    @Override
    public @Nonnull Object[] toArray() {
        return delegate.toArray();
    }

    @Override
    public @Nonnull <T1> T1[] toArray(@Nonnull T1[] a) {
        //noinspection SuspiciousToArrayCall
        return delegate.toArray(a);
    }

    @Override
    public boolean add(T t) {
        return delegate.add(t);
    }

    @Override
    public boolean remove(Object o) {
        return delegate.remove(o);
    }

    @Override
    public boolean containsAll(@Nonnull Collection<?> c) {
        return delegate.containsAll(c);
    }

    @Override
    public boolean addAll(@Nonnull Collection<? extends T> c) {
        return delegate.addAll(c);
    }

    @Override
    public boolean removeAll(@Nonnull Collection<?> c) {
        return delegate.removeAll(c);
    }

    @Override
    public boolean retainAll(@Nonnull Collection<?> c) {
        return delegate.retainAll(c);
    }

    @Override
    public void clear() {
        delegate.clear();
    }
}
