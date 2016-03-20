package org.libsmith.sql;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

/**
 * @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
 * @created 08.05.2015 21:30
 */
class ResultListWrapper<T> extends ResultCollectionWrapper<T> implements ResultList<T> {
    private final List<T> delegate;

    ResultListWrapper(List<T> delegate, Integer foundRows, Long offset, Integer limit) {
        super(delegate, foundRows, offset, limit);
        this.delegate = delegate;
    }

    @Override
    protected T getFirst() {
        return delegate.get(0);
    }

    @Override
    public boolean addAll(int index, @Nonnull Collection<? extends T> c) {
        return delegate.addAll(index, c);
    }

    @Override
    public T get(int index) {
        return delegate.get(index);
    }

    @Override
    public T set(int index, T element) {
        return delegate.set(index, element);
    }

    @Override
    public void add(int index, T element) {
        delegate.add(index, element);
    }

    @Override
    public T remove(int index) {
        return delegate.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return delegate.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return delegate.lastIndexOf(o);
    }

    @Override
    public @Nonnull ListIterator<T> listIterator() {
        return delegate.listIterator();
    }

    @Override
    public @Nonnull ListIterator<T> listIterator(int index) {
        return delegate.listIterator(index);
    }

    @Override
    public @Nonnull List<T> subList(int fromIndex, int toIndex) {
        return delegate.subList(fromIndex, toIndex);
    }
}
