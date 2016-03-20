package org.libsmith.sql;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
 * @created 04.06.2015 15:00
 */
public class ResultCollections {
    private static final ResultList<Object> EMPTY_LIST = wrap(Collections.emptyList());

    public static <C extends Collection<T>, T> ResultCollection<T> wrap(C collection) {
        return new ResultCollectionWrapper<>(collection, null, null, null);
    }

    public static <C extends Collection<T>, T> ResultCollection<T> wrap(C collection, Integer foundRows, Long offset, Integer limit) {
        return new ResultCollectionWrapper<>(collection, foundRows, offset, limit);
    }

    public static <L extends List<T>, T> ResultListWrapper<T> wrap(L list) {
        return new ResultListWrapper<>(list, null, null, null);
    }

    public static <L extends List<T>, T> ResultListWrapper<T> wrap(L list, Integer foundRows, Long offset, Integer limit) {
        return new ResultListWrapper<>(list, foundRows, offset, limit);
    }

    @SuppressWarnings("unchecked")
    public static <T> ResultList<T> emptyList() {
        return (ResultList<T>) EMPTY_LIST;
    }

    public static <T> ResultList<T> singletonList(T object) {
        return wrap(Collections.singletonList(object));
    }
}
