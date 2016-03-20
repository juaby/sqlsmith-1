package org.libsmith.sql;

import java.util.Collection;
import java.util.Optional;

/**
 * @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
 * @created 08.11.2015 2:58
 */
public class ResultArrayListOfOptionals<T> extends ResultArrayList<Optional<T>> implements ResultListOfOptionals<T> {
    private static final long serialVersionUID = 3354464529411935713L;

    public ResultArrayListOfOptionals()
    { }

    public ResultArrayListOfOptionals(Integer foundRows) {
        super(foundRows);
    }

    public ResultArrayListOfOptionals(Integer foundRows, Long offset, Integer limit) {
        super(foundRows, offset, limit);
    }

    public ResultArrayListOfOptionals(Integer foundRows, Long offset, Integer limit,
                                      Collection<? extends Optional<T>> collection) {
        super(foundRows, offset, limit, collection);
    }

    @Override
    public Optional<T> getFirstResult() {
        return getFirstResultOr(Optional.empty());
    }

    @Override
    public Optional<T> getSingleResult() throws IllegalStateException {
        return getSingleResultOr(Optional.empty());
    }
}
