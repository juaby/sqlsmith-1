package org.libsmith.sql;

import java.util.Optional;

/**
 * @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
 * @created 08.11.2015 2:55
 */
public interface ResultListOfOptionals<T> extends ResultList<Optional<T>> {
    /**
     * Возвращает первый элемент из списка или {@link Optional#empty()}, если это пустая коллекция. В случае, если размер
     * коллекции содержит более одного элемента, то кидается {@link IllegalStateException}
     */
    Optional<T> getSingleResult() throws IllegalStateException;

    /**
     * Возвращает первый элемент из списка или {@link Optional#empty()}, если это пустая коллекция.
     */
    Optional<T> getFirstResult();
}
