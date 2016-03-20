package org.libsmith.sql;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

/**
 * Коллекция, которая отражает суть результата запроса данных и которая не содержит <code>null</code> значений
 *
 * @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
 * @created 22.01.2015 18:32
 */
public interface ResultCollection<T> extends Collection<T> {

    /**
     * Возвращает метаинформацию о сдвиге данной коллекции отностительно оригинальной
     */
    @Nullable Long getOffset();

    /**
     * Возвращает метаинформацию об ограничении данной коллекции отностительно оригинальной
     */
    @Nullable Integer getLimit();

    /**
     * Возвращает метаинформацию об общем количестве строк в оригинальной коллекции
     */
    @Nullable Integer getFoundRows();

    /**
     * Возвращает первый элемент из списка или {@code defaultValue}, если это пустая коллекция. В случае, если размер
     * коллекции содержит более одного элемента, то кидается {@link IllegalStateException}
     *
     * @param defaultValue значение по-умолчанию
     * @return первый элемент коллекции или defaultValue
     * @throws IllegalStateException в случае, если size() > 1
     */
    //@Contract("null -> _; !null -> !null")
    T getSingleResultOr(T defaultValue) throws IllegalStateException;

    /**
     * Возвращает первый элемент из списка или кидается исключение {@code E}, если это пустая коллекция. В случае,
     * если размер коллекции содержит более одного элемента, то кидается {@link IllegalStateException}
     *
     * @return первый элемент коллекции или defaultValue
     * @throws E throwableType.newInstance() в случае, если size() == 0
     *         или {@link IllegalStateException}, если size() > 1
     */
    @Nonnull
    <E extends Exception> T getSingleResultOrThrow(Class<E> throwableType) throws E;

    /**
     * Возвращает первый элемент из списка или {@code defaultValue}, если это пустая коллекция.
     *
     * @param defaultValue значение по-умолчанию
     * @return первый элемент коллекции или defaultValue
     */
    //@Contract("null -> _; !null -> !null")
    T getFirstResultOr(T defaultValue);

    /**
     * Возвращает первый элемент из списка или кидается исключение {@code E}, если это пустая коллекция.
     *
     * @return первый элемент коллекции
     * @throws E throwableType.newInstance() в случае, если size() == 0
     *           или {@link IllegalStateException}, если size() > 1
     */
    @Nonnull
    <E extends Exception> T getFirstResultOrThrow(Class<E> throwableType) throws E;
}
