package org.libsmith.sql;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Dmitriy Balakin <dmitriy.balakin@0x0000.ru>
 * @created 14.02.2015 21:00
 */
public interface SQLFragment {
    @Nonnull String getFragment();
    @Nonnull List<Object> getParameters();

    class Builder implements Appendable {
        private StringBuilder stringBuilder = new StringBuilder();
        private List<Object> parameters = new ArrayList<>();

        @Override
        public Builder append(CharSequence csq) {
            stringBuilder.append(csq);
            return this;
        }

        @Override
        public Builder append(CharSequence csq, int start, int end) {
            stringBuilder.append(csq, start, end);
            return this;
        }

        @Override
        public Builder append(char c) {
            stringBuilder.append(c);
            return this;
        }

        public Builder append(Object object) {
            stringBuilder.append("?");
            parameters.add(object);
            return this;
        }

        public Builder append(@Nonnull SQLFragment sqlFragment) {
            stringBuilder.append(sqlFragment.getFragment());
            parameters.addAll(sqlFragment.getParameters());
            return this;
        }

        public Builder append(@Nonnull SQLValue<?> sqlValue) {
            stringBuilder.append("?");
            parameters.add(sqlValue.getSQLValueEntity());
            return this;
        }

        public Builder smartAppend(Object object) {
            if (object instanceof SQLFragment) {
                return append((SQLFragment) object);
            }
            else if (object instanceof SQLValue) {
                return append((SQLValue) object);
            }
            else {
                return append(object);
            }
        }

        public SQLFragment build() {
            return new Impl(stringBuilder.toString(), new ArrayList<>(parameters));
        }
    }

    class Impl implements SQLFragment {

        private final String fragment;
        private final List<Object> parameters;

        public Impl(@Nonnull String fragment, @Nonnull List<Object> parameters) {
            this.fragment = fragment;
            this.parameters = parameters;
        }

        @Override
        public @Nonnull String getFragment() {
            return fragment;
        }

        @Override
        public @Nonnull List<Object> getParameters() {
            return parameters;
        }

        public static @Nonnull SQLFragment ofJoinedFragments(@Nonnull SQLFragment ... fragments) {
            return ofJoinedFragments("", fragments);
        }

        public static @Nonnull SQLFragment ofJoinedFragments(@Nonnull CharSequence separator,
                                                             @Nonnull SQLFragment ... fragments) {
            return ofJoinedFragments(separator, Arrays.asList(fragments));
        }

        public static @Nonnull SQLFragment ofJoinedFragments(@Nonnull CharSequence separator,
                                                             @Nonnull Iterable<SQLFragment> fragments) {
            Builder builder = new Builder();
            int i = 0;
            for (SQLFragment fragment : fragments) {
                if (fragment != null) {
                    if (i++ > 0) {
                        builder.append(separator);
                    }
                    builder.append(fragment);
                }
            }
            return builder.build();
        }

        public static SQLFragment of(String fragment) {
            return new Impl(fragment, Collections.emptyList());
        }

        public static SQLFragment of(String fragment, Object ... parameters) {
            return new Impl(fragment, Arrays.asList(parameters));
        }

        public static SQLFragment ofArray(@Nonnull Object[] array, Object defaultValueIfEmptyArray) {
            return ofCollection(Arrays.asList(array), defaultValueIfEmptyArray);
        }

        public static SQLFragment ofCollection(@Nonnull Iterable<?> iterable, Object defaultValueIfEmptyCollection) {
            Builder builder = new Builder();
            int i = 0;
            for (Object o : iterable) {
                if (i++ > 0) {
                    builder.append(",");
                }
                builder.smartAppend(o);
            }
            if (i == 0) {
                if (defaultValueIfEmptyCollection == null) {
                    builder.append("NULL");
                }
                else {
                    builder.smartAppend(defaultValueIfEmptyCollection);
                }
            }
            return builder.build();
        }
    }
}
