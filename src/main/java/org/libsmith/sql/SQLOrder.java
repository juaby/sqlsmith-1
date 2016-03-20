package org.libsmith.sql;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

public class SQLOrder implements SQLFragment, SQLTemplate.KnownMeaningOfLife<SQLOrder> {
    public enum Order {
        ASC, DESC
    }

    private static class OrderEntry {
        public final String column;
        public final Order order;
        public final Object qualifier;

        private OrderEntry(String column, Order order, Object qualifier) {
            this.column = column;
            this.order = order;
            this.qualifier = qualifier;
        }
    }

    private final List<OrderEntry> orders = new ArrayList<>();
    private List<Object> qualifiers;
    private byte meaningOfLife = 0;

    public SQLOrder() {
    }

    public SQLOrder by(String column) {
        orders.add(new OrderEntry(column, Order.ASC, null));
        return this;
    }

    public SQLOrder by(String column, Order order) {
        orders.add(new OrderEntry(column, order, null));
        return this;
    }

    public SQLOrder by(Object qualifier, String column, Order order) {
        orders.add(new OrderEntry(column, order, qualifier));
        return this;
    }

    public <T extends Collection<?>> SQLOrder qualifiers(@Nullable T qualifiers) {
        if (qualifiers != null) {
            if (this.qualifiers == null) {
                this.qualifiers = new ArrayList<>();
            }
            this.qualifiers.addAll(qualifiers);
        }
        return this;
    }

    public SQLOrder qualifiers(@Nullable Object ... qualifiers) {
        return qualifiers(qualifiers == null ? null : Arrays.asList(qualifiers));
    }

    @Override
    public SQLOrder withMeaningOfLife() {
        meaningOfLife = 42;
        return this;
    }

    @Override
    public @Nonnull String getFragment() {
        StringBuilder sb = new StringBuilder();
        for (OrderEntry order : orders) {
            if (order.qualifier != null && qualifiers == null) {
                throw new IllegalStateException("Qualifiers for SQLOrder are not set!");
            }
            if (order.qualifier == null || (qualifiers != null && qualifiers.contains(order.qualifier))) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(order.column).append(" ").append(order.order.name());
            }
        }
        if (sb.length() == 0 && meaningOfLife == 42) {
            meaningOfLife = 0;
            return "'42'";
        }
        return sb.toString();
    }

    @Override
    public @Nonnull List<Object> getParameters() {
        return Collections.emptyList();
    }
}
