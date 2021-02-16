package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

final class BigDecimalDescendingSimpleIterator<V> extends AbstractBigDecimalDescendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<BigDecimalDescendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, BigDecimalDescendingSimpleIterator::new);

    @SuppressWarnings("unchecked")
    public static <V> BigDecimalDescendingSimpleIterator<V> create() {
        final BigDecimalDescendingSimpleIterator<V> value = POOL.get();
        value.init();
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> BigDecimalDescendingSimpleIterator<V> create(boolean exclusive, @Nonnull BigDecimal fromKey) {
        final BigDecimalDescendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey);
        return value;
    }

    private BigDecimalDescendingSimpleIterator() {
        // empty
    }

    @Override
    protected boolean ended() {
        return false;
    }

    @Override
    protected boolean nullEnded() {
        return false;
    }

    @Override
    protected boolean ended(@Nonnull BigDecimal key) {
        return false;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
