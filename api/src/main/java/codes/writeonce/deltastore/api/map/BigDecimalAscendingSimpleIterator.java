package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

final class BigDecimalAscendingSimpleIterator<V> extends AbstractBigDecimalAscendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<BigDecimalAscendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, BigDecimalAscendingSimpleIterator::new);

    @SuppressWarnings("unchecked")
    public static <V> BigDecimalAscendingSimpleIterator<V> create(boolean exclusive) {
        final BigDecimalAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> BigDecimalAscendingSimpleIterator<V> create(boolean exclusive, @Nonnull BigDecimal fromKey) {
        final BigDecimalAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey);
        return value;
    }

    private BigDecimalAscendingSimpleIterator() {
        // empty
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
    protected boolean ended() {
        return false;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
