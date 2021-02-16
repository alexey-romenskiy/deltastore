package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

final class BigDecimalAscendingChainedIterator<V, T> extends AbstractBigDecimalAscendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<BigDecimalAscendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, BigDecimalAscendingChainedIterator::new);

    @SuppressWarnings("unchecked")
    public static <V, T> BigDecimalAscendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive) {
        final BigDecimalAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> BigDecimalAscendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, @Nonnull BigDecimal fromKey) {
        final BigDecimalAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey);
        return value;
    }

    private BigDecimalAscendingChainedIterator() {
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
