package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

final class BigDecimalDescendingChainedIterator<V, T> extends AbstractBigDecimalDescendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<BigDecimalDescendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, BigDecimalDescendingChainedIterator::new);

    @SuppressWarnings("unchecked")
    public static <V, T> BigDecimalDescendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator) {
        final BigDecimalDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> BigDecimalDescendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, @Nonnull BigDecimal fromKey) {
        final BigDecimalDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey);
        return value;
    }

    private BigDecimalDescendingChainedIterator() {
        // empty
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
    protected boolean nullEnded() {
        return false;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
