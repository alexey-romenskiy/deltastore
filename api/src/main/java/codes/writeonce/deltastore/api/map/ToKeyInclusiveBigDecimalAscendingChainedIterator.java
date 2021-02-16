package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

final class ToKeyInclusiveBigDecimalAscendingChainedIterator<V, T>
        extends AbstractBigDecimalAscendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyInclusiveBigDecimalAscendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyInclusiveBigDecimalAscendingChainedIterator::new);

    private BigDecimal toKey;

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyInclusiveBigDecimalAscendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, boolean exclusive, @Nonnull BigDecimal toKey) {
        final ToKeyInclusiveBigDecimalAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyInclusiveBigDecimalAscendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, boolean exclusive, @Nonnull BigDecimal fromKey,
            @Nonnull BigDecimal toKey) {
        final ToKeyInclusiveBigDecimalAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyInclusiveBigDecimalAscendingChainedIterator() {
        // empty
    }

    protected void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, @Nonnull BigDecimal toKey) {
        super.init(iterator, exclusive);
        this.toKey = toKey;
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, @Nonnull BigDecimal fromKey,
            @Nonnull BigDecimal toKey) {
        super.init(iterator, exclusive, fromKey);
        this.toKey = toKey;
    }

    @Override
    protected boolean ended() {
        return toKey.compareTo((BigDecimal) map.keys[index]) < 0;
    }

    @Override
    protected boolean nullEnded() {
        return false;
    }

    @Override
    protected boolean ended(@Nonnull BigDecimal key) {
        return toKey.compareTo(key) < 0;
    }

    @Override
    public void close() {
        super.close();
        this.toKey = null;
        POOL.put(this);
    }
}
