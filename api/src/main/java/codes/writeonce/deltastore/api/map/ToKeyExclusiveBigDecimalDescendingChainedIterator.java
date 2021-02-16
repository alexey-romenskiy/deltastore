package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

final class ToKeyExclusiveBigDecimalDescendingChainedIterator<V, T>
        extends AbstractBigDecimalDescendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveBigDecimalDescendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveBigDecimalDescendingChainedIterator::new);

    private BigDecimal toKey;

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyExclusiveBigDecimalDescendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, @Nonnull BigDecimal toKey) {
        final ToKeyExclusiveBigDecimalDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyExclusiveBigDecimalDescendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, boolean exclusive, @Nonnull BigDecimal fromKey,
            @Nonnull BigDecimal toKey) {
        final ToKeyExclusiveBigDecimalDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyExclusiveBigDecimalDescendingChainedIterator() {
        // empty
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, @Nonnull BigDecimal toKey) {
        super.init(iterator);
        this.toKey = toKey;
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, @Nonnull BigDecimal fromKey,
            @Nonnull BigDecimal toKey) {
        super.init(iterator, exclusive, fromKey);
        this.toKey = toKey;
    }

    @Override
    protected boolean ended(@Nonnull BigDecimal key) {
        return toKey.compareTo(key) >= 0;
    }

    @Override
    protected boolean ended() {
        return toKey.compareTo((BigDecimal) map.keys[index]) >= 0;
    }

    @Override
    protected boolean nullEnded() {
        return true;
    }

    @Override
    public void close() {
        super.close();
        this.toKey = null;
        POOL.put(this);
    }
}
