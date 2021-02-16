package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

final class ToKeyExclusiveBigDecimalDescendingSimpleIterator<V> extends AbstractBigDecimalDescendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveBigDecimalDescendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveBigDecimalDescendingSimpleIterator::new);

    private BigDecimal toKey;

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveBigDecimalDescendingSimpleIterator<V> create(@Nonnull BigDecimal toKey) {
        final ToKeyExclusiveBigDecimalDescendingSimpleIterator<V> value = POOL.get();
        value.init(toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveBigDecimalDescendingSimpleIterator<V> create(boolean exclusive,
            @Nonnull BigDecimal fromKey, @Nonnull BigDecimal toKey) {
        final ToKeyExclusiveBigDecimalDescendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyExclusiveBigDecimalDescendingSimpleIterator() {
        // empty
    }

    private void init(@Nonnull BigDecimal toKey) {
        super.init();
        this.toKey = toKey;
    }

    private void init(boolean exclusive, @Nonnull BigDecimal fromKey, @Nonnull BigDecimal toKey) {
        super.init(exclusive, fromKey);
        this.toKey = toKey;
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
    protected boolean ended(@Nonnull BigDecimal key) {
        return toKey.compareTo(key) >= 0;
    }

    @Override
    public void close() {
        super.close();
        this.toKey = null;
        POOL.put(this);
    }
}
