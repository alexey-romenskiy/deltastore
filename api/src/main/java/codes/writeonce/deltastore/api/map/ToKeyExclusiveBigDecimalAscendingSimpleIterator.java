package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

final class ToKeyExclusiveBigDecimalAscendingSimpleIterator<V> extends AbstractBigDecimalAscendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveBigDecimalAscendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveBigDecimalAscendingSimpleIterator::new);

    private BigDecimal toKey;

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveBigDecimalAscendingSimpleIterator<V> create(boolean exclusive,
            @Nonnull BigDecimal toKey) {
        final ToKeyExclusiveBigDecimalAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveBigDecimalAscendingSimpleIterator<V> create(boolean exclusive,
            @Nonnull BigDecimal fromKey, @Nonnull BigDecimal toKey) {
        final ToKeyExclusiveBigDecimalAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyExclusiveBigDecimalAscendingSimpleIterator() {
        // empty
    }

    protected void init(boolean exclusive, @Nonnull BigDecimal toKey) {
        super.init(exclusive);
        this.toKey = toKey;
    }

    private void init(boolean exclusive, @Nonnull BigDecimal fromKey, @Nonnull BigDecimal toKey) {
        super.init(exclusive, fromKey);
        this.toKey = toKey;
    }

    @Override
    protected boolean ended() {
        return toKey.compareTo((BigDecimal) map.keys[index]) <= 0;
    }

    @Override
    protected boolean nullEnded() {
        return false;
    }

    @Override
    protected boolean ended(@Nonnull BigDecimal key) {
        return toKey.compareTo(key) <= 0;
    }

    @Override
    public void close() {
        super.close();
        this.toKey = null;
        POOL.put(this);
    }
}
