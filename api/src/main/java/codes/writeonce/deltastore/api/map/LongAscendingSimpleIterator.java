package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class LongAscendingSimpleIterator<V> extends AbstractLongAscendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<LongAscendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, LongAscendingSimpleIterator::new);

    @SuppressWarnings("unchecked")
    public static <V> LongAscendingSimpleIterator<V> create(boolean exclusive) {
        final LongAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> LongAscendingSimpleIterator<V> create(boolean exclusive, long fromKey) {
        final LongAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey);
        return value;
    }

    private LongAscendingSimpleIterator() {
        // empty
    }

    @Override
    protected boolean nullEnded() {
        return false;
    }

    @Override
    protected boolean ended(long key) {
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
