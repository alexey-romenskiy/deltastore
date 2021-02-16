package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class LongDescendingSimpleIterator<V> extends AbstractLongDescendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<LongDescendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, LongDescendingSimpleIterator::new);

    @SuppressWarnings("unchecked")
    public static <V> LongDescendingSimpleIterator<V> create() {
        final LongDescendingSimpleIterator<V> value = POOL.get();
        value.init();
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> LongDescendingSimpleIterator<V> create(boolean exclusive, long fromKey) {
        final LongDescendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey);
        return value;
    }

    private LongDescendingSimpleIterator() {
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
    protected boolean ended(long key) {
        return false;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
