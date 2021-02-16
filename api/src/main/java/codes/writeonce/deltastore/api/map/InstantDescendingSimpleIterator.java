package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class InstantDescendingSimpleIterator<V> extends AbstractInstantDescendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<InstantDescendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, InstantDescendingSimpleIterator::new);

    @SuppressWarnings("unchecked")
    public static <V> InstantDescendingSimpleIterator<V> create() {
        final InstantDescendingSimpleIterator<V> value = POOL.get();
        value.init();
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> InstantDescendingSimpleIterator<V> create(boolean exclusive, long fromKey1, int fromKey2) {
        final InstantDescendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey1, fromKey2);
        return value;
    }

    private InstantDescendingSimpleIterator() {
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
    protected boolean ended(long key1, int key2) {
        return false;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
