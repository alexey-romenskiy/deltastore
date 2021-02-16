package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class InstantAscendingSimpleIterator<V> extends AbstractInstantAscendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<InstantAscendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, InstantAscendingSimpleIterator::new);

    @SuppressWarnings("unchecked")
    public static <V> InstantAscendingSimpleIterator<V> create(boolean exclusive) {
        final InstantAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> InstantAscendingSimpleIterator<V> create(boolean exclusive, long fromKey1, int fromKey2) {
        final InstantAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey1, fromKey2);
        return value;
    }

    private InstantAscendingSimpleIterator() {
        // empty
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
    protected boolean ended() {
        return false;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
