package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class ToKeyInclusiveIdDescendingSimpleIterator<V> extends AbstractIdDescendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyInclusiveIdDescendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyInclusiveIdDescendingSimpleIterator::new);

    private long toKey;

    @SuppressWarnings("unchecked")
    public static <V> ToKeyInclusiveIdDescendingSimpleIterator<V> create(long toKey) {
        final ToKeyInclusiveIdDescendingSimpleIterator<V> value = POOL.get();
        value.init(toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ToKeyInclusiveIdDescendingSimpleIterator<V> create(boolean exclusive, long fromKey, long toKey) {
        final ToKeyInclusiveIdDescendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyInclusiveIdDescendingSimpleIterator() {
        // empty
    }

    private void init(long toKey) {
        super.init();
        this.toKey = toKey;
    }

    private void init(boolean exclusive, long fromKey, long toKey) {
        super.init(exclusive, fromKey);
        this.toKey = toKey;
    }

    @Override
    protected boolean ended() {
        return toKey > map.keys[index];
    }

    @Override
    protected boolean nullEnded() {
        return true;
    }

    @Override
    protected boolean ended(long key) {
        return toKey > key;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
