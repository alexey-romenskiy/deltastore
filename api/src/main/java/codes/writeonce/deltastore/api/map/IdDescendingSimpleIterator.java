package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class IdDescendingSimpleIterator<V> extends AbstractIdDescendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<IdDescendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, IdDescendingSimpleIterator::new);

    @SuppressWarnings("unchecked")
    public static <V> IdDescendingSimpleIterator<V> create() {
        final IdDescendingSimpleIterator<V> value = POOL.get();
        value.init();
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> IdDescendingSimpleIterator<V> create(boolean exclusive, long fromKey) {
        final IdDescendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey);
        return value;
    }

    private IdDescendingSimpleIterator() {
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
