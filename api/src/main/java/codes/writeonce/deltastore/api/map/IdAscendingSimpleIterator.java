package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class IdAscendingSimpleIterator<V> extends AbstractIdAscendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<IdAscendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, IdAscendingSimpleIterator::new);

    @SuppressWarnings("unchecked")
    public static <V> IdAscendingSimpleIterator<V> create(boolean exclusive) {
        final IdAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> IdAscendingSimpleIterator<V> create(boolean exclusive, long fromKey) {
        final IdAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey);
        return value;
    }

    private IdAscendingSimpleIterator() {
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
