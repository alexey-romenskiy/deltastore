package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class IntegerAscendingSimpleIterator<V> extends AbstractIntegerAscendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<IntegerAscendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, IntegerAscendingSimpleIterator::new);

    @SuppressWarnings("unchecked")
    public static <V> IntegerAscendingSimpleIterator<V> create(boolean exclusive) {
        final IntegerAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> IntegerAscendingSimpleIterator<V> create(boolean exclusive, int fromKey) {
        final IntegerAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey);
        return value;
    }

    private IntegerAscendingSimpleIterator() {
        // empty
    }

    @Override
    protected boolean nullEnded() {
        return false;
    }

    @Override
    protected boolean ended(int key) {
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
