package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class IntegerDescendingSimpleIterator<V> extends AbstractIntegerDescendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<IntegerDescendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, IntegerDescendingSimpleIterator::new);

    @SuppressWarnings("unchecked")
    public static <V> IntegerDescendingSimpleIterator<V> create() {
        final IntegerDescendingSimpleIterator<V> value = POOL.get();
        value.init();
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> IntegerDescendingSimpleIterator<V> create(boolean exclusive, int fromKey) {
        final IntegerDescendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey);
        return value;
    }

    private IntegerDescendingSimpleIterator() {
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
    protected boolean ended(int key) {
        return false;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
