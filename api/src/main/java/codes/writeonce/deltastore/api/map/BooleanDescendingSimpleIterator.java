package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class BooleanDescendingSimpleIterator<V> extends AbstractBooleanDescendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<BooleanDescendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, BooleanDescendingSimpleIterator::new);

    @SuppressWarnings("unchecked")
    public static <V> BooleanDescendingSimpleIterator<V> create() {
        final BooleanDescendingSimpleIterator<V> value = POOL.get();
        value.init();
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> BooleanDescendingSimpleIterator<V> create(boolean exclusive, boolean fromKey) {
        final BooleanDescendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey);
        return value;
    }

    private BooleanDescendingSimpleIterator() {
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
    protected boolean ended(boolean key) {
        return false;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
