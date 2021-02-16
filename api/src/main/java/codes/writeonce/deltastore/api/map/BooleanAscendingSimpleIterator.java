package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class BooleanAscendingSimpleIterator<V> extends AbstractBooleanAscendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<BooleanAscendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, BooleanAscendingSimpleIterator::new);

    @SuppressWarnings("unchecked")
    public static <V> BooleanAscendingSimpleIterator<V> create(boolean exclusive) {
        final BooleanAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> BooleanAscendingSimpleIterator<V> create(boolean exclusive, boolean fromKey) {
        final BooleanAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey);
        return value;
    }

    private BooleanAscendingSimpleIterator() {
        // empty
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
    protected boolean ended() {
        return false;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
