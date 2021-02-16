package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class EnumDescendingSimpleIterator<V> extends AbstractEnumDescendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<EnumDescendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, EnumDescendingSimpleIterator::new);

    @SuppressWarnings("unchecked")
    public static <V> EnumDescendingSimpleIterator<V> create() {
        final EnumDescendingSimpleIterator<V> value = POOL.get();
        value.init();
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> EnumDescendingSimpleIterator<V> create(boolean exclusive, int fromKey) {
        final EnumDescendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey);
        return value;
    }

    private EnumDescendingSimpleIterator() {
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
