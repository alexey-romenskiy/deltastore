package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class EnumAscendingSimpleIterator<V> extends AbstractEnumAscendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<EnumAscendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, EnumAscendingSimpleIterator::new);

    @SuppressWarnings("unchecked")
    public static <V> EnumAscendingSimpleIterator<V> create(boolean exclusive) {
        final EnumAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> EnumAscendingSimpleIterator<V> create(boolean exclusive, int fromKey) {
        final EnumAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey);
        return value;
    }

    private EnumAscendingSimpleIterator() {
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
