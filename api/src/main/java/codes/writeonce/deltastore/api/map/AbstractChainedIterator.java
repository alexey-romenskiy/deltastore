package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;

abstract class AbstractChainedIterator<K, V, M extends AbstractTreeMap<K, V, M>, T>
        extends AbstractEntryIterator<K, V, M, T> {

    protected NestedIterator<V, T> iterator;

    protected void init(@Nonnull NestedIterator<V, T> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean skip() {
        return get() != null;
    }

    @Override
    public int skip(int count) {
        int skipped = 0;
        while (skipped < count && get() != null) {
            skipped++;
        }
        return skipped;
    }

    protected void resetIterator() {
        iterator.reset(value());
    }

    protected void mapIterator() {
        iterator.map(value());
    }

    protected void resetOrMapIterator() {
        if (sameKey()) {
            mapIterator();
        } else {
            resetIterator();
        }
    }

    @SuppressWarnings("unchecked")
    private V value() {
        return (V) map.values[index];
    }

    @Override
    public void close() {
        super.close();
        iterator.close();
        iterator = null;
    }
}
