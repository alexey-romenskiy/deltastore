package codes.writeonce.deltastore.api.map;

import javax.annotation.Nullable;

abstract class AbstractSimpleIterator<K, V, M extends AbstractTreeMap<K, V, M>> extends
        AbstractEntryIterator<K, V, M, V> {

    public abstract int nextEntry();

    @Override
    public boolean skip() {
        return nextEntry() != 0;
    }

    @Override
    public int skip(int count) {
        int skipped = 0;
        while (skipped < count && nextEntry() != 0) {
            skipped++;
        }
        return skipped;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    @Override
    public final V get() {
        return (V) map.values[nextEntry()];
    }
}
