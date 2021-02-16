package codes.writeonce.deltastore.api.map;

abstract class AbstractEntryIterator<K, V, M extends AbstractTreeMap<K, V, M>, T> extends NestedIterator<M, T> {

    protected M map;
    protected int state;
    protected int index;
    protected int expectedModCount;

    protected abstract void resetKey();

    protected abstract int ceiling();

    protected abstract int higher();

    protected abstract void updateLastKey();

    protected abstract boolean sameKey();

    protected abstract boolean ended();

    protected abstract boolean nullEnded();

    @Override
    public void close() {
        super.close();
        this.map = null;
    }
}
