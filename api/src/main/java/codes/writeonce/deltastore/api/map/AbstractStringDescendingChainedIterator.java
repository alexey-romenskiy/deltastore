package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

abstract class AbstractStringDescendingChainedIterator<V, T>
        extends DescendingChainedIterator<String, V, StringTreeMap<V>, T> {

    protected boolean init;

    protected boolean exclusive;

    @Nullable
    private String fromKey;

    @Nullable
    protected String lastKey;

    protected void init(@Nonnull NestedIterator<V, T> iterator) {
        super.init(iterator);
        this.init = true;
    }

    protected void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, @Nonnull String fromKey) {
        super.init(iterator);
        this.init = false;
        this.exclusive = exclusive;
        this.fromKey = fromKey;
    }

    @Override
    protected void resetKey() {

        if (init) {
            state = STATE_INIT;
        } else if (fromKey == null) {
            if (exclusive || nullEnded()) {
                state = STATE_ENDED;
            } else {
                state = STATE_NULL_INCLUSIVE_RESET;
            }
        } else {
            if (ended(fromKey)) {
                state = STATE_ENDED;
            } else {
                lastKey = fromKey;
                if (exclusive) {
                    state = STATE_LAST_EXCLUSIVE;
                } else {
                    state = STATE_LAST_INCLUSIVE_RESET;
                }
            }
        }
    }

    @Override
    protected int ceiling() {
        assert lastKey != null;
        return map.getFloorEntry(lastKey);
    }

    @Override
    protected int higher() {
        assert lastKey != null;
        return map.getLowerEntry(lastKey);
    }

    @Override
    protected void updateLastKey() {
        lastKey = (String) map.keys[index];
    }

    @Override
    protected boolean sameKey() {
        assert lastKey != null;
        return lastKey.equals(map.keys[index]);
    }

    protected abstract boolean ended(@Nonnull String key);

    @Override
    public void close() {
        super.close();
        this.fromKey = null;
        this.lastKey = null;
    }
}
