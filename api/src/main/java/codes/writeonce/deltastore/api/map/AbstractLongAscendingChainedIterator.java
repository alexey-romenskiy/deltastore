package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;

abstract class AbstractLongAscendingChainedIterator<V, T> extends AscendingChainedIterator<Long, V, LongTreeMap<V>, T> {

    protected boolean exclusive;

    private long fromKey;

    private boolean fromNull;

    protected long lastKey;

    protected void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive) {
        super.init(iterator);
        this.exclusive = exclusive;
        this.fromNull = true;
    }

    protected void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, long fromKey) {
        super.init(iterator);
        this.exclusive = exclusive;
        this.fromKey = fromKey;
        this.fromNull = false;
    }

    @Override
    protected void resetKey() {

        if (fromNull) {
            if (nullEnded()) {
                state = STATE_ENDED;
            } else if (exclusive) {
                state = STATE_NULL_EXCLUSIVE;
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
        return map.getCeilingEntry(lastKey);
    }

    @Override
    protected int higher() {
        return map.getHigherEntry(lastKey);
    }

    @Override
    protected void updateLastKey() {
        lastKey = map.keys[index];
    }

    @Override
    protected boolean sameKey() {
        return lastKey == map.keys[index];
    }

    protected abstract boolean ended(long key);
}
