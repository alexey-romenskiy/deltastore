package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.Id;

import javax.annotation.Nonnull;

abstract class AbstractIdDescendingChainedIterator<V, T> extends DescendingChainedIterator<Id<?>, V, IdTreeMap<V>, T> {

    protected boolean init;

    protected boolean exclusive;

    private long fromKey;

    private boolean fromNull;

    protected long lastKey;

    protected void init(@Nonnull NestedIterator<V, T> iterator) {
        super.init(iterator);
        this.init = true;
    }

    protected void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive,
            long fromKey) {
        super.init(iterator);
        this.init = false;
        this.exclusive = exclusive;
        this.fromKey = fromKey;
        this.fromNull = false;
    }

    @Override
    protected void resetKey() {

        if (init) {
            state = STATE_INIT;
        } else if (fromNull) {
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
        return map.getFloorEntry(lastKey);
    }

    @Override
    protected int higher() {
        return map.getLowerEntry(lastKey);
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
