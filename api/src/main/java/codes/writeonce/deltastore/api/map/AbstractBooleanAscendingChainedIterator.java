package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;

abstract class AbstractBooleanAscendingChainedIterator<V, T>
        extends AscendingChainedIterator<Boolean, V, BooleanTreeMap<V>, T> {

    protected boolean exclusive;

    private boolean fromKey;

    private boolean fromNull;

    protected boolean lastKey;

    protected void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive) {
        super.init(iterator);
        this.exclusive = exclusive;
        this.fromNull = true;
    }

    protected void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive,
            boolean fromKey) {
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

    protected abstract boolean ended(boolean key);
}
