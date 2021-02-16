package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;
import java.time.Instant;

abstract class AbstractInstantDescendingChainedIterator<V, T>
        extends DescendingChainedIterator<Instant, V, InstantTreeMap<V>, T> {

    protected boolean init;

    protected boolean exclusive;

    private long fromKey1;

    private int fromKey2;

    private boolean fromNull;

    protected long lastKey1;

    protected int lastKey2;

    protected void init(@Nonnull NestedIterator<V, T> iterator) {
        super.init(iterator);
        this.init = true;
    }

    protected void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, long fromKey1, int fromKey2) {
        super.init(iterator);
        this.init = false;
        this.exclusive = exclusive;
        this.fromKey1 = fromKey1;
        this.fromKey2 = fromKey2;
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
            if (ended(fromKey1, fromKey2)) {
                state = STATE_ENDED;
            } else {
                lastKey1 = fromKey1;
                lastKey2 = fromKey2;
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
        return map.getFloorEntry(lastKey1, lastKey2);
    }

    @Override
    protected int higher() {
        return map.getLowerEntry(lastKey1, lastKey2);
    }

    @Override
    protected void updateLastKey() {
        lastKey1 = map.keys1[index];
        lastKey2 = map.keys2[index];
    }

    @Override
    protected boolean sameKey() {
        return lastKey1 == map.keys1[index] && lastKey2 == map.keys2[index];
    }

    protected abstract boolean ended(long key1, int key2);
}
