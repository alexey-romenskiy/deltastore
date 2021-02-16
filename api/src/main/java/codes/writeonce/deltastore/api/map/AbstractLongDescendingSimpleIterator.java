package codes.writeonce.deltastore.api.map;

abstract class AbstractLongDescendingSimpleIterator<V> extends DescendingSimpleIterator<Long, V, LongTreeMap<V>> {

    protected boolean init;

    protected boolean exclusive;

    private long fromKey;

    private boolean fromNull;

    protected long lastKey;

    protected void init() {
        this.init = true;
    }

    protected void init(boolean exclusive, long fromKey) {
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
                state = STATE_NULL_INCLUSIVE;
            }
        } else {
            if (ended(fromKey)) {
                state = STATE_ENDED;
            } else {
                lastKey = fromKey;
                if (exclusive) {
                    state = STATE_LAST_EXCLUSIVE;
                } else {
                    state = STATE_LAST_INCLUSIVE;
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
