package codes.writeonce.deltastore.api.map;

abstract class AbstractLongAscendingSimpleIterator<V> extends AscendingSimpleIterator<Long, V, LongTreeMap<V>> {

    protected boolean exclusive;

    private long fromKey;

    private boolean fromNull;

    protected long lastKey;

    protected void init(boolean exclusive) {
        this.exclusive = exclusive;
        this.fromNull = true;
    }

    protected void init(boolean exclusive, long fromKey) {
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
