package codes.writeonce.deltastore.api.map;

abstract class AbstractEnumAscendingSimpleIterator<V> extends AscendingSimpleIterator<Enum<?>, V, EnumTreeMap<V>> {

    protected boolean exclusive;

    private int fromKey;

    private boolean fromNull;

    protected int lastKey;

    protected void init(boolean exclusive) {
        this.exclusive = exclusive;
        this.fromNull = true;
    }

    protected void init(boolean exclusive, int fromKey) {
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

    protected abstract boolean ended(int key);
}
