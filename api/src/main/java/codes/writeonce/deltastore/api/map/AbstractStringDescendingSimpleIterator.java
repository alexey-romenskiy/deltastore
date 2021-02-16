package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

abstract class AbstractStringDescendingSimpleIterator<V> extends DescendingSimpleIterator<String, V, StringTreeMap<V>> {

    protected boolean init;

    protected boolean exclusive;

    @Nullable
    private String fromKey;

    @Nullable
    protected String lastKey;

    protected void init() {
        this.init = true;
    }

    protected void init(boolean exclusive, @Nonnull String fromKey) {
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
