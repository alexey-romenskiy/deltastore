package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

abstract class AbstractStringAscendingSimpleIterator<V> extends AscendingSimpleIterator<String, V, StringTreeMap<V>> {

    protected boolean exclusive;

    @Nullable
    private String fromKey;

    @Nullable
    protected String lastKey;

    protected void init(boolean exclusive) {
        this.exclusive = exclusive;
        this.fromKey = null;
    }

    protected void init(boolean exclusive, @Nonnull String fromKey) {
        this.exclusive = exclusive;
        this.fromKey = requireNonNull(fromKey);
    }

    @Override
    protected void resetKey() {

        if (fromKey == null) {
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
        assert lastKey != null;
        return map.getCeilingEntry(lastKey);
    }

    @Override
    protected int higher() {
        assert lastKey != null;
        return map.getHigherEntry(lastKey);
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
