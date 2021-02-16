package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

abstract class AbstractStringAscendingChainedIterator<V, T>
        extends AscendingChainedIterator<String, V, StringTreeMap<V>, T> {

    protected boolean exclusive;

    @Nullable
    private String fromKey;

    @Nullable
    protected String lastKey;

    protected void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive) {
        super.init(iterator);
        this.exclusive = exclusive;
        this.fromKey = null;
    }

    protected void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, @Nonnull String fromKey) {
        super.init(iterator);
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
