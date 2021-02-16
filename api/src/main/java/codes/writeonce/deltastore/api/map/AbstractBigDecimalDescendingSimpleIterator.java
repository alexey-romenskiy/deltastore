package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;

abstract class AbstractBigDecimalDescendingSimpleIterator<V>
        extends DescendingSimpleIterator<BigDecimal, V, BigDecimalTreeMap<V>> {

    protected boolean init;

    protected boolean exclusive;

    @Nullable
    private BigDecimal fromKey;

    @Nullable
    protected BigDecimal lastKey;

    protected void init() {
        this.init = true;
    }

    protected void init(boolean exclusive, @Nonnull BigDecimal fromKey) {
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
        lastKey = (BigDecimal) map.keys[index];
    }

    @Override
    protected boolean sameKey() {
        assert lastKey != null;
        return lastKey.compareTo((BigDecimal) map.keys[index]) == 0;
    }

    protected abstract boolean ended(@Nonnull BigDecimal key);

    @Override
    public void close() {
        super.close();
        this.fromKey = null;
        this.lastKey = null;
    }
}
