package codes.writeonce.deltastore.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.UnaryOperator;

public abstract class Store<S extends Store<S>> implements DistributedTransactional {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public final Pool<StoreTransaction<S>> transactionPool = new Pool<StoreTransaction<S>>() {
        @Nonnull
        @Override
        public StoreTransaction<S> get() {
            return new StoreTransaction<>();
        }

        @Override
        public void put(@Nonnull StoreTransaction<S> value) {
            value.clean();
        }
    };

    protected final Schema schema;

    private final CommitListener<S> commitListener;

    public StoreTransaction<S> currentTransaction;

    public Store(Schema schema, CommitListener<S> commitListener) {
        this.schema = schema;
        this.commitListener = commitListener;
    }

    public Schema getSchema() {
        return schema;
    }

    public boolean isTransactionPending() {
        return currentTransaction != null;
    }

    @Nonnull
    @Override
    public StoreTransaction<S> begin() {
        return begin(false);
    }

    public StoreTransaction<S> begin(boolean deferIndex) {
        currentTransaction = transactionPool.get().init(self(), deferIndex);
        return currentTransaction;
    }

    public void prepareCommit() {
        reindexNow();
    }

    public void finalCommit() {

        final Record<S, ?, ?> tail = currentTransaction.changeListTail;
        final StoreTransaction<S> prev = currentTransaction.transactionStackTop;

        if (prev == null) {
            try {
                final long start = System.nanoTime();
                commitListener.commit(tail);
                final long end = System.nanoTime();
                logger.trace("Final commit in {} nanos", end - start);
            } catch (Throwable e) {
                logger.error("Final commit failed, trying rollback", e);
                rollback();
                throw e;
            }
        } else if (prev.changeListTail == null) {
            prev.changeListTail = tail;
            final long start = System.nanoTime();
            doForEach(Record::patch);
            final long end = System.nanoTime();
            logger.trace("Patch commit in {} nanos", end - start);
        } else {
            final long start = System.nanoTime();
            doForEach(Record::commit);
            final long end = System.nanoTime();
            logger.trace("Regular commit in {} nanos", end - start);
        }

        transactionPool.put(currentTransaction);
        currentTransaction = prev;
    }

    public void rollback() {
        final long start = System.nanoTime();
        doForEach(Record::rollback);
        final long end = System.nanoTime();
        logger.trace("Rollback in {} nanos", end - start);
        final StoreTransaction<S> prev = currentTransaction.transactionStackTop;
        transactionPool.put(currentTransaction);
        currentTransaction = prev;
    }

    private void reindexNow() {
        if (currentTransaction.deferIndex) {
            currentTransaction.deferIndex = false;
            final long unindexStart = System.nanoTime();
            doForEach(Record::unindexCommit);
            final long unindexEnd = System.nanoTime();
            doForEach(Record::reindexCommit);
            final long reindexEnd = System.nanoTime();
            logger.trace("Deferred reindexing: unindex in {} nanos, reindex in {} nanos", unindexEnd - unindexStart,
                    reindexEnd - unindexEnd);
        }
    }

    private void doForEach(UnaryOperator<Record<S, ?, ?>> operator) {
        doForEach(currentTransaction.changeListTail, operator);
    }

    private void doForEach(Record<S, ?, ?> tail, UnaryOperator<Record<S, ?, ?>> operator) {
        if (tail != null) {
            final Record<S, ?, ?> head = tail.nextInChangeList;
            Record<S, ?, ?> item = head;
            do {
                item = operator.apply(item);
            } while (item != head);
        }
    }

    public abstract List<Table<?>> getTables();

    public abstract Table<?> getTable(String name);

    public abstract <E extends Entity<E>> Table<E> getTable(EntityType<E> entityType);

    protected abstract S self();
}
