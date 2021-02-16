package codes.writeonce.deltastore.api;

public class StoreTransaction<S extends Store<S>> implements DistributedTransaction {

    public boolean ended;
    public boolean deferIndex;
    public S store;
    public Record<S, ?, ?> changeListTail;
    public StoreTransaction<S> transactionStackTop;

    StoreTransaction() {
        // empty
    }

    StoreTransaction<S> init(S store, boolean deferIndex) {
        this.ended = false;
        this.deferIndex = deferIndex;
        this.store = store;
        this.changeListTail = null;
        this.transactionStackTop = store.currentTransaction;
        return this;
    }

    @Override
    public void close() {
        if (!ended) {
            rollback();
        }
    }

    @Override
    public void rollback() {

        if (ended) {
            throw new IllegalStateException();
        }

        if (store.currentTransaction != this) {
            throw new IllegalStateException();
        }

        store.rollback();
        ended = true;
    }

    @Override
    public void prepareCommit() {

        if (ended) {
            throw new IllegalStateException();
        }

        if (store.currentTransaction != this) {
            throw new IllegalStateException();
        }

        store.prepareCommit();
    }

    @Override
    public void finalCommit() {

        if (ended) {
            throw new IllegalStateException();
        }

        if (store.currentTransaction != this) {
            throw new IllegalStateException();
        }

        store.finalCommit();
        ended = true;
    }

    @Override
    public void commit() {
        prepareCommit();
        finalCommit();
    }

    public void clean() {
        ended = true;
        deferIndex = false;
        store = null;
        changeListTail = null;
        transactionStackTop = null;
    }
}
