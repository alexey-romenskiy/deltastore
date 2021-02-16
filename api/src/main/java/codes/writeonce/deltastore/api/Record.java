package codes.writeonce.deltastore.api;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.Instant;

public abstract class Record<S extends Store<S>, E extends Entity<E>, D extends DeltaRecord<S, E, D>>
        implements Entity<E>, FieldValueSupplierVisitor<RuntimeException, E> {

    public static final long MASK_EXISTS = 1;

    public S store;

    public D diffStackTop;

    public Record<S, ?, ?> nextInChangeList;

    public StoreTransaction<S> currentTransaction;

    public long mask0;

    @Override
    public void remove() {

        checkTransaction();

        if (isSetField0(MASK_EXISTS)) {
            if (!store.currentTransaction.deferIndex) {
                unindex();
            }

            if (isCleanRecord()) {
                pushRecord(store.currentTransaction);
                diffStackTop.diff0 ^= MASK_EXISTS;
            } else if (!isDirtyField0(MASK_EXISTS)) {
                diffStackTop.diff0 ^= MASK_EXISTS;
            }

            diffStackTop.mask0 ^= MASK_EXISTS;
            mask0 ^= MASK_EXISTS;
        }
    }

    public abstract Record<S, ?, ?> rollback();

    protected abstract void unindex();

    public Record<S, ?, ?> commit() {

        final StoreTransaction<S> transaction = currentTransaction.transactionStackTop;

        final Record<S, ?, ?> nextToCommit = nextInChangeList;

        if (transaction == diffStackTop.currentTransaction) {
            popRecordOnCommit();
        } else {
            addToChangeList(transaction);
        }

        return nextToCommit;
    }

    protected void popRecordOnCommit() {

        diffStackTop.diffStackTop.diff0 |= diffStackTop.diff0;
        diffStackTop.diffStackTop.mask0 ^= diffStackTop.mask0;

        popRecord();
    }

    public Record<S, ?, ?> cleanRecord() {
        final Record<S, ?, ?> nextToCommit = nextInChangeList;
        nextInChangeList = null;
        currentTransaction = null;
        diffStackTop = null;
        return nextToCommit;
    }

    public Record<S, ?, ?> patch() {
        currentTransaction = currentTransaction.transactionStackTop;
        return nextInChangeList;
    }

    protected void popRecord() {

        nextInChangeList = diffStackTop.nextInChangeList;

        currentTransaction = diffStackTop.currentTransaction;

        final D restoredStack = diffStackTop.diffStackTop;
        diffStackTop.release();
        diffStackTop = restoredStack;
    }

    protected void checkTransaction() {
        if (store.currentTransaction == null) {
            throw new IllegalStateException("Field modification attempt out of transaction");
        }
    }

    protected boolean isCleanRecord() {
        return currentTransaction != store.currentTransaction;
    }

    protected void pushRecord(StoreTransaction<S> transaction) {
        final D recordDataDiff = get();
        recordDataDiff.init(currentTransaction, diffStackTop, nextInChangeList);
        diffStackTop = recordDataDiff;
        addToChangeList(transaction);
    }

    @Nonnull
    protected abstract D get();

    protected void addToChangeList(StoreTransaction<S> transaction) {
        final Record<S, ?, ?> tail = transaction.changeListTail;
        if (tail == null) {
            nextInChangeList = this;
        } else {
            if (tail.currentTransaction == transaction) {
                nextInChangeList = tail.nextInChangeList;
                tail.nextInChangeList = this;
            } else {
                nextInChangeList = tail.diffStackTop.nextInChangeList;
                tail.diffStackTop.nextInChangeList = this;
            }
        }
        transaction.changeListTail = this;
        currentTransaction = transaction;
    }

    protected boolean isDirtyField0(long mask) {
        return (diffStackTop.diff0 & mask) != 0;
    }

    protected boolean isSetField0(long mask) {
        return (this.mask0 & mask) != 0;
    }

    protected void revertMaskBit0(long mask) {
        this.mask0 ^= diffStackTop.mask0 & mask;
    }

    public abstract Record<S, ?, ?> unindexCommit();

    public abstract Record<S, ?, ?> reindexCommit();

    public abstract void release();

    public abstract boolean normalizeChange();

    public void clean() {
        store = null;
        diffStackTop = null;
        nextInChangeList = null;
        currentTransaction = null;
        mask0 = 0;
    }

    protected abstract E asEntity();

    @Override
    public BigDecimal visit(BigDecimalField<E> field) {
        return field.getValue(asEntity());
    }

    @Override
    public Long visit(LongField<E> field) {
        return field.getValue(asEntity());
    }

    @Override
    public Instant visit(InstantField<E> field) {
        return field.getValue(asEntity());
    }

    @Override
    public Boolean visit(BooleanField<E> field) {
        return field.getValue(asEntity());
    }

    @Override
    public <N extends Enum<N>> N visit(EnumField<E, N> field) {
        return field.getValue(asEntity());
    }

    @Override
    public <I extends Id> I visit(IdField<E, I> field) {
        return field.getValue(asEntity());
    }

    @Override
    public String visit(StringField<E> field) {
        return field.getValue(asEntity());
    }

    @Override
    public Integer visit(IntegerField<E> field) {
        return field.getValue(asEntity());
    }

    @Override
    public boolean visitAsBoolean(@Nonnull BooleanField<E> field) {
        return field.getValueAsBoolean(asEntity());
    }

    @Override
    public int visitAsInt(@Nonnull IntegerField<E> field) {
        return field.getValueAsInt(asEntity());
    }

    @Override
    public long visitAsLong(@Nonnull LongField<E> field) {
        return field.getValueAsLong(asEntity());
    }
}
