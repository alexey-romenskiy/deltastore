package codes.writeonce.deltastore.api;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public abstract class DeltaRecord<S extends Store<S>, E extends Entity<E>, D extends DeltaRecord<S, E, D>>
        implements FieldValueSupplierVisitor<RuntimeException, E> {

    public D diffStackTop;

    public DeltaRecord<S, ?, ?> next;

    public Record<S, ?, ?> nextInChangeList;

    public StoreTransaction<S> currentTransaction;

    /**
     * Changed if the respective bit set
     */
    public long diff0;

    /**
     * Invert if the respective bit set
     */
    public long mask0;

    public void init(StoreTransaction<S> currentTransaction, D diffStackTop, Record<S, ?, ?> nextInChangeList) {
        this.currentTransaction = currentTransaction;
        this.diffStackTop = diffStackTop;
        this.nextInChangeList = nextInChangeList;
    }

    public List<Field<E, ?>> getChangedFields() {
        final List<Field<E, ?>> fields = new ArrayList<>();
        accept(new FieldValueConsumerVisitor<Void, RuntimeException, E>() {
            @Override
            public <F extends Field<E, V>, V> Void visitClear(@Nonnull F field) {
                fields.add(field);
                return null;
            }

            @Override
            public <F extends Field<E, V>, V> Void visitSet(@Nonnull F field, V value) {
                fields.add(field);
                return null;
            }

            @Override
            public Void visitSet(@Nonnull BooleanField<E> field, boolean value) {
                fields.add(field);
                return null;
            }

            @Override
            public Void visitSet(@Nonnull IntegerField<E> field, int value) {
                fields.add(field);
                return null;
            }

            @Override
            public Void visitSet(@Nonnull LongField<E> field, long value) {
                fields.add(field);
                return null;
            }
        });
        return fields;
    }

    public List<Field<E, ?>> getAllChangedFields() {
        final List<Field<E, ?>> fields = new ArrayList<>();
        acceptAll(new FieldValueConsumerVisitor<Void, RuntimeException, E>() {
            @Override
            public <F extends Field<E, V>, V> Void visitClear(@Nonnull F field) {
                fields.add(field);
                return null;
            }

            @Override
            public <F extends Field<E, V>, V> Void visitSet(@Nonnull F field, V value) {
                fields.add(field);
                return null;
            }

            @Override
            public Void visitSet(@Nonnull BooleanField<E> field, boolean value) {
                fields.add(field);
                return null;
            }

            @Override
            public Void visitSet(@Nonnull IntegerField<E> field, int value) {
                fields.add(field);
                return null;
            }

            @Override
            public Void visitSet(@Nonnull LongField<E> field, long value) {
                fields.add(field);
                return null;
            }
        });
        return fields;
    }

    public abstract void release();

    public abstract EntityType<E> getEntityType();

    public abstract E asEntity();

    public abstract D asDeltaRecord();

    public abstract <U, X extends Throwable> U accept(DeltaOperationVisitor<U, X> visitor) throws X;

    public abstract <X extends Throwable> void accept(FieldValueConsumerVisitor<Void, X, E> visitor) throws X;

    public abstract <X extends Throwable> void acceptAll(FieldValueConsumerVisitor<Void, X, E> visitor) throws X;

    protected void clean() {
        diffStackTop = null;
        next = null;
        nextInChangeList = null;
        currentTransaction = null;
        diff0 = 0;
        mask0 = 0;
    }

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

    public static <S extends Store<S>, E extends Throwable> void iterate(
            DeltaRecord<S, ?, ?> tail,
            IteratorConsumer<S, E> consumer
    ) throws E {
        if (tail != null) {
            DeltaRecord<S, ?, ?> item = tail.next;
            while (true) {
                if (item == tail) {
                    consumer.accept(item);
                    break;
                } else {
                    final DeltaRecord<S, ?, ?> next = item.next;
                    consumer.accept(item);
                    item = next;
                }
            }
        }
    }

    public abstract void merge(D sourceDeltaRecord);

    public abstract void applyDefaults();

    public interface IteratorConsumer<S extends Store<S>, E extends Throwable> {

        void accept(DeltaRecord<S, ?, ?> record) throws E;
    }
}
