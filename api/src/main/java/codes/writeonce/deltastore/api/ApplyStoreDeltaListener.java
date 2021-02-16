package codes.writeonce.deltastore.api;

import javax.annotation.Nonnull;

public class ApplyStoreDeltaListener<S extends Store<S>> implements DeltaListener<S> {

    private final S store;

    public ApplyStoreDeltaListener(S store) {
        this.store = store;
    }

    @Override
    public void acceptDeltas(
            DeltaRecord<S, ?, ?> removeListTail,
            DeltaRecord<S, ?, ?> updateListTail,
            DeltaRecord<S, ?, ?> insertListTail
    ) {
        try (StoreTransaction<S> t = store.begin(true)) {
            DeltaRecord.iterate(removeListTail, item -> processRemove(store, item));
            DeltaRecord.iterate(updateListTail, item -> processUpdate(store, item));
            DeltaRecord.iterate(insertListTail, item -> processInsert(store, item));
            t.commit();
        }
    }

    private <D extends DeltaRecord<S, E, D>,
            E extends Entity<E>>
    void processInsert(S store, DeltaRecord<S, E, D> deltaRecord) {
        update(deltaRecord, store.getTable(deltaRecord.getEntityType()).create(deltaRecord));
        deltaRecord.release();
    }

    private <D extends DeltaRecord<S, E, D>,
            E extends Entity<E>>
    void processUpdate(S store, DeltaRecord<S, E, D> deltaRecord) {
        update(deltaRecord, store.getTable(deltaRecord.getEntityType()).get(deltaRecord));
        deltaRecord.release();
    }

    private <D extends DeltaRecord<S, E, D>,
            E extends Entity<E>>
    void processRemove(S store, DeltaRecord<S, E, D> deltaRecord) {
        store.getTable(deltaRecord.getEntityType()).get(deltaRecord).remove();
        deltaRecord.release();
    }

    private <D extends DeltaRecord<S, E, D>,
            E extends Entity<E>>
    void update(DeltaRecord<S, E, D> deltaRecord, E entity) {

        deltaRecord.accept(new FieldValueConsumerVisitor<Void, RuntimeException, E>() {
            @Override
            public <F extends Field<E, V>, V> Void visitClear(@Nonnull F field) {
                field.clearValue(entity);
                return null;
            }

            @Override
            public <F extends Field<E, V>, V> Void visitSet(@Nonnull F field, V value) {
                field.setValue(entity, value);
                return null;
            }

            @Override
            public Void visitSet(@Nonnull BooleanField<E> field, boolean value) {
                field.setValue(entity, value);
                return null;
            }

            @Override
            public Void visitSet(@Nonnull IntegerField<E> field, int value) {
                field.setValue(entity, value);
                return null;
            }

            @Override
            public Void visitSet(@Nonnull LongField<E> field, long value) {
                field.setValue(entity, value);
                return null;
            }
        });
    }
}
