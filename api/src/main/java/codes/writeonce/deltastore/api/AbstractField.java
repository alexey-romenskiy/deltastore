package codes.writeonce.deltastore.api;

public abstract class AbstractField<E extends Entity<E>, V> implements Field<E, V> {

    private final EntityType<E> entityType;

    private final String name;

    private final boolean nullable;

    private final boolean mutable;

    public AbstractField(EntityType<E> entityType, String name, boolean nullable, boolean mutable) {
        this.entityType = entityType;
        this.name = name;
        this.nullable = nullable;
        this.mutable = mutable;
    }

    @Override
    public EntityType<E> getEntityType() {
        return entityType;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public boolean isMutable() {
        return mutable;
    }
}
