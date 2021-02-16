package codes.writeonce.deltastore.api;

public abstract class AbstractTable<S extends Store<S>, E extends Entity<E>> implements Table<E> {

    protected final S store;
    protected final EntityType<E> entityType;
    protected final String name;

    public AbstractTable(S store, EntityType<E> entityType) {
        this.store = store;
        this.entityType = entityType;
        this.name = entityType.getName();
    }

    @Override
    public EntityType<E> getType() {
        return entityType;
    }

    @Override
    public String getName() {
        return name;
    }
}
