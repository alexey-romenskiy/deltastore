package codes.writeonce.deltastore.api;

public interface Entity<E extends Entity<E>> {

    void remove();

    EntityType<E> getEntityType();
}
