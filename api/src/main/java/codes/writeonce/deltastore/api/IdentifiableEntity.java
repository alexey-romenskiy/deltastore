package codes.writeonce.deltastore.api;

public interface IdentifiableEntity<E extends IdentifiableEntity<E>> extends Entity<E>, Identifiable<Id<E>> {

}
