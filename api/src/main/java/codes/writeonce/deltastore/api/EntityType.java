package codes.writeonce.deltastore.api;

import java.util.List;

public interface EntityType<E extends Entity<E>> {

    boolean isInstantiable();

    List<EntityType<?>> getSupertypes();

    String getName();

    List<Field<E, ?>> getFields();

    Field<E, ?> getField(String name);

    Key<E> getKey();

    List<Key<E>> getKeys();

    Key<E> getKey(String name);
}
