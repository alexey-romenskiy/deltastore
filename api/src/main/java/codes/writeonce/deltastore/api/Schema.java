package codes.writeonce.deltastore.api;

import java.util.List;

public interface Schema {

    List<EntityType<?>> getEntityTypes();

    EntityType<?> getEntityType(String name);
}
