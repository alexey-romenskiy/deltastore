package codes.writeonce.deltastore.api;

import java.util.List;

public abstract class AbstractEntityType<E extends Entity<E>> implements EntityType<E> {

    private final String name;

    private final boolean instantiable;

    private final List<EntityType<?>> supertypes;

    public AbstractEntityType(String name, boolean instantiable, List<EntityType<?>> supertypes) {
        this.name = name;
        this.instantiable = instantiable;
        this.supertypes = supertypes;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isInstantiable() {
        return instantiable;
    }

    @Override
    public List<EntityType<?>> getSupertypes() {
        return supertypes;
    }
}
