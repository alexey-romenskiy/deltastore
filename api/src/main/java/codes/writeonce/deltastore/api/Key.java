package codes.writeonce.deltastore.api;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

public class Key<E extends Entity<E>> {

    private final EntityType<E> entityType;

    private final String name;

    private final List<Field<E, ?>> fields;

    private final boolean unique;

    private final Map<String, Field<E, ?>> fieldMap;

    public Key(EntityType<E> entityType, String name, List<Field<E, ?>> fields, boolean unique) {
        this.entityType = entityType;
        this.name = name;
        this.fields = fields;
        this.unique = unique;
        this.fieldMap = fields.stream().collect(Collectors.toMap(Field::getName, identity()));
    }

    public String getName() {
        return name;
    }

    public EntityType<E> getEntityType() {
        return entityType;
    }

    public List<Field<E, ?>> getFields() {
        return fields;
    }

    public Field<E, ?> getField(String name) {
        return fieldMap.get(name);
    }

    public boolean isUnique() {
        return unique;
    }
}
