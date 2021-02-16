package codes.writeonce.deltastore.schema.xml.reader;

import java.util.LinkedHashSet;

public class KeyInfo {

    private final String schemaName;
    private final String name;
    private final boolean unique;
    private final LinkedHashSet<String> fields;
    private final EntityTypeInfo entityType;

    public KeyInfo(String schemaName, String name, boolean unique, LinkedHashSet<String> fields,
            EntityTypeInfo entityTypeInfo) {
        this.schemaName = schemaName;
        this.name = name;
        this.unique = unique;
        this.fields = fields;
        this.entityType = entityTypeInfo;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getName() {
        return name;
    }

    public boolean isUnique() {
        return unique;
    }

    public LinkedHashSet<String> getFields() {
        return fields;
    }

    public EntityTypeInfo getEntityType() {
        return entityType;
    }
}
