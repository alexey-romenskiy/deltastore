package codes.writeonce.deltastore.schema.xml.reader;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

public class EntityTypeInfo {

    private final String schemaName;
    private final LinkedHashSet<String> parents;
    private final String name;
    private final boolean instantiable;
    private final String key;
    private final LinkedHashMap<String, FieldInfo> fieldMap;
    private final LinkedHashMap<String, KeyInfo> keyMap;

    public EntityTypeInfo(String schemaName, LinkedHashSet<String> parents, String name, boolean instantiable,
            String key, LinkedHashMap<String, FieldInfo> fieldMap, LinkedHashMap<String, KeyInfo> keyMap) {
        this.schemaName = schemaName;
        this.parents = parents;
        this.name = name;
        this.instantiable = instantiable;
        this.key = key;
        this.fieldMap = fieldMap;
        this.keyMap = keyMap;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public LinkedHashSet<String> getParents() {
        return parents;
    }

    public String getName() {
        return name;
    }

    public boolean isInstantiable() {
        return instantiable;
    }

    public String getKey() {
        return key;
    }

    public LinkedHashMap<String, FieldInfo> getFieldMap() {
        return fieldMap;
    }

    public LinkedHashMap<String, KeyInfo> getKeyMap() {
        return keyMap;
    }
}
