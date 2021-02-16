package codes.writeonce.deltastore.schema.xml.reader;

import java.util.LinkedHashMap;

public class SchemaInfo {

    private final String name;
    private final String packageName;
    private final boolean instantiable;
    private final LinkedHashMap<String, EntityTypeInfo> typeMap;
    private final LinkedHashMap<String, KeyInfo> keyMap;
    private final LinkedHashMap<String, SchemaInfo> parentSchemaMap;
    private final LinkedHashMap<String, SchemaInfo> directParentSchemaMap;

    public SchemaInfo(String name, String packageName, boolean instantiable,
            LinkedHashMap<String, EntityTypeInfo> typeMap, LinkedHashMap<String, KeyInfo> keyMap,
            LinkedHashMap<String, SchemaInfo> parentSchemaMap,
            LinkedHashMap<String, SchemaInfo> directParentSchemaMap
    ) {
        this.name = name;
        this.packageName = packageName;
        this.instantiable = instantiable;
        this.typeMap = typeMap;
        this.keyMap = keyMap;
        this.parentSchemaMap = parentSchemaMap;
        this.directParentSchemaMap = directParentSchemaMap;
    }

    public String getName() {
        return name;
    }

    public String getPackageName() {
        return packageName;
    }

    public boolean isInstantiable() {
        return instantiable;
    }

    public LinkedHashMap<String, EntityTypeInfo> getTypeMap() {
        return typeMap;
    }

    public LinkedHashMap<String, KeyInfo> getKeyMap() {
        return keyMap;
    }

    public LinkedHashMap<String, SchemaInfo> getParentSchemaMap() {
        return parentSchemaMap;
    }

    public LinkedHashMap<String, SchemaInfo> getDirectParentSchemaMap() {
        return directParentSchemaMap;
    }
}
