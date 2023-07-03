package codes.writeonce.deltastore.schema.generator;

import codes.writeonce.deltastore.schema.xml.reader.BigDecimalFieldInfo;
import codes.writeonce.deltastore.schema.xml.reader.BooleanFieldInfo;
import codes.writeonce.deltastore.schema.xml.reader.EntityTypeInfo;
import codes.writeonce.deltastore.schema.xml.reader.EnumFieldInfo;
import codes.writeonce.deltastore.schema.xml.reader.FieldInfo;
import codes.writeonce.deltastore.schema.xml.reader.IdFieldInfo;
import codes.writeonce.deltastore.schema.xml.reader.InstantFieldInfo;
import codes.writeonce.deltastore.schema.xml.reader.IntegerFieldInfo;
import codes.writeonce.deltastore.schema.xml.reader.KeyInfo;
import codes.writeonce.deltastore.schema.xml.reader.LongFieldInfo;
import codes.writeonce.deltastore.schema.xml.reader.SchemaInfo;
import codes.writeonce.deltastore.schema.xml.reader.StringFieldInfo;

import javax.annotation.Nonnull;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class SchemaGenerator {

    public void generate(SchemaInfo schemaInfo, Path targetDirectory, Charset charset)
            throws IOException {

        if (schemaInfo.isInstantiable()) {
            generateSchemaClass(schemaInfo, targetDirectory, charset);
            generateStoreClass(schemaInfo, targetDirectory, charset);
            generateTableClasses(schemaInfo, targetDirectory, charset);
            generateEntityTypes(schemaInfo, targetDirectory, charset);
            generateRecordClasses(schemaInfo, targetDirectory, charset);
            generateDeltaRecordClasses(schemaInfo, targetDirectory, charset);
        } else {
            generateAbstractStoreClass(schemaInfo, targetDirectory, charset);
        }

        generateTableInterfaces(schemaInfo, targetDirectory, charset);
        generateKeyClasses(schemaInfo, targetDirectory, charset);
        generateTypeInterfaces(schemaInfo, targetDirectory, charset);
    }

    private void generateSchemaClass(SchemaInfo schemaInfo, Path path, Charset charset)
            throws IOException {

        final String schemaName = getSchemaName(schemaInfo);

        try (FileOutputStream out = new FileOutputStream(path.resolve(schemaName + ".java").toFile());
             OutputStreamWriter writer = new OutputStreamWriter(out, charset)) {

            writer.append("package ").append(schemaInfo.getPackageName()).append(";\n");
            writer.append("\n");
            writer.append("import codes.writeonce.deltastore.api.EntityType;\n");
            writer.append("import codes.writeonce.deltastore.api.Schema;\n");
            writer.append("\n");
            writer.append("import java.util.Arrays;\n");
            writer.append("import java.util.Collections;\n");
            writer.append("import java.util.List;\n");
            writer.append("import java.util.Map;\n");
            writer.append("import java.util.stream.Collectors;\n");
            writer.append("\n");
            writer.append("import static java.util.function.Function.identity;\n");

            if (!schemaInfo.getParentSchemaMap().isEmpty()) {

                writer.append("\n");

                for (final SchemaInfo parentSchemaInfo : schemaInfo.getParentSchemaMap().values()) {
                    for (final EntityTypeInfo entityTypeInfo : parentSchemaInfo.getTypeMap().values()) {
                        if (entityTypeInfo.getSchemaName().equals(parentSchemaInfo.getName())) {
                            writer.append("import ").append(parentSchemaInfo.getPackageName()).append('.')
                                    .append(entityTypeInfo.getName()).append(";\n");
                        }
                    }
                }
            }

            writer.append("\n");
            writer.append("public class ").append(schemaName).append(" implements Schema {\n");
            writer.append("\n");

            indent(writer, 1).append("public static final Schema INSTANCE = new ").append(schemaName).append("();\n");

            for (final EntityTypeInfo entityTypeInfo : schemaInfo.getTypeMap().values()) {
                if (entityTypeInfo.isInstantiable()) {
                    writer.append("\n");
                    indent(writer, 1).append("public static final EntityType<").append(entityTypeInfo.getName())
                            .append("> ENTITY_TYPE_").append(toUpperCase(entityTypeInfo.getName())).append(" = ")
                            .append(entityTypeInfo.getName()).append("EntityType.INSTANCE;\n");
                }
            }

            writer.append("\n");
            indent(writer, 1)
                    .append("private static final List<EntityType<?>> ENTITY_TYPES = Collections.unmodifiableList(Arrays.asList(");

            boolean first = true;

            for (final EntityTypeInfo entityTypeInfo : schemaInfo.getTypeMap().values()) {
                if (entityTypeInfo.isInstantiable()) {
                    if (first) {
                        first = false;
                    } else {
                        writer.append(",");
                    }
                    indent(writer.append("\n"), 3).append("ENTITY_TYPE_").append(toUpperCase(entityTypeInfo.getName()));
                }
            }

            if (!first) {
                indent(writer.append("\n"), 1);
            }

            writer.append("));\n");

            writer.append("\n");
            indent(writer, 1).append("private static final Map<String, EntityType<?>> ENTITY_TYPE_MAP =\n");
            indent(writer, 3)
                    .append("ENTITY_TYPES.stream().collect(Collectors.toMap(EntityType::getName, identity()));\n");

            writer.append("\n");
            indent(writer, 1).append("private ").append(schemaName).append("() {\n");
            indent(writer, 2).append("// empty\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            indent(writer, 1).append("public List<EntityType<?>> getEntityTypes() {\n");
            indent(writer, 2).append("return ENTITY_TYPES;\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            indent(writer, 1).append("public EntityType<?> getEntityType(String name) {\n");
            indent(writer, 2).append("return ENTITY_TYPE_MAP.get(name);\n");
            indent(writer, 1).append("}\n");

            writer.append("}\n");
        }
    }

    private void generateStoreClass(SchemaInfo schemaInfo, Path path, Charset charset) throws IOException {

        try (FileOutputStream out = new FileOutputStream(path.resolve(schemaInfo.getName() + "Store.java").toFile());
             OutputStreamWriter writer = new OutputStreamWriter(out, charset)) {

            final String storeName = getStoreName(schemaInfo);
            final String schemaName = getSchemaName(schemaInfo);

            writer.append("package ").append(schemaInfo.getPackageName()).append(";\n");
            writer.append("\n");
            writer.append("import codes.writeonce.deltastore.api.CommitListener;\n");
            writer.append("import codes.writeonce.deltastore.api.Entity;\n");
            writer.append("import codes.writeonce.deltastore.api.EntityType;\n");
            writer.append("import codes.writeonce.deltastore.api.Store;\n");
            writer.append("import codes.writeonce.deltastore.api.Table;\n");
            writer.append("\n");
            writer.append("import java.util.Arrays;\n");
            writer.append("import java.util.List;\n");
            writer.append("import java.util.Map;\n");
            writer.append("import java.util.stream.Collectors;\n");
            writer.append("\n");
            writer.append("import static java.util.function.Function.identity;\n");

            if (!schemaInfo.getParentSchemaMap().isEmpty()) {

                writer.append("\n");

                for (final SchemaInfo parentSchemaInfo : schemaInfo.getParentSchemaMap().values()) {
                    for (final KeyInfo keyInfo : parentSchemaInfo.getKeyMap().values()) {
                        if (keyInfo.getSchemaName().equals(parentSchemaInfo.getName())) {
                            writer.append("import ").append(parentSchemaInfo.getPackageName()).append('.')
                                    .append(keyInfo.getName()).append(";\n");
                        }
                    }
                    for (final EntityTypeInfo entityTypeInfo : parentSchemaInfo.getTypeMap().values()) {
                        if (entityTypeInfo.getSchemaName().equals(parentSchemaInfo.getName()) &&
                            entityTypeInfo.isInstantiable()) {
                            writer.append("import ").append(parentSchemaInfo.getPackageName()).append('.')
                                    .append(entityTypeInfo.getName()).append("Table;\n");
                        }
                    }
                }
            }

            writer.append("\n");

            writer.append("public class ").append(storeName).append(" extends Store<").append(storeName).append(">");

            appendImplementsAbstractStore(writer, schemaInfo, true);

            writer.append(" {\n");

            for (final KeyInfo keyInfo : schemaInfo.getKeyMap().values()) {
                writer.append("\n");
                writer.append("    private final ").append(keyInfo.getName()).append(" ")
                        .append(withSmallLetter(keyInfo.getName())).append(" = new ").append(keyInfo.getName())
                        .append("();\n");
            }

            for (final EntityTypeInfo entityTypeInfo : schemaInfo.getTypeMap().values()) {
                if (entityTypeInfo.isInstantiable()) {
                    final String tableName = entityTypeInfo.getName() + "Table";
                    writer.append("\n");
                    writer.append("    private final ").append(tableName).append(" ")
                            .append(withSmallLetter(entityTypeInfo.getName())).append(" = new ").append(tableName)
                            .append("Impl(this);\n");
                }
            }

            writer.append("\n");
            indent(writer, 1).append("private final List<Table<?>> tables = Arrays.asList(");

            boolean first = true;
            for (final EntityTypeInfo entityTypeInfo : schemaInfo.getTypeMap().values()) {
                if (entityTypeInfo.isInstantiable()) {
                    if (first) {
                        first = false;
                    } else {
                        writer.append(", ");
                    }
                    writer.append(withSmallLetter(entityTypeInfo.getName()));
                }
            }

            writer.append(");\n");

            writer.append("\n");
            indent(writer, 1)
                    .append("private final Map<String, Table<?>> tableMap = tables.stream().collect(Collectors.toMap(Table::getName, identity()));\n");

            writer.append("\n");
            indent(writer, 1).append("public ").append(storeName).append("(CommitListener<").append(storeName)
                    .append("> commitListener) {\n");
            indent(writer, 2).append("super(").append(schemaName).append(".INSTANCE, commitListener);\n");
            indent(writer, 1).append("}\n");

            for (final KeyInfo keyInfo : schemaInfo.getKeyMap().values()) {
                writer.append("\n");
                writer.append("    public ").append(keyInfo.getName()).append(" ")
                        .append(withSmallLetter(keyInfo.getName())).append("() {\n");
                writer.append("        return ").append(withSmallLetter(keyInfo.getName())).append(";\n");
                writer.append("    }\n");
            }

            for (final EntityTypeInfo entityTypeInfo : schemaInfo.getTypeMap().values()) {
                if (entityTypeInfo.isInstantiable()) {
                    final String tableName = entityTypeInfo.getName() + "Table";
                    writer.append("\n");
                    writer.append("    public ").append(tableName).append(" ")
                            .append(withSmallLetter(entityTypeInfo.getName())).append("() {\n");
                    writer.append("        return ").append(withSmallLetter(entityTypeInfo.getName())).append(";\n");
                    writer.append("    }\n");
                }
            }

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            indent(writer, 1).append("public List<Table<?>> getTables() {\n");
            indent(writer, 2).append("return tables;\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            indent(writer, 1).append("public Table<?> getTable(String name) {\n");
            indent(writer, 2).append("return tableMap.get(name);\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            indent(writer, 1).append("@SuppressWarnings(\"unchecked\")\n");
            indent(writer, 1).append("public <E extends Entity<E>> Table<E> getTable(EntityType<E> entityType) {\n");
            indent(writer, 2).append("if (schema.getEntityType(entityType.getName()) != entityType) {\n");
            indent(writer, 3).append("throw new IllegalArgumentException();\n");
            indent(writer, 2).append("}\n");
            indent(writer, 2).append("return (Table<E>) tableMap.get(entityType.getName());\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            indent(writer, 1).append("protected ").append(storeName).append(" self() {\n");
            indent(writer, 2).append("return this;\n");
            indent(writer, 1).append("}\n");

            writer.append("}\n");
        }
    }

    private void generateAbstractStoreClass(SchemaInfo schemaInfo, Path path, Charset charset) throws IOException {

        try (FileOutputStream out = new FileOutputStream(path.resolve(schemaInfo.getName() + "Store.java").toFile());
             OutputStreamWriter writer = new OutputStreamWriter(out, charset)) {

            final String storeName = getStoreName(schemaInfo);

            writer.append("package ").append(schemaInfo.getPackageName()).append(";\n");

            if (!schemaInfo.getParentSchemaMap().isEmpty()) {

                writer.append("\n");

                for (final SchemaInfo parentSchemaInfo : schemaInfo.getParentSchemaMap().values()) {
                    for (final KeyInfo keyInfo : parentSchemaInfo.getKeyMap().values()) {
                        if (keyInfo.getSchemaName().equals(parentSchemaInfo.getName())) {
                            writer.append("import ").append(parentSchemaInfo.getPackageName()).append('.')
                                    .append(keyInfo.getName()).append(";\n");
                        }
                    }
                    for (final EntityTypeInfo entityTypeInfo : parentSchemaInfo.getTypeMap().values()) {
                        if (entityTypeInfo.getSchemaName().equals(parentSchemaInfo.getName()) &&
                            entityTypeInfo.isInstantiable()) {
                            writer.append("import ").append(parentSchemaInfo.getPackageName()).append('.')
                                    .append(entityTypeInfo.getName()).append("Table;\n");
                        }
                    }
                }
            }

            writer.append("\n");
            writer.append("public interface ").append(storeName);

            appendImplementsAbstractStore(writer, schemaInfo, false);

            writer.append(" {\n");

            for (final KeyInfo keyInfo : schemaInfo.getKeyMap().values()) {
                writer.append("\n");
                indent(writer, 1).append(keyInfo.getName()).append(" ").append(withSmallLetter(keyInfo.getName()))
                        .append("();\n");
            }

            for (final EntityTypeInfo entityTypeInfo : schemaInfo.getTypeMap().values()) {
                if (entityTypeInfo.isInstantiable()) {
                    final String tableName = entityTypeInfo.getName() + "Table";
                    writer.append("\n");
                    indent(writer, 1).append(tableName).append(" ").append(withSmallLetter(entityTypeInfo.getName()))
                            .append("();\n");
                }
            }

            writer.append("}\n");
        }
    }

    private void appendImplementsAbstractStore(OutputStreamWriter writer, SchemaInfo schemaInfo, boolean instantiable)
            throws IOException {
        if (!schemaInfo.getParentSchemaMap().isEmpty()) {
            writer.append(instantiable ? " implements " : " extends ");
            boolean first = true;
            for (final SchemaInfo parentSchemaInfo : schemaInfo.getDirectParentSchemaMap().values()) {
                if (first) {
                    first = false;
                } else {
                    writer.append(", ");
                }
                writer.append(parentSchemaInfo.getPackageName()).append('.').append(getStoreName(parentSchemaInfo));
            }
        }
    }

    @Nonnull
    private String getStoreName(SchemaInfo schemaInfo) {
        return schemaInfo.getName() + "Store";
    }

    @Nonnull
    private String getSchemaName(SchemaInfo schemaInfo) {
        return schemaInfo.getName() + "Schema";
    }

    private void generateRecordClasses(SchemaInfo schemaInfo, Path path,
            Charset charset) throws IOException {

        for (final EntityTypeInfo entityTypeInfo : schemaInfo.getTypeMap().values()) {
            if (entityTypeInfo.isInstantiable()) {
                try (FileOutputStream out = new FileOutputStream(
                        path.resolve(entityTypeInfo.getName() + "Record.java").toFile());
                     OutputStreamWriter writer = new OutputStreamWriter(out, charset)) {

                    generateRecordClass(schemaInfo, schemaInfo.getPackageName(), writer, entityTypeInfo);
                }
            }
        }
    }

    private void generateDeltaRecordClasses(SchemaInfo schemaInfo, Path path,
            Charset charset) throws IOException {

        for (final EntityTypeInfo entityTypeInfo : schemaInfo.getTypeMap().values()) {
            if (entityTypeInfo.isInstantiable()) {
                try (FileOutputStream out = new FileOutputStream(
                        path.resolve(entityTypeInfo.getName() + "DeltaRecord.java").toFile());
                     OutputStreamWriter writer = new OutputStreamWriter(out, charset)) {

                    generateDeltaRecordClass(schemaInfo, schemaInfo.getPackageName(), writer, entityTypeInfo);
                }
            }
        }
    }

    private void generateDeltaRecordClass(SchemaInfo schemaInfo, String packageName, Appendable writer,
            EntityTypeInfo entityTypeInfo) throws IOException {

        writer.append("package ").append(packageName).append(";\n");
        writer.append("\n");
        writer.append("import codes.writeonce.deltastore.api.DeltaOperationVisitor;\n");
        writer.append("import codes.writeonce.deltastore.api.DeltaRecord;\n");
        writer.append("import codes.writeonce.deltastore.api.EntityType;\n");
        writer.append("import codes.writeonce.deltastore.api.FieldValueConsumerVisitor;\n");
        writer.append("import codes.writeonce.deltastore.api.Id;\n");
        writer.append("import codes.writeonce.deltastore.api.Pool;\n");
        writer.append("\n");
        writer.append("import javax.annotation.Nonnull;\n");
        writer.append("\n");
        writer.append("import static codes.writeonce.deltastore.api.Record.MASK_EXISTS;\n");

        final List<FieldInfo> fields = getFields(schemaInfo, entityTypeInfo);

        final Map<String, Integer> bitIndex = getBitIndex(fields);

        for (final FieldInfo fieldInfo : fields) {
            writer.append("import static ").append(packageName).append(".").append(entityTypeInfo.getName())
                    .append("Record.MASK_").append(toUpperCase(fieldInfo.getName())).append(";\n");
            writer.append("import static ").append(packageName).append(".").append(entityTypeInfo.getName())
                    .append("Record.DEFAULT_").append(toUpperCase(fieldInfo.getName())).append(";\n");
        }

        writer.append("import static java.util.Objects.requireNonNull;\n");

        appendImport(writer, schemaInfo, entityTypeInfo);

        for (final SchemaInfo parentSchemaInfo : schemaInfo.getParentSchemaMap().values()) {
            for (final EntityTypeInfo eti : parentSchemaInfo.getTypeMap().values()) {
                if (eti.getSchemaName().equals(parentSchemaInfo.getName())) {
                    writer.append("import ").append(parentSchemaInfo.getPackageName()).append('.')
                            .append(eti.getName()).append(";\n");
                }
            }
        }

        writer.append("\n");
        writer.append("class ").append(entityTypeInfo.getName()).append("DeltaRecord extends DeltaRecord<")
                .append(schemaInfo.getName()).append("Store, ").append(entityTypeInfo.getName()).append(", ")
                .append(entityTypeInfo.getName()).append("DeltaRecord> implements ").append(entityTypeInfo.getName())
                .append(" {\n");
        writer.append("\n");
        indent(writer, 1).append("public static final Pool<").append(entityTypeInfo.getName())
                .append("DeltaRecord> POOL = new Pool<").append(entityTypeInfo.getName()).append("DeltaRecord>() {\n");
        indent(writer, 2).append("@Nonnull\n");
        indent(writer, 2).append("@Override\n");
        indent(writer, 2).append("public ").append(entityTypeInfo.getName()).append("DeltaRecord get() {\n");
        indent(writer, 3).append("return new ").append(entityTypeInfo.getName()).append("DeltaRecord();\n");
        indent(writer, 2).append("}\n");
        writer.append("\n");
        indent(writer, 2).append("@Override\n");
        indent(writer, 2).append("public void put(@Nonnull ").append(entityTypeInfo.getName())
                .append("DeltaRecord value) {\n");
        indent(writer, 3).append("value.clean();\n");
        indent(writer, 2).append("}\n");
        indent(writer, 1).append("};\n");

        for (int i = 1; i < getMaskFieldCount(bitIndex); i++) {
            writer.append("\n");
            indent(writer, 1).append("public long diff").append(String.valueOf(i)).append(";\n");
            writer.append("\n");
            indent(writer, 1).append("public long mask").append(String.valueOf(i)).append(";\n");
        }

        for (final FieldInfo fieldInfo : fields) {
            writer.append("\n");
            indent(writer, 1).append(typeName(schemaInfo, fieldInfo, entityTypeInfo.getName(), false)).append(" ")
                    .append(fieldInfo.getName()).append(";\n");
        }

        writer.append("\n");
        indent(writer, 1).append("private ").append(entityTypeInfo.getName()).append("DeltaRecord() {\n");
        indent(writer, 2).append("// empty\n");
        indent(writer, 1).append("}\n");

        final KeyInfo key = getKey(schemaInfo, entityTypeInfo);
        final LinkedHashSet<String> keyFields = key.getFields();

        for (final FieldInfo fieldInfo : fields) {

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            indent(writer, 1).append("public ").append(typeName(schemaInfo, fieldInfo, entityTypeInfo.getName(), false))
                    .append(" ").append(getterName(fieldInfo)).append("() {\n");
            appendCheck(writer, keyFields, fieldInfo, bitIndex);
            indent(writer, 2).append("return ").append(fieldInfo.getName()).append(";\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            indent(writer, 1).append("public boolean ").append(changeTesterName(fieldInfo)).append("() {\n");
            indent(writer, 2).append("return (diff").append(getMaskField(bitIndex, fieldInfo)).append(" & MASK_")
                    .append(toUpperCase(fieldInfo.getName()))
                    .append(") != 0;\n");
            indent(writer, 1).append("}\n");

            if (fieldInfo.isMutable()) {

                writer.append("\n");
                indent(writer, 1).append("@Override\n");
                indent(writer, 1).append("public boolean ").append(testerName(fieldInfo)).append("() {\n");
                appendCheck(writer, keyFields, fieldInfo, bitIndex);
                indent(writer, 2).append("return (mask").append(getMaskField(bitIndex, fieldInfo)).append(" & MASK_")
                        .append(toUpperCase(fieldInfo.getName())).append(") != 0;\n");
                indent(writer, 1).append("}\n");

                writer.append("\n");
                indent(writer, 1).append("@Override\n");
                indent(writer, 1).append("public void ").append(clearerName(fieldInfo)).append("() {\n");
                indent(writer, 2).append("throw new UnsupportedOperationException();\n");
                indent(writer, 1).append("}\n");

                writer.append("\n");
                indent(writer, 1).append("@Override\n");
                indent(writer, 1).append("public void ").append(setterName(fieldInfo)).append("(")
                        .append(typeName(schemaInfo, fieldInfo, entityTypeInfo.getName(), false)).append(" value) {\n");
                indent(writer, 2).append("throw new UnsupportedOperationException();\n");
                indent(writer, 1).append("}\n");
            }
        }

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public void remove() {\n");
        indent(writer, 2).append("throw new UnsupportedOperationException();\n");
        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public void clean() {\n");
        indent(writer, 2).append("super.clean();\n");

        for (int i = 1; i < getMaskFieldCount(bitIndex); i++) {
            indent(writer, 2).append("diff").append(String.valueOf(i)).append(" = 0;\n");
            indent(writer, 2).append("mask").append(String.valueOf(i)).append(" = 0;\n");
        }

        for (final FieldInfo fieldInfo : fields) {
            indent(writer, 2).append(fieldInfo.getName()).append(" = ").append(getDefault(fieldInfo)).append(";\n");
        }

        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public void release() {\n");
        indent(writer, 2).append("POOL.put(this);\n");
        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public EntityType<").append(entityTypeInfo.getName()).append("> getEntityType() {\n");
        indent(writer, 2).append("return ").append(entityTypeInfo.getName()).append("EntityType.INSTANCE;\n");
        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public ").append(entityTypeInfo.getName()).append(" asEntity() {\n");
        indent(writer, 2).append("return this;\n");
        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public ").append(entityTypeInfo.getName()).append("DeltaRecord asDeltaRecord() {\n");
        indent(writer, 2).append("return this;\n");
        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1)
                .append("public <U, X extends Throwable> U accept(DeltaOperationVisitor<U, X> visitor) throws X {\n");
        indent(writer, 2).append("if ((diff0 & MASK_EXISTS) != 0) {\n");
        indent(writer, 3).append("if ((mask0 & MASK_EXISTS) != 0) {\n");
        indent(writer, 4).append("return visitor.visitInsert();\n");
        indent(writer, 3).append("} else {\n");
        indent(writer, 4).append("return visitor.visitDelete();\n");
        indent(writer, 3).append("}\n");
        indent(writer, 2).append("} else {\n");
        indent(writer, 3).append("return visitor.visitUpdate();\n");
        indent(writer, 2).append("}\n");
        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public <X extends Throwable> void accept(FieldValueConsumerVisitor<Void, X, ")
                .append(entityTypeInfo.getName()).append("> visitor) throws X {\n");

        for (final FieldInfo fieldInfo : fields) {
            if (fieldInfo.isMutable()) {
                writer.append("\n");
                indent(writer, 2).append("if ((diff").append(getMaskField(bitIndex, fieldInfo)).append(" & MASK_")
                        .append(toUpperCase(fieldInfo.getName())).append(") != 0) {\n");
                indent(writer, 3).append("if ((mask").append(getMaskField(bitIndex, fieldInfo)).append(" & MASK_")
                        .append(toUpperCase(fieldInfo.getName())).append(") != 0) {\n");
                indent(writer, 4).append("visitor.visitSet(").append(entityTypeInfo.getName())
                        .append("EntityType.FIELD_").append(toUpperCase(fieldInfo.getName())).append(", ")
                        .append(fieldInfo.getName()).append(");\n");
                indent(writer, 3).append("} else {\n");
                indent(writer, 4).append("visitor.visitClear(").append(entityTypeInfo.getName())
                        .append("EntityType.FIELD_").append(toUpperCase(fieldInfo.getName())).append(");\n");
                indent(writer, 3).append("}\n");
                indent(writer, 2).append("}\n");
            }
        }

        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public <X extends Throwable> void acceptAll(FieldValueConsumerVisitor<Void, X, ")
                .append(entityTypeInfo.getName()).append("> visitor) throws X {\n");

        for (final FieldInfo fieldInfo : fields) {
            if (!keyFields.contains(fieldInfo.getName())) {
                writer.append("\n");
                indent(writer, 2).append("if ((diff").append(getMaskField(bitIndex, fieldInfo)).append(" & MASK_")
                        .append(toUpperCase(fieldInfo.getName())).append(") != 0) {\n");
                indent(writer, 3).append("if ((mask").append(getMaskField(bitIndex, fieldInfo)).append(" & MASK_")
                        .append(toUpperCase(fieldInfo.getName())).append(") != 0) {\n");
                indent(writer, 4).append("visitor.visitSet(").append(entityTypeInfo.getName())
                        .append("EntityType.FIELD_").append(toUpperCase(fieldInfo.getName())).append(", ")
                        .append(fieldInfo.getName()).append(");\n");
                indent(writer, 3).append("} else {\n");
                indent(writer, 4).append("visitor.visitClear(").append(entityTypeInfo.getName())
                        .append("EntityType.FIELD_").append(toUpperCase(fieldInfo.getName())).append(");\n");
                indent(writer, 3).append("}\n");
                indent(writer, 2).append("}\n");
            }
        }

        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public void merge(").append(entityTypeInfo.getName())
                .append("DeltaRecord sourceDeltaRecord) {\n");

        for (final FieldInfo fieldInfo : fields) {
            if (!keyFields.contains(fieldInfo.getName())) {
                writer.append("\n");
                indent(writer, 2).append("if ((sourceDeltaRecord.diff").append(getMaskField(bitIndex, fieldInfo))
                        .append(" & MASK_").append(toUpperCase(fieldInfo.getName())).append(") != 0) {\n");
                indent(writer, 3).append("diff").append(getMaskField(bitIndex, fieldInfo)).append(" |= MASK_")
                        .append(toUpperCase(fieldInfo.getName())).append(";\n");
                indent(writer, 3).append("mask").append(getMaskField(bitIndex, fieldInfo)).append(" = mask")
                        .append(getMaskField(bitIndex, fieldInfo)).append(" & ~MASK_")
                        .append(toUpperCase(fieldInfo.getName()))
                        .append(" | sourceDeltaRecord.mask").append(getMaskField(bitIndex, fieldInfo))
                        .append(" & MASK_").append(toUpperCase(fieldInfo.getName()))
                        .append(";\n");
                indent(writer, 3).append(fieldInfo.getName()).append(" = sourceDeltaRecord.")
                        .append(fieldInfo.getName()).append(";\n");
                indent(writer, 2).append("}\n");
            }
        }

        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public void applyDefaults() {\n");

        for (final FieldInfo fieldInfo : fields) {
            if (!keyFields.contains(fieldInfo.getName())) {
                writer.append("\n");
                indent(writer, 2).append("if ((diff").append(getMaskField(bitIndex, fieldInfo)).append(" & MASK_")
                        .append(toUpperCase(fieldInfo.getName()))
                        .append(") == 0) {\n");
                indent(writer, 3).append("diff").append(getMaskField(bitIndex, fieldInfo)).append(" |= MASK_")
                        .append(toUpperCase(fieldInfo.getName())).append(";\n");
                indent(writer, 3).append(fieldInfo.getName()).append(" = DEFAULT_")
                        .append(toUpperCase(fieldInfo.getName())).append(";\n");
                indent(writer, 2).append("}\n");
            }
        }

        indent(writer, 1).append("}\n");

        writer.append("}\n");
    }

    private void appendCheck(Appendable writer, LinkedHashSet<String> keyFields, FieldInfo fieldInfo,
            Map<String, Integer> bitIndex) throws IOException {

        if (!keyFields.contains(fieldInfo.getName())) {
            indent(writer, 2).append("if ((diff").append(getMaskField(bitIndex, fieldInfo)).append(" & MASK_")
                    .append(toUpperCase(fieldInfo.getName())).append(") == 0) {\n");
            indent(writer, 3).append("throw new IllegalStateException();\n");
            indent(writer, 2).append("}\n");
        }
    }

    private void generateRecordClass(SchemaInfo schemaInfo, String packageName, Appendable writer,
            EntityTypeInfo entityTypeInfo) throws IOException {

        writer.append("package ").append(packageName).append(";\n");
        writer.append("\n");
        writer.append("import codes.writeonce.deltastore.api.EntityType;\n");
        writer.append("import codes.writeonce.deltastore.api.Id;\n");
        writer.append("import codes.writeonce.deltastore.api.Pool;\n");
        writer.append("import codes.writeonce.deltastore.api.Record;\n");
        writer.append("\n");
        writer.append("import javax.annotation.Nonnull;\n");
        writer.append("import java.util.Objects;\n");

        appendImport(writer, schemaInfo, entityTypeInfo);

        for (final SchemaInfo parentSchemaInfo : schemaInfo.getParentSchemaMap().values()) {
            for (final EntityTypeInfo eti : parentSchemaInfo.getTypeMap().values()) {
                if (eti.getSchemaName().equals(parentSchemaInfo.getName())) {
                    writer.append("import ").append(parentSchemaInfo.getPackageName()).append('.')
                            .append(eti.getName()).append(";\n");
                }
            }
        }

        writer.append("\n");
        writer.append("class ").append(entityTypeInfo.getName()).append("Record extends Record<")
                .append(schemaInfo.getName()).append("Store, ").append(entityTypeInfo.getName()).append(", ")
                .append(entityTypeInfo.getName()).append("DeltaRecord> implements ").append(entityTypeInfo.getName())
                .append(" {\n");

        final List<FieldInfo> fields = getFields(schemaInfo, entityTypeInfo);

        final Map<String, Integer> bitIndex = getBitIndex(fields);

        for (final FieldInfo fieldInfo : fields) {

            final long mask = getMask(bitIndex, fieldInfo);

            writer.append("\n");
            indent(writer, 1).append("static final long MASK_").append(toUpperCase(fieldInfo.getName())).append(" = ")
                    .append(String.valueOf(mask)).append("L;\n");

            writer.append("\n");
            indent(writer, 1).append("static final ")
                    .append(typeName(schemaInfo, fieldInfo, entityTypeInfo.getName(), false)).append(" DEFAULT_")
                    .append(toUpperCase(fieldInfo.getName())).append(" = ").append(getDefault(fieldInfo)).append(";\n");
        }

        writer.append("\n");
        indent(writer, 1).append("public static final Pool<").append(entityTypeInfo.getName())
                .append("Record> POOL = new Pool<").append(entityTypeInfo.getName()).append("Record>() {\n");
        indent(writer, 2).append("@Nonnull\n");
        indent(writer, 2).append("@Override\n");
        indent(writer, 2).append("public ").append(entityTypeInfo.getName()).append("Record get() {\n");
        indent(writer, 3).append("return new ").append(entityTypeInfo.getName()).append("Record();\n");
        indent(writer, 2).append("}\n");
        writer.append("\n");
        indent(writer, 2).append("@Override\n");
        indent(writer, 2).append("public void put(@Nonnull ").append(entityTypeInfo.getName())
                .append("Record value) {\n");
        indent(writer, 3).append("value.clean();\n");
        indent(writer, 2).append("}\n");
        indent(writer, 1).append("};\n");

        for (int i = 1; i < getMaskFieldCount(bitIndex); i++) {
            writer.append("\n");
            indent(writer, 1).append("public long mask").append(String.valueOf(i)).append(";\n");
        }

        for (final FieldInfo fieldInfo : fields) {
            writer.append("\n");
            indent(writer, 1).append("private ")
                    .append(typeName(schemaInfo, fieldInfo, entityTypeInfo.getName(), false)).append(" ")
                    .append(fieldInfo.getName()).append(";\n");
        }

        writer.append("\n");
        indent(writer, 1).append("private ").append(entityTypeInfo.getName()).append("Record() {\n");
        indent(writer, 2).append("// empty\n");
        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("public static ").append(entityTypeInfo.getName()).append("Record create(")
                .append(schemaInfo.getName()).append("Store store");

        for (final FieldInfo fieldInfo : fields) {
            if (!fieldInfo.isMutable() || !hasValidDefaultValue(fieldInfo)) {
                writer.append(", ").append(typeName(schemaInfo, fieldInfo, entityTypeInfo.getName(), false)).append(" ")
                        .append(fieldInfo.getName());
            }
        }

        writer.append(") {\n");
        writer.append("\n");

        indent(writer, 2).append("final ").append(entityTypeInfo.getName()).append("Record record = POOL.get();\n");
        indent(writer, 2).append("try {\n");
        indent(writer, 3).append("record.store = store;\n");
        indent(writer, 3).append("record.mask0 = MASK_EXISTS");

        for (final FieldInfo fieldInfo : fields) {
            if ((!fieldInfo.isMutable() || !hasValidDefaultValue(fieldInfo)) &&
                getMaskFieldNumber(bitIndex, fieldInfo) == 0) {
                writer.append(" | MASK_").append(toUpperCase(fieldInfo.getName()));
            }
        }

        writer.append(";\n");

        for (int i = 1; i < getMaskFieldCount(bitIndex); i++) {

            indent(writer, 3).append("record.mask").append(String.valueOf(i)).append(" = ");

            boolean first = true;

            for (final FieldInfo fieldInfo : fields) {
                if ((!fieldInfo.isMutable() || !hasValidDefaultValue(fieldInfo)) &&
                    getMaskFieldNumber(bitIndex, fieldInfo) == i) {
                    if (first) {
                        first = false;
                        writer.append("MASK_");
                    } else {
                        writer.append(" | MASK_");
                    }
                    writer.append(toUpperCase(fieldInfo.getName()));
                }
            }

            if (first) {
                writer.append("0");
            }

            writer.append(";\n");
        }

        for (final FieldInfo fieldInfo : fields) {
            indent(writer, 3).append("record.").append(fieldInfo.getName()).append(" = ");

            if (fieldInfo.isMutable() && hasValidDefaultValue(fieldInfo)) {
                writer.append("DEFAULT_").append(toUpperCase(fieldInfo.getName()));
            } else {
                writer.append(fieldInfo.getName());
            }

            writer.append(";\n");
        }

        writer.append("\n");
        indent(writer, 3).append("if (!store.currentTransaction.deferIndex) {\n");
        indent(writer, 4).append("record.reindex();\n");
        indent(writer, 3).append("}\n");
        writer.append("\n");
        indent(writer, 3).append("final ").append(entityTypeInfo.getName()).append("DeltaRecord deltaRecord = ")
                .append(entityTypeInfo.getName()).append("DeltaRecord.POOL.get();\n");
        indent(writer, 3).append("try {\n");
        indent(writer, 4).append("deltaRecord.init(null, null, null);\n");

        indent(writer, 4).append("deltaRecord.diff0 = MASK_EXISTS");

        for (final FieldInfo fieldInfo : fields) {
            if ((!fieldInfo.isMutable() || !hasValidDefaultValue(fieldInfo)) &&
                getMaskFieldNumber(bitIndex, fieldInfo) == 0) {
                writer.append(" | MASK_").append(toUpperCase(fieldInfo.getName()));
            }
        }

        writer.append(";\n");
        indent(writer, 4).append("deltaRecord.mask0 = MASK_EXISTS");

        for (final FieldInfo fieldInfo : fields) {
            if ((!fieldInfo.isMutable() || !hasValidDefaultValue(fieldInfo)) &&
                getMaskFieldNumber(bitIndex, fieldInfo) == 0) {
                writer.append(" | MASK_").append(toUpperCase(fieldInfo.getName()));
            }
        }

        writer.append(";\n");

        for (int i = 1; i < getMaskFieldCount(bitIndex); i++) {
            indent(writer, 4).append("deltaRecord.diff").append(String.valueOf(i)).append(" = ");

            boolean first = true;

            for (final FieldInfo fieldInfo : fields) {
                if ((!fieldInfo.isMutable() || !hasValidDefaultValue(fieldInfo)) &&
                    getMaskFieldNumber(bitIndex, fieldInfo) == i) {
                    if (first) {
                        first = false;
                        writer.append("MASK_");
                    } else {
                        writer.append(" | MASK_");
                    }
                    writer.append(toUpperCase(fieldInfo.getName()));
                }
            }

            if (first) {
                writer.append("0");
            }

            writer.append(";\n");
            indent(writer, 4).append("deltaRecord.mask").append(String.valueOf(i)).append(" = ");

            first = true;

            for (final FieldInfo fieldInfo : fields) {
                if ((!fieldInfo.isMutable() || !hasValidDefaultValue(fieldInfo)) &&
                    getMaskFieldNumber(bitIndex, fieldInfo) == i) {
                    if (first) {
                        first = false;
                        writer.append("MASK_");
                    } else {
                        writer.append(" | MASK_");
                    }
                    writer.append(toUpperCase(fieldInfo.getName()));
                }
            }

            if (first) {
                writer.append("0");
            }

            writer.append(";\n");
        }

        indent(writer, 4).append("record.diffStackTop = deltaRecord;\n");
        indent(writer, 3).append("} catch (Throwable e) {\n");
        indent(writer, 4).append("deltaRecord.release();\n");
        indent(writer, 4).append("throw e;\n");
        indent(writer, 3).append("}\n");
        indent(writer, 2).append("} catch (Throwable e) {\n");
        indent(writer, 3).append("record.release();\n");
        indent(writer, 3).append("throw e;\n");
        indent(writer, 2).append("}\n");
        writer.append("\n");
        indent(writer, 2).append("record.addToChangeList(store.currentTransaction);\n");
        indent(writer, 2).append("return record;\n");
        indent(writer, 1).append("}\n");

        for (final FieldInfo fieldInfo : fields) {

            appendGetterImpl(schemaInfo, writer, entityTypeInfo, fieldInfo);
            appendChangeTesterImpl(writer, fieldInfo, bitIndex);

            if (fieldInfo.isMutable()) {
                appendTesterImpl(writer, fieldInfo, bitIndex);
                appendClearerImpl(schemaInfo, writer, entityTypeInfo, fieldInfo, bitIndex);
                appendSetterImpl(schemaInfo, writer, entityTypeInfo, fieldInfo, bitIndex);
            }

            appendBackupField(writer, fieldInfo, bitIndex);
        }

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("protected void popRecordOnCommit() {\n");

        for (final FieldInfo fieldInfo : fields) {
            final String name = fieldInfo.getName();
            writer.append("\n");
            indent(writer, 2).append("if ((diffStackTop.diff").append(getMaskField(bitIndex, fieldInfo))
                    .append(" & ~diffStackTop.diffStackTop.diff").append(getMaskField(bitIndex, fieldInfo))
                    .append(" & MASK_").append(toUpperCase(name)).append(") != 0) {\n");
            indent(writer, 3).append("diffStackTop.diffStackTop.").append(name).append(" = diffStackTop.").append(name)
                    .append(";\n");
            indent(writer, 2).append("}\n");
        }

        writer.append("\n");

        for (int i = 1; i < getMaskFieldCount(bitIndex); i++) {
            indent(writer, 2).append("diffStackTop.diffStackTop.diff").append(String.valueOf(i))
                    .append(" |= diffStackTop.diff").append(String.valueOf(i)).append(";\n");
            indent(writer, 2).append("diffStackTop.diffStackTop.mask").append(String.valueOf(i))
                    .append(" ^= diffStackTop.mask").append(String.valueOf(i)).append(";\n");
        }

        indent(writer, 2).append("super.popRecordOnCommit();\n");
        indent(writer, 1).append("}\n");

        for (int i = 1; i < getMaskFieldCount(bitIndex); i++) {

            writer.append("\n");
            indent(writer, 1).append("protected boolean isDirtyField").append(String.valueOf(i))
                    .append("(long mask) {\n");
            indent(writer, 2).append("return (diffStackTop.diff").append(String.valueOf(i)).append(" & mask) != 0;\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("protected boolean isSetField").append(String.valueOf(i))
                    .append("(long mask) {\n");
            indent(writer, 2).append("return (this.mask").append(String.valueOf(i)).append(" & mask) != 0;\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("protected void revertMaskBit").append(String.valueOf(i))
                    .append("(long mask) {\n");
            indent(writer, 2).append("this.mask").append(String.valueOf(i)).append(" ^= diffStackTop.mask")
                    .append(String.valueOf(i)).append(" & mask;\n");
            indent(writer, 1).append("}\n");
        }

        appendUnindexCommit(schemaInfo, writer, entityTypeInfo, bitIndex);
        appendReindexCommit(schemaInfo, writer, entityTypeInfo, bitIndex);
        appendRollback(schemaInfo, writer, entityTypeInfo, bitIndex);
        appendNormalize(schemaInfo, writer, entityTypeInfo, bitIndex);

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public void clean() {\n");
        indent(writer, 2).append("super.clean();\n");

        for (int i = 1; i < getMaskFieldCount(bitIndex); i++) {
            indent(writer, 2).append("mask").append(String.valueOf(i)).append(" = 0;\n");
        }

        for (final FieldInfo fieldInfo : fields) {
            indent(writer, 2).append(fieldInfo.getName()).append(" = ").append(getDefault(fieldInfo)).append(";\n");
        }

        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("private void reindex() {\n");
        appendRecursiveReindex(writer, getKeys(schemaInfo, entityTypeInfo).iterator(), 2);
        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("protected void unindex() {\n");

        for (final KeyInfo keyInfo : getKeys(schemaInfo, entityTypeInfo)) {
            indent(writer, 2).append("store.").append(withSmallLetter(keyInfo.getName())).append("().remove(this");
            for (final String fieldName : keyInfo.getFields()) {
                writer.append(", ").append(fieldName);
            }
            writer.append(");\n");
        }

        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public EntityType<").append(entityTypeInfo.getName()).append("> getEntityType() {\n");
        indent(writer, 2).append("return ").append(entityTypeInfo.getName()).append("EntityType.INSTANCE;\n");
        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public void release() {\n");
        indent(writer, 2).append("POOL.put(this);\n");
        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("@Nonnull\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("protected ").append(entityTypeInfo.getName()).append("DeltaRecord get() {\n");
        indent(writer, 2).append("return ").append(entityTypeInfo.getName()).append("DeltaRecord.POOL.get();\n");
        indent(writer, 1).append("}\n");

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public ").append(entityTypeInfo.getName()).append(" asEntity() {\n");
        indent(writer, 2).append("return this;\n");
        indent(writer, 1).append("}\n");

        writer.append("}\n");
    }

    private Map<String, Integer> getBitIndex(List<FieldInfo> fields) {
        int index = 0;

        final Map<String, Integer> bitIndex = new HashMap<>();

        for (final FieldInfo field : fields) {
            bitIndex.put(field.getName(), ++index);
        }
        return bitIndex;
    }

    private long getMask(Map<String, Integer> bitIndex, FieldInfo fieldInfo) {
        return 1L << (bitIndex.get(fieldInfo.getName()) & 0x3f);
    }

    private String getMaskField(Map<String, Integer> bitIndex, FieldInfo fieldInfo) {
        return String.valueOf(getMaskFieldNumber(bitIndex, fieldInfo));
    }

    private int getMaskFieldNumber(Map<String, Integer> bitIndex, FieldInfo fieldInfo) {
        return bitIndex.get(fieldInfo.getName()) >> 6;
    }

    private int getMaskFieldNumber(Map<String, Integer> bitIndex, String fieldName) {
        return bitIndex.get(fieldName) >> 6;
    }

    private int getMaskFieldCount(Map<String, Integer> bitIndex) {
        return bitIndex.size() + 64 >> 6;
    }

    private void appendRecursiveReindex(Appendable writer, Iterator<KeyInfo> iterator, int level)
            throws IOException {

        final KeyInfo keyInfo = iterator.next();

        indent(writer, level).append("store.").append(withSmallLetter(keyInfo.getName())).append("().add(this");
        for (final String fieldName : keyInfo.getFields()) {
            writer.append(", ").append(fieldName);
        }
        writer.append(");\n");

        if (iterator.hasNext()) {
            indent(writer, level).append("try {\n");
            appendRecursiveReindex(writer, iterator, level + 1);
            indent(writer, level).append("} catch (Throwable e) {\n");
            indent(writer, level + 1).append("store.").append(withSmallLetter(keyInfo.getName()))
                    .append("().remove(this");
            for (final String fieldName : keyInfo.getFields()) {
                writer.append(", ").append(fieldName);
            }
            writer.append(");\n");
            indent(writer, level + 1).append("throw e;\n");
            indent(writer, level).append("}\n");
        }
    }

    private void appendNormalize(SchemaInfo schemaInfo, Appendable writer, EntityTypeInfo entityTypeInfo,
            Map<String, Integer> bitIndex) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public boolean normalizeChange() {\n");
        writer.append("\n");
        indent(writer, 2).append("if (isDirtyField0(MASK_EXISTS) && (diffStackTop.mask0 & MASK_EXISTS) == 0) {\n");
        indent(writer, 3).append("diffStackTop.diff0 ^= MASK_EXISTS;\n");
        indent(writer, 2).append("}\n");
        writer.append("\n");
        indent(writer, 2).append("diffStackTop.mask0 = diffStackTop.mask0 & ~MASK_EXISTS | mask0 & MASK_EXISTS;\n");
        writer.append("\n");

        final LinkedHashSet<String> keyFields = getKey(schemaInfo, entityTypeInfo).getFields();

        for (final String keyFieldName : keyFields) {
            indent(writer, 2).append("diffStackTop.").append(keyFieldName).append(" = ").append(keyFieldName)
                    .append(";\n");
        }

        for (final FieldInfo fieldInfo : getFields(schemaInfo, entityTypeInfo)) {
            if (!keyFields.contains(fieldInfo.getName())) {
                writer.append("\n");
                indent(writer, 2).append("if (isDirtyField").append(getMaskField(bitIndex, fieldInfo)).append("(MASK_")
                        .append(toUpperCase(fieldInfo.getName())).append(")) {\n");
                indent(writer, 3).append("if ((diffStackTop.mask").append(getMaskField(bitIndex, fieldInfo))
                        .append(" & MASK_").append(toUpperCase(fieldInfo.getName())).append(") == 0) {\n");

                if (isValueTestable(fieldInfo)) {
                    indent(writer, 4).append("if (diffStackTop.").append(fieldInfo.getName()).append(" == ")
                            .append(fieldInfo.getName()).append(") {\n");
                } else {
                    indent(writer, 4).append("if (Objects.equals(diffStackTop.").append(fieldInfo.getName())
                            .append(", ").append(fieldInfo.getName()).append(")) {\n");
                }

                indent(writer, 5).append("diffStackTop.diff").append(getMaskField(bitIndex, fieldInfo))
                        .append(" ^= MASK_").append(toUpperCase(fieldInfo.getName())).append(";\n");
                indent(writer, 5).append("diffStackTop.").append(fieldInfo.getName()).append(" = DEFAULT_")
                        .append(toUpperCase(fieldInfo.getName())).append(";\n");
                indent(writer, 4).append("} else {\n");
                indent(writer, 5).append("diffStackTop.mask").append(getMaskField(bitIndex, fieldInfo))
                        .append(" ^= mask").append(getMaskField(bitIndex, fieldInfo)).append(" & MASK_")
                        .append(toUpperCase(fieldInfo.getName())).append(";\n");
                indent(writer, 5).append("diffStackTop.").append(fieldInfo.getName()).append(" = ")
                        .append(fieldInfo.getName()).append(";\n");
                indent(writer, 4).append("}\n");
                indent(writer, 3).append("} else {\n");
                indent(writer, 4).append("diffStackTop.mask").append(getMaskField(bitIndex, fieldInfo))
                        .append(" ^= mask").append(getMaskField(bitIndex, fieldInfo)).append(" & MASK_")
                        .append(toUpperCase(fieldInfo.getName()))
                        .append(" ^ MASK_").append(toUpperCase(fieldInfo.getName())).append(";\n");
                indent(writer, 4).append("diffStackTop.").append(fieldInfo.getName()).append(" = ")
                        .append(fieldInfo.getName()).append(";\n");
                indent(writer, 3).append("}\n");
                indent(writer, 2).append("}\n");
            }
        }

        writer.append("\n");
        indent(writer, 2).append("return diffStackTop.diff0 != 0");

        for (int i = 1; i < getMaskFieldCount(bitIndex); i++) {
            writer.append(" || diffStackTop.diff").append(String.valueOf(i)).append(" != 0");
        }

        writer.append(";\n");
        indent(writer, 1).append("}\n");
    }

    private void appendRollback(SchemaInfo schemaInfo, Appendable writer, EntityTypeInfo entityTypeInfo,
            Map<String, Integer> bitIndex) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public Record<").append(schemaInfo.getName()).append("Store, ?, ?> rollback() {\n");
        writer.append("\n");
        indent(writer, 2).append("long reindex0 = diffStackTop.mask0 & MASK_EXISTS;\n");

        final List<FieldInfo> fields = getFields(schemaInfo, entityTypeInfo);

        for (int i = 1; i < getMaskFieldCount(bitIndex); i++) {
            if (hasMask(bitIndex, i,
                    fields.stream().filter(fieldInfo -> !getKeys(schemaInfo, fieldInfo, entityTypeInfo).isEmpty())
                            .map(FieldInfo::getName))) {
                indent(writer, 2).append("long reindex").append(String.valueOf(i)).append(" = 0;\n");
            }
        }

        for (final FieldInfo fieldInfo : fields) {
            if (getKeys(schemaInfo, fieldInfo, entityTypeInfo).isEmpty()) {
                writer.append("\n");
                indent(writer, 2).append("if (isDirtyField").append(getMaskField(bitIndex, fieldInfo)).append("(MASK_")
                        .append(toUpperCase(fieldInfo.getName())).append(")) {\n");
                indent(writer, 3).append("mask").append(getMaskField(bitIndex, fieldInfo))
                        .append(" ^= diffStackTop.mask").append(getMaskField(bitIndex, fieldInfo)).append(" & MASK_")
                        .append(toUpperCase(fieldInfo.getName())).append(";\n");
                indent(writer, 3).append(fieldInfo.getName()).append(" = diffStackTop.").append(fieldInfo.getName())
                        .append(";\n");
                indent(writer, 3).append("diffStackTop.diff").append(getMaskField(bitIndex, fieldInfo))
                        .append(" &= ~MASK_").append(toUpperCase(fieldInfo.getName())).append(";\n");
                indent(writer, 3).append("diffStackTop.mask").append(getMaskField(bitIndex, fieldInfo))
                        .append(" &= ~MASK_").append(toUpperCase(fieldInfo.getName())).append(";\n");
                indent(writer, 2).append("}\n");
            } else {
                appendSaveOldField(schemaInfo, writer, entityTypeInfo, fieldInfo, bitIndex);
            }
        }

        appendExistFlags(writer);

        for (final KeyInfo keyInfo : getKeys(schemaInfo, entityTypeInfo)) {
            writer.append("\n");
            indent(writer, 2).append("if (!store.currentTransaction.deferIndex && ((reindex0 & (MASK_EXISTS");
            for (final String fieldName : keyInfo.getFields()) {
                if (getMaskFieldNumber(bitIndex, fieldName) == 0) {
                    writer.append(" | MASK_").append(toUpperCase(fieldName));
                }
            }
            writer.append(")) != 0");

            for (int i = 1; i < getMaskFieldCount(bitIndex); i++) {
                if (hasMask(bitIndex, i, keyInfo.getFields().stream())) {
                    writer.append(" || (reindex").append(String.valueOf(i)).append(" & (");
                    boolean first = true;
                    for (final String fieldName : keyInfo.getFields()) {
                        if (getMaskFieldNumber(bitIndex, fieldName) == i) {
                            if (first) {
                                first = false;
                                writer.append("MASK_");
                            } else {
                                writer.append(" | MASK_");
                            }
                            writer.append(toUpperCase(fieldName));
                        }
                    }
                    writer.append(")) != 0");
                }
            }

            writer.append(")) {\n");

            indent(writer, 3).append("if (existed) {\n");
            indent(writer, 4).append("store.").append(withSmallLetter(keyInfo.getName())).append("().add");
            if (keyInfo.isUnique()) {
                writer.append("Unsafe");
            }
            writer.append("(this");
            for (final String fieldName : keyInfo.getFields()) {
                writer.append(", old").append(withCapitalLetter(fieldName));
            }
            writer.append(");\n");
            indent(writer, 3).append("}\n");

            indent(writer, 3).append("if (exists) {\n");
            indent(writer, 4).append("store.").append(withSmallLetter(keyInfo.getName())).append("().remove(this");
            for (final String fieldName : keyInfo.getFields()) {
                writer.append(", ").append(fieldName);
            }
            writer.append(");\n");
            indent(writer, 3).append("}\n");

            indent(writer, 2).append("}\n");
        }

        writer.append("\n");

        for (final FieldInfo fieldInfo : fields) {
            if (!getKeys(schemaInfo, fieldInfo, entityTypeInfo).isEmpty()) {
                indent(writer, 2).append(fieldInfo.getName()).append(" = old")
                        .append(withCapitalLetter(fieldInfo.getName())).append(";\n");
            }
        }

        for (int i = 0; i < getMaskFieldCount(bitIndex); i++) {
            indent(writer, 2).append("mask").append(String.valueOf(i)).append(" ^= diffStackTop.mask")
                    .append(String.valueOf(i)).append(";\n");
        }

        writer.append("\n");
        indent(writer, 2).append("final Record<").append(schemaInfo.getName())
                .append("Store, ?, ?> nextToRollback = nextInChangeList;\n");
        indent(writer, 2).append("popRecord();\n");

        writer.append("\n");
        indent(writer, 2).append("if (currentTransaction == null && (mask0 & MASK_EXISTS) == 0) {\n");
        indent(writer, 3).append("release();\n");
        indent(writer, 2).append("}\n");

        writer.append("\n");
        indent(writer, 2).append("return nextToRollback;\n");
        indent(writer, 1).append("}\n");
    }

    private boolean hasMask(Map<String, Integer> bitIndex, int i, Stream<String> stream) {
        return stream.anyMatch(fieldName -> getMaskFieldNumber(bitIndex, fieldName) == i);
    }

    private void appendUnindexCommit(SchemaInfo schemaInfo, Appendable writer, EntityTypeInfo entityTypeInfo,
            Map<String, Integer> bitIndex) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public Record<").append(schemaInfo.getName())
                .append("Store, ?, ?> unindexCommit() {\n");
        writer.append("\n");
        indent(writer, 2).append("long reindex0 = diffStackTop.mask0 & MASK_EXISTS;\n");

        final List<FieldInfo> fields = getFields(schemaInfo, entityTypeInfo);

        for (int i = 1; i < getMaskFieldCount(bitIndex); i++) {
            if (hasMask(bitIndex, i,
                    fields.stream().filter(fieldInfo -> !getKeys(schemaInfo, fieldInfo, entityTypeInfo).isEmpty())
                            .map(FieldInfo::getName))) {
                indent(writer, 2).append("long reindex").append(String.valueOf(i)).append(" = 0;\n");
            }
        }

        for (final FieldInfo fieldInfo : fields) {
            if (!getKeys(schemaInfo, fieldInfo, entityTypeInfo).isEmpty()) {
                appendSaveOldField(schemaInfo, writer, entityTypeInfo, fieldInfo, bitIndex);
            }
        }

        writer.append("\n");
        indent(writer, 2).append("if (((mask0 ^ diffStackTop.mask0) & MASK_EXISTS) != 0) {\n");

        for (final KeyInfo keyInfo : getKeys(schemaInfo, entityTypeInfo)) {
            writer.append("\n");
            indent(writer, 3).append("if (");

            final LinkedHashSet<String> keyFields = keyInfo.getFields();

            writer.append("(reindex0 & (MASK_EXISTS");
            for (final String fieldName : keyFields) {
                if (getMaskFieldNumber(bitIndex, fieldName) == 0) {
                    writer.append(" | MASK_").append(toUpperCase(fieldName));
                }
            }
            writer.append(")) != 0");

            for (int i = 1; i < getMaskFieldCount(bitIndex); i++) {
                if (hasMask(bitIndex, i, keyFields.stream())) {
                    boolean first = true;
                    writer.append(" || (reindex").append(String.valueOf(i)).append(" & (");
                    for (final String fieldName : keyFields) {
                        if (getMaskFieldNumber(bitIndex, fieldName) == i) {
                            if (first) {
                                first = false;
                                writer.append("MASK_");
                            } else {
                                writer.append(" | MASK_");
                            }
                            writer.append(toUpperCase(fieldName));
                        }
                    }
                    writer.append(")) != 0");
                }
            }

            writer.append(") {\n");

            indent(writer, 4).append("store.").append(withSmallLetter(keyInfo.getName())).append("().remove(this");
            for (final String fieldName : keyFields) {
                writer.append(", old").append(withCapitalLetter(fieldName));
            }
            writer.append(");\n");

            indent(writer, 3).append("}\n");
        }

        indent(writer, 2).append("}\n");

        writer.append("\n");
        indent(writer, 2).append("return nextInChangeList;\n");
        indent(writer, 1).append("}\n");
    }

    private void appendReindexCommit(SchemaInfo schemaInfo, Appendable writer, EntityTypeInfo entityTypeInfo,
            Map<String, Integer> bitIndex) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public Record<").append(schemaInfo.getName())
                .append("Store, ?, ?> reindexCommit() {\n");
        writer.append("\n");
        indent(writer, 2).append("long reindex0 = diffStackTop.mask0 & MASK_EXISTS;\n");

        final List<FieldInfo> fields = getFields(schemaInfo, entityTypeInfo);

        for (int i = 1; i < getMaskFieldCount(bitIndex); i++) {
            if (hasMask(bitIndex, i,
                    fields.stream().filter(fieldInfo -> !getKeys(schemaInfo, fieldInfo, entityTypeInfo).isEmpty())
                            .map(FieldInfo::getName))) {
                indent(writer, 2).append("long reindex").append(String.valueOf(i)).append(" = 0;\n");
            }
        }

        for (final FieldInfo fieldInfo : fields) {
            if (!getKeys(schemaInfo, fieldInfo, entityTypeInfo).isEmpty()) {
                appendCheckIndexField(writer, fieldInfo, bitIndex);
            }
        }

        writer.append("\n");
        indent(writer, 2).append("if ((mask0 & MASK_EXISTS) != 0) {\n");

        for (final KeyInfo keyInfo : getKeys(schemaInfo, entityTypeInfo)) {
            writer.append("\n");
            indent(writer, 3).append("if (");

            final LinkedHashSet<String> keyFields = keyInfo.getFields();

            writer.append("(reindex0 & (MASK_EXISTS");
            for (final String fieldName : keyFields) {
                if (getMaskFieldNumber(bitIndex, fieldName) == 0) {
                    writer.append(" | MASK_").append(toUpperCase(fieldName));
                }
            }
            writer.append(")) != 0");

            for (int i = 1; i < getMaskFieldCount(bitIndex); i++) {
                if (hasMask(bitIndex, i, keyFields.stream())) {
                    boolean first = true;
                    writer.append(" || (reindex").append(String.valueOf(i)).append(" & (");
                    for (final String fieldName : keyFields) {
                        if (getMaskFieldNumber(bitIndex, fieldName) == i) {
                            if (first) {
                                first = false;
                                writer.append("MASK_");
                            } else {
                                writer.append(" | MASK_");
                            }
                            writer.append(toUpperCase(fieldName));
                        }
                    }
                    writer.append(")) != 0");
                }
            }

            writer.append(") {\n");

            indent(writer, 4).append("store.").append(withSmallLetter(keyInfo.getName())).append("().add(this");
            for (final String fieldName : keyFields) {
                writer.append(", ").append(fieldName);
            }
            writer.append(");\n");

            indent(writer, 3).append("}\n");
        }

        indent(writer, 2).append("}\n");

        writer.append("\n");
        indent(writer, 2).append("return nextInChangeList;\n");
        indent(writer, 1).append("}\n");
    }

    private void appendExistFlags(Appendable writer) throws IOException {
        writer.append("\n");
        indent(writer, 2).append("final boolean existed = ((mask0 ^ diffStackTop.mask0) & MASK_EXISTS) != 0;\n");
        indent(writer, 2).append("final boolean exists = (mask0 & MASK_EXISTS) != 0;\n");
    }

    private void appendSaveOldField(SchemaInfo schemaInfo, Appendable writer, EntityTypeInfo entityTypeInfo,
            FieldInfo fieldInfo, Map<String, Integer> bitIndex) throws IOException {
        writer.append("\n");
        indent(writer, 2).append("final ").append(typeName(schemaInfo, fieldInfo, entityTypeInfo.getName(), false))
                .append(" old").append(withCapitalLetter(fieldInfo.getName())).append(";\n");
        indent(writer, 2).append("if (isDirtyField").append(getMaskField(bitIndex, fieldInfo)).append("(MASK_")
                .append(toUpperCase(fieldInfo.getName())).append(")) {\n");
        if (isValueTestable(fieldInfo)) {
            indent(writer, 3).append("if (diffStackTop.").append(fieldInfo.getName()).append(" != ")
                    .append(fieldInfo.getName()).append(") {\n");
        } else {
            indent(writer, 3).append("if (!Objects.equals(diffStackTop.").append(fieldInfo.getName())
                    .append(", ").append(fieldInfo.getName()).append(")) {\n");
        }
        indent(writer, 4).append("reindex").append(getMaskField(bitIndex, fieldInfo)).append(" |= MASK_")
                .append(toUpperCase(fieldInfo.getName())).append(";\n");
        indent(writer, 3).append("}\n");
        indent(writer, 3).append("old").append(withCapitalLetter(fieldInfo.getName()))
                .append(" = diffStackTop.").append(fieldInfo.getName()).append(";\n");
        indent(writer, 2).append("} else {\n");
        indent(writer, 3).append("old").append(withCapitalLetter(fieldInfo.getName())).append(" = ")
                .append(fieldInfo.getName()).append(";\n");
        indent(writer, 2).append("}\n");
    }

    private void appendCheckIndexField(Appendable writer, FieldInfo fieldInfo,
            Map<String, Integer> bitIndex) throws IOException {
        writer.append("\n");
        indent(writer, 2).append("if (isDirtyField").append(getMaskField(bitIndex, fieldInfo)).append("(MASK_")
                .append(toUpperCase(fieldInfo.getName())).append(") && ");
        if (isValueTestable(fieldInfo)) {
            writer.append("diffStackTop.").append(fieldInfo.getName()).append(" != ").append(fieldInfo.getName());
        } else {
            writer.append("!Objects.equals(diffStackTop.").append(fieldInfo.getName()).append(", ")
                    .append(fieldInfo.getName()).append(")");
        }
        writer.append(") {\n");
        indent(writer, 3).append("reindex").append(getMaskField(bitIndex, fieldInfo)).append(" |= MASK_")
                .append(toUpperCase(fieldInfo.getName())).append(";\n");
        indent(writer, 2).append("}\n");
    }

    private void appendBackupField(Appendable writer, FieldInfo fieldInfo,
            Map<String, Integer> bitIndex) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("private void backupField").append(withCapitalLetter(fieldInfo.getName()))
                .append("() {\n");
        writer.append("\n");
        indent(writer, 2).append("if (isCleanRecord()) {\n");
        indent(writer, 3).append("pushRecord(store.currentTransaction);\n");
        indent(writer, 2).append("} else if (isDirtyField").append(getMaskField(bitIndex, fieldInfo)).append("(MASK_")
                .append(toUpperCase(fieldInfo.getName())).append(")) {\n");
        indent(writer, 3).append("return;\n");
        indent(writer, 2).append("}\n");
        writer.append("\n");
        indent(writer, 2).append("diffStackTop.diff").append(getMaskField(bitIndex, fieldInfo)).append(" ^= MASK_")
                .append(toUpperCase(fieldInfo.getName())).append(";\n");
        indent(writer, 2).append("diffStackTop.").append(fieldInfo.getName()).append(" = ").append(fieldInfo.getName())
                .append(";\n");
        indent(writer, 1).append("}\n");
    }

    private void appendChangeTesterImpl(Appendable writer, FieldInfo fieldInfo,
            Map<String, Integer> bitIndex) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public boolean ").append(changeTesterName(fieldInfo)).append("() {\n");
        indent(writer, 2).append("return diffStackTop != null && (diffStackTop.diff")
                .append(getMaskField(bitIndex, fieldInfo)).append(" & MASK_")
                .append(toUpperCase(fieldInfo.getName())).append(") != 0 && ((diffStackTop.mask")
                .append(getMaskField(bitIndex, fieldInfo)).append(" & MASK_")
                .append(toUpperCase(fieldInfo.getName())).append(") != 0 || ");
        if (isValueTestable(fieldInfo)) {
            writer.append("diffStackTop.").append(fieldInfo.getName()).append(" != ").append(fieldInfo.getName())
                    .append(");\n");
        } else {
            writer.append("!Objects.equals(diffStackTop.").append(fieldInfo.getName()).append(", ")
                    .append(fieldInfo.getName()).append("));\n");
        }
        indent(writer, 1).append("}\n");
    }

    private void appendTesterImpl(Appendable writer, FieldInfo fieldInfo,
            Map<String, Integer> bitIndex) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public boolean ").append(testerName(fieldInfo)).append("() {\n");
        indent(writer, 2).append("return isSetField").append(getMaskField(bitIndex, fieldInfo)).append("(MASK_")
                .append(toUpperCase(fieldInfo.getName())).append(");\n");
        indent(writer, 1).append("}\n");
    }

    private void appendClearerImpl(SchemaInfo schemaInfo, Appendable writer, EntityTypeInfo entityTypeInfo,
            FieldInfo fieldInfo, Map<String, Integer> bitIndex) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public void ").append(clearerName(fieldInfo)).append("() {\n");
        writer.append("\n");
        indent(writer, 2).append("checkTransaction();\n");

        final List<KeyInfo> keys = getKeys(schemaInfo, fieldInfo, entityTypeInfo);
        if (!keys.isEmpty()) {
            writer.append("\n");
            indent(writer, 2).append("if (!store.currentTransaction.deferIndex && ");
            if (isValueTestable(fieldInfo)) {
                writer.append("this.").append(fieldInfo.getName()).append(" != DEFAULT_")
                        .append(toUpperCase(fieldInfo.getName()));
            } else {
                writer.append("!Objects.equals(this.").append(fieldInfo.getName()).append(", DEFAULT_")
                        .append(toUpperCase(fieldInfo.getName())).append(")");
            }
            writer.append(") {\n");

            for (final KeyInfo keyInfo : keys) {
                indent(writer, 3).append("store.").append(withSmallLetter(keyInfo.getName()))
                        .append("().add(this");
                for (final String fieldName : keyInfo.getFields()) {
                    if (fieldName.equals(fieldInfo.getName())) {
                        writer.append(", DEFAULT_").append(toUpperCase(fieldInfo.getName()));
                    } else {
                        writer.append(", this.").append(fieldName);
                    }
                }
                writer.append(");\n");
            }

            appendUnindex(writer, keys);

            indent(writer, 2).append("}\n");
        }

        writer.append("\n");
        indent(writer, 2).append("if (isSetField").append(getMaskField(bitIndex, fieldInfo)).append("(MASK_")
                .append(toUpperCase(fieldInfo.getName())).append(")) {\n");
        indent(writer, 3).append("backupField").append(withCapitalLetter(fieldInfo.getName())).append("();\n");
        indent(writer, 3).append("diffStackTop.mask").append(getMaskField(bitIndex, fieldInfo)).append(" ^= MASK_")
                .append(toUpperCase(fieldInfo.getName())).append(";\n");
        indent(writer, 3).append("mask").append(getMaskField(bitIndex, fieldInfo)).append(" ^= MASK_")
                .append(toUpperCase(fieldInfo.getName())).append(";\n");
        indent(writer, 3).append(fieldInfo.getName()).append(" = DEFAULT_").append(toUpperCase(fieldInfo.getName()))
                .append(";\n");
        indent(writer, 2).append("}\n");
        indent(writer, 1).append("}\n");
    }

    private void appendGetterImpl(SchemaInfo schemaInfo, Appendable writer, EntityTypeInfo entityTypeInfo,
            FieldInfo fieldInfo) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public ").append(typeName(schemaInfo, fieldInfo, entityTypeInfo.getName(), false))
                .append(" ").append(getterName(fieldInfo)).append("() {\n");
        indent(writer, 2).append("return ").append(fieldInfo.getName()).append(";\n");
        indent(writer, 1).append("}\n");
    }

    private void appendSetterImpl(SchemaInfo schemaInfo, Appendable writer, EntityTypeInfo entityTypeInfo,
            FieldInfo fieldInfo, Map<String, Integer> bitIndex) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        indent(writer, 1).append("public void ").append(setterName(fieldInfo)).append("(")
                .append(typeName(schemaInfo, fieldInfo, entityTypeInfo.getName(), false)).append(" value) {\n");

        writer.append("\n");
        indent(writer, 2).append("checkTransaction();\n");

        final List<KeyInfo> keys = getKeys(schemaInfo, fieldInfo, entityTypeInfo);
        if (!keys.isEmpty()) {
            writer.append("\n");
            indent(writer, 2).append("if (!store.currentTransaction.deferIndex && ");
            if (isValueTestable(fieldInfo)) {
                writer.append("this.").append(fieldInfo.getName()).append(" != value");
            } else {
                writer.append("!Objects.equals(").append(fieldInfo.getName()).append(", value)");
            }
            writer.append(") {\n");

            for (final KeyInfo keyInfo : keys) {
                indent(writer, 3).append("store.").append(withSmallLetter(keyInfo.getName()))
                        .append("().add(this");
                for (final String fieldName : keyInfo.getFields()) {
                    if (fieldName.equals(fieldInfo.getName())) {
                        writer.append(", value");
                    } else {
                        writer.append(", this.").append(fieldName);
                    }
                }
                writer.append(");\n");
            }

            appendUnindex(writer, keys);

            indent(writer, 2).append("}\n");
        }

        writer.append("\n");
        indent(writer, 2).append("if (!isSetField").append(getMaskField(bitIndex, fieldInfo)).append("(MASK_")
                .append(toUpperCase(fieldInfo.getName())).append(")) {\n");
        indent(writer, 3).append("backupField").append(withCapitalLetter(fieldInfo.getName())).append("();\n");
        indent(writer, 3).append("diffStackTop.mask").append(getMaskField(bitIndex, fieldInfo)).append(" ^= MASK_")
                .append(toUpperCase(fieldInfo.getName())).append(";\n");
        indent(writer, 3).append("mask").append(getMaskField(bitIndex, fieldInfo)).append(" ^= MASK_")
                .append(toUpperCase(fieldInfo.getName())).append(";\n");

        if (isValueTestable(fieldInfo)) {
            indent(writer, 2).append("} else if (this.").append(fieldInfo.getName()).append(" != value) {\n");
        } else {
            indent(writer, 2).append("} else if (!Objects.equals(this.").append(fieldInfo.getName())
                    .append(", value)) {\n");
        }

        indent(writer, 3).append("backupField").append(withCapitalLetter(fieldInfo.getName())).append("();\n");
        indent(writer, 2).append("}\n");
        writer.append("\n");
        indent(writer, 2).append("this.").append(fieldInfo.getName()).append(" = value;\n");
        indent(writer, 1).append("}\n");
    }

    private void appendUnindex(Appendable writer, List<KeyInfo> keys) throws IOException {
        for (final KeyInfo keyInfo : keys) {
            indent(writer, 3).append("store.").append(withSmallLetter(keyInfo.getName()))
                    .append("().remove(this");
            for (final String fieldName : keyInfo.getFields()) {
                writer.append(", this.").append(fieldName);
            }
            writer.append(");\n");
        }
    }

    private boolean isValueTestable(FieldInfo fieldInfo) {
        return fieldInfo.accept(new FieldInfo.Visitor<Boolean, RuntimeException>() {
            @Override
            public Boolean visit(StringFieldInfo fieldInfo) {
                return false;
            }

            @Override
            public Boolean visit(IntegerFieldInfo fieldInfo) {
                return !fieldInfo.isNullable();
            }

            @Override
            public Boolean visit(LongFieldInfo fieldInfo) {
                return !fieldInfo.isNullable();
            }

            @Override
            public Boolean visit(BooleanFieldInfo fieldInfo) {
                return !fieldInfo.isNullable();
            }

            @Override
            public Boolean visit(InstantFieldInfo fieldInfo) {
                return false;
            }

            @Override
            public Boolean visit(IdFieldInfo fieldInfo) {
                return false;
            }

            @Override
            public Boolean visit(EnumFieldInfo fieldInfo) {
                return true;
            }

            @Override
            public Boolean visit(BigDecimalFieldInfo fieldInfo) {
                return false;
            }
        });
    }

    private List<KeyInfo> getKeys(SchemaInfo schemaInfo, FieldInfo fieldInfo, EntityTypeInfo entityTypeInfo) {
        final List<KeyInfo> keys = new ArrayList<>();
        for (final String parentName : entityTypeInfo.getParents()) {
            keys.addAll(getKeys(schemaInfo, fieldInfo, schemaInfo.getTypeMap().get(parentName)));
        }
        for (final KeyInfo keyInfo : entityTypeInfo.getKeyMap().values()) {
            if (keyInfo.getFields().contains(fieldInfo.getName())) {
                keys.add(keyInfo);
            }
        }
        return keys;
    }

    private List<KeyInfo> getKeys(SchemaInfo schemaInfo, EntityTypeInfo entityTypeInfo) {
        final List<KeyInfo> keys = new ArrayList<>();
        for (final String parentName : entityTypeInfo.getParents()) {
            keys.addAll(getKeys(schemaInfo, schemaInfo.getTypeMap().get(parentName)));
        }
        keys.addAll(entityTypeInfo.getKeyMap().values());
        return keys;
    }

    private CharSequence getDefault(FieldInfo fieldInfo) {
        return fieldInfo.accept(new FieldInfo.Visitor<CharSequence, RuntimeException>() {
            @Override
            public CharSequence visit(StringFieldInfo fieldInfo) {
                return "null";
            }

            @Override
            public CharSequence visit(IntegerFieldInfo fieldInfo) {
                return fieldInfo.isNullable() ? "null" : "0";
            }

            @Override
            public CharSequence visit(LongFieldInfo fieldInfo) {
                return fieldInfo.isNullable() ? "null" : "0L";
            }

            @Override
            public CharSequence visit(BooleanFieldInfo fieldInfo) {
                return fieldInfo.isNullable() ? "null" : "false";
            }

            @Override
            public CharSequence visit(InstantFieldInfo fieldInfo) {
                return "null";
            }

            @Override
            public CharSequence visit(IdFieldInfo fieldInfo) {
                return "null";
            }

            @Override
            public CharSequence visit(EnumFieldInfo fieldInfo) {
                return "null";
            }

            @Override
            public CharSequence visit(BigDecimalFieldInfo fieldInfo) {
                return "null";
            }
        });
    }

    private void generateTypeInterfaces(SchemaInfo schemaInfo, Path path, Charset charset) throws IOException {

        final Set<String> identifiableEntityNames = schemaInfo.getTypeMap().values().stream()
                .flatMap(e -> e.getFieldMap().values().stream())
                .map(e -> e.accept(IdentifiableEntityVisitor.INSTANCE))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        for (final EntityTypeInfo entityTypeInfo : schemaInfo.getTypeMap().values()) {
            if (entityTypeInfo.getSchemaName().equals(schemaInfo.getName())) {
                generateTypeInterface(schemaInfo, path, charset, identifiableEntityNames, entityTypeInfo);
            }
        }
    }

    private void generateTypeInterface(SchemaInfo schemaInfo, Path path, Charset charset,
            Set<String> identifiableEntityNames, EntityTypeInfo entityTypeInfo) throws IOException {

        final String entityName = entityTypeInfo.getName();

        final boolean hasId = identifiableEntityNames.contains(entityTypeInfo.getName());
        final LinkedHashSet<String> parents = entityTypeInfo.getParents();

        try (FileOutputStream out = new FileOutputStream(path.resolve(entityName + ".java").toFile());
             OutputStreamWriter writer = new OutputStreamWriter(out, charset)) {

            writer.append("package ").append(schemaInfo.getPackageName()).append(";\n");
            writer.append("\n");
            if (hasId) {
                writer.append("import codes.writeonce.deltastore.api.IdentifiableEntity;\n");
            } else if (parents.isEmpty()) {
                writer.append("import codes.writeonce.deltastore.api.Entity;\n");
            }

            for (final SchemaInfo parentSchemaInfo : schemaInfo.getParentSchemaMap().values()) {
                for (final EntityTypeInfo eti : parentSchemaInfo.getTypeMap().values()) {
                    if (eti.getSchemaName().equals(parentSchemaInfo.getName())) {
                        writer.append("import ").append(parentSchemaInfo.getPackageName()).append('.')
                                .append(eti.getName()).append(";\n");
                    }
                }
            }

            writer.append("\n");
            writer.append("public interface ").append(entityName);

            if (!entityTypeInfo.isInstantiable()) {
                writer.append("<T extends ").append(entityName).append("<T>>");
            }

            writer.append(" extends ");

            final String type = entityTypeInfo.isInstantiable() ? entityTypeInfo.getName() : "T";

            boolean firstParent;

            if (hasId) {
                writer.append("IdentifiableEntity<").append(type).append(">");
                firstParent = false;
            } else if (parents.isEmpty()) {
                writer.append("Entity<").append(type).append(">");
                firstParent = false;
            } else {
                firstParent = true;
            }

            for (final String parent : parents) {
                if (firstParent) {
                    firstParent = false;
                } else {
                    writer.append(", ");
                }
                appendPackagePrefix(writer, schemaInfo, parent);
                writer.append(parent).append("<").append(type).append(">");
            }

            writer.append(" {\n");

            for (final FieldInfo fieldInfo : entityTypeInfo.getFieldMap().values()) {

                writer.append("\n");
                writer.append("    ").append(typeName(schemaInfo, fieldInfo, type, false)).append(" ")
                        .append(getterName(fieldInfo)).append("();\n");

                writer.append("\n");
                writer.append("    boolean ").append(changeTesterName(fieldInfo)).append("();\n");

                if (fieldInfo.isMutable()) {
                    writer.append("\n");
                    writer.append("    boolean ").append(testerName(fieldInfo)).append("();\n");
                    writer.append("\n");
                    writer.append("    void ").append(clearerName(fieldInfo)).append("();\n");
                    writer.append("\n");
                    writer.append("    void ").append(setterName(fieldInfo)).append("(")
                            .append(typeName(schemaInfo, fieldInfo, type, false)).append(" value);\n");
                }
            }

            writer.append("}\n");
        }
    }

    private Appendable appendPackagePrefix(Appendable writer, SchemaInfo schemaInfo, String typeName)
            throws IOException {
        final String schemaName = schemaInfo.getTypeMap().get(typeName).getSchemaName();
        if (!schemaName.equals(schemaInfo.getName())) {
            writer.append(schemaInfo.getParentSchemaMap().get(schemaName).getPackageName()).append('.');
        }
        return writer;
    }

    private void generateEntityTypes(SchemaInfo schemaInfo, Path path,
            Charset charset) throws IOException {

        for (final EntityTypeInfo entityTypeInfo : schemaInfo.getTypeMap().values()) {
            if (entityTypeInfo.isInstantiable()) {
                generateEntityType(schemaInfo, schemaInfo.getPackageName(), path, entityTypeInfo, charset);
            }
        }
    }

    private void generateEntityType(SchemaInfo schemaInfo, String packageName, Path path, EntityTypeInfo entityTypeInfo,
            Charset charset)
            throws IOException {

        try (FileOutputStream out = new FileOutputStream(
                path.resolve(entityTypeInfo.getName() + "EntityType.java").toFile());
             OutputStreamWriter writer = new OutputStreamWriter(out, charset)) {

            writer.append("package ").append(packageName).append(";\n");
            writer.append("\n");
            writer.append("import codes.writeonce.deltastore.api.*;\n");
            writer.append("\n");
            writer.append("import java.util.Arrays;\n");
            writer.append("import java.util.Collections;\n");
            writer.append("import java.util.List;\n");
            writer.append("import java.util.Map;\n");
            writer.append("import java.util.stream.Collectors;\n");
            writer.append("\n");
            writer.append("import static java.util.function.Function.identity;\n");

            appendImport(writer, schemaInfo, entityTypeInfo);

            for (final SchemaInfo parentSchemaInfo : schemaInfo.getParentSchemaMap().values()) {
                for (final EntityTypeInfo eti : parentSchemaInfo.getTypeMap().values()) {
                    if (eti.getSchemaName().equals(parentSchemaInfo.getName())) {
                        writer.append("import ").append(parentSchemaInfo.getPackageName()).append('.')
                                .append(eti.getName()).append(";\n");
                    }
                }
            }

            writer.append("\n");
            writer.append("public class ").append(entityTypeInfo.getName())
                    .append("EntityType extends AbstractEntityType<").append(entityTypeInfo.getName()).append("> {\n");

            writer.append("\n");
            indent(writer, 1).append("public static final EntityType<").append(entityTypeInfo.getName())
                    .append("> INSTANCE = new ").append(entityTypeInfo.getName()).append("EntityType();\n");

            appendFields(writer, schemaInfo, entityTypeInfo);

            appendKeys(writer, schemaInfo, entityTypeInfo);

            writer.append("\n");
            indent(writer, 1).append("public static EntityType<").append(entityTypeInfo.getName())
                    .append("> getInstance() {\n");
            indent(writer, 2).append("return INSTANCE;\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("private ").append(entityTypeInfo.getName()).append("EntityType() {\n");
            indent(writer, 2).append("super(\"").append(entityTypeInfo.getName())
                    .append("\", true, Collections.emptyList());\n");  // TODO: empty list of supertypes here, for now
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            indent(writer, 1).append("public List<Field<").append(entityTypeInfo.getName())
                    .append(", ?>> getFields() {\n");
            indent(writer, 2).append("return FIELDS;\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            indent(writer, 1).append("public Field<").append(entityTypeInfo.getName())
                    .append(", ?> getField(String name) {\n");
            indent(writer, 2).append("return FIELD_MAP.get(name);\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            indent(writer, 1).append("public Key<").append(entityTypeInfo.getName()).append("> getKey() {\n");
            indent(writer, 2).append("return KEY_")
                    .append(toUpperCase(getKey(schemaInfo, entityTypeInfo).getName())).append(";\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            indent(writer, 1).append("public List<Key<").append(entityTypeInfo.getName())
                    .append(">> getKeys() {\n");
            indent(writer, 2).append("return KEYS;\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            indent(writer, 1).append("public Key<").append(entityTypeInfo.getName())
                    .append("> getKey(String name) {\n");
            indent(writer, 2).append("return KEY_MAP.get(name);\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            indent(writer, 1).append("public Class<").append(entityTypeInfo.getName())
                    .append("> getEntityClass() {\n");
            indent(writer, 2).append("return ").append(entityTypeInfo.getName()).append(".class;\n");
            indent(writer, 1).append("}\n");

            writer.write("}\n");
        }
    }

    private void appendImport(Appendable writer, SchemaInfo schemaInfo, EntityTypeInfo entityTypeInfo)
            throws IOException {

        if (!entityTypeInfo.getSchemaName().equals(schemaInfo.getName())) {
            writer.append("\n");
            final String name = schemaInfo.getParentSchemaMap().get(entityTypeInfo.getSchemaName()).getPackageName();
            writer.append("import ").append(name).append('.').append(entityTypeInfo.getName()).append(";\n");
        }
    }

    private void appendFields(OutputStreamWriter writer, SchemaInfo schemaInfo, EntityTypeInfo entityTypeInfo)
            throws IOException {

        final List<FieldInfo> fields = getFields(schemaInfo, entityTypeInfo);

        for (final FieldInfo fieldInfo : fields) {
            writer.append("\n");
            indent(writer, 1).append("public static final ")
                    .append(fieldTypeDef(schemaInfo, entityTypeInfo, fieldInfo)).append(" FIELD_")
                    .append(toUpperCase(fieldInfo.getName())).append(" = new ");

            appendField(writer, schemaInfo, fieldInfo);
            indent(writer.append("\n"), 1).append(");\n");
        }

        writer.append("\n");
        indent(writer, 1).append("private static final List<Field<").append(entityTypeInfo.getName())
                .append(", ?>> FIELDS = Collections.unmodifiableList(Arrays.asList(");

        boolean first = true;

        for (final FieldInfo fieldInfo : fields) {
            if (first) {
                first = false;
            } else {
                writer.append(",");
            }
            indent(writer.append("\n"), 3).append("FIELD_").append(toUpperCase(fieldInfo.getName()));
        }

        if (!first) {
            indent(writer.append("\n"), 1);
        }

        writer.append("));\n");

        writer.append("\n");
        indent(writer, 1).append("private static final Map<String, Field<").append(entityTypeInfo.getName())
                .append(", ?>> FIELD_MAP =\n");
        indent(writer, 3).append("FIELDS.stream().collect(Collectors.toMap(Field::getName, identity()));\n");
    }

    @Nonnull
    private List<FieldInfo> getFields(SchemaInfo schemaInfo, EntityTypeInfo entityTypeInfo) {
        final List<FieldInfo> fields = new ArrayList<>();
        for (final EntityTypeInfo typeInfo : getTypes(schemaInfo, entityTypeInfo)) {
            fields.addAll(typeInfo.getFieldMap().values());
        }
        return fields;
    }

    private void appendKeys(OutputStreamWriter writer, SchemaInfo schemaInfo, EntityTypeInfo entityTypeInfo)
            throws IOException {

        final List<KeyInfo> keys = new ArrayList<>();
        for (final EntityTypeInfo typeInfo : getTypes(schemaInfo, entityTypeInfo)) {
            keys.addAll(typeInfo.getKeyMap().values());
        }

        for (final KeyInfo keyInfo : keys) {
            writer.append("\n");
            indent(writer, 1).append("public static final Key<").append(entityTypeInfo.getName()).append("> KEY_")
                    .append(toUpperCase(keyInfo.getName())).append(" = new Key<>(INSTANCE, \"")
                    .append(keyInfo.getName()).append("\", Collections.unmodifiableList(Arrays.asList(");

            boolean first = true;

            for (final String fieldName : keyInfo.getFields()) {
                if (first) {
                    first = false;
                } else {
                    writer.append(",");
                }
                indent(writer.append("\n"), 3).append(" FIELD_").append(toUpperCase(fieldName));
            }

            if (!first) {
                indent(writer.append("\n"), 1);
            }

            writer.append(")), ").append(String.valueOf(keyInfo.isUnique())).append(");\n");
        }

        writer.append("\n");
        indent(writer, 1).append("private static final List<Key<").append(entityTypeInfo.getName())
                .append(">> KEYS = Collections.unmodifiableList(Arrays.asList(");

        boolean first = true;

        for (final KeyInfo keyInfo : keys) {
            if (first) {
                first = false;
            } else {
                writer.append(",");
            }
            indent(writer.append("\n"), 3).append("KEY_").append(toUpperCase(keyInfo.getName()));
        }

        if (!first) {
            indent(writer.append("\n"), 1);
        }

        writer.append("));\n");

        writer.append("\n");
        indent(writer, 1).append("private static final Map<String, Key<").append(entityTypeInfo.getName())
                .append(">> KEY_MAP =\n");
        indent(writer, 3).append("KEYS.stream().collect(Collectors.toMap(Key::getName, identity()));\n");
    }

    private void appendField(Appendable writer, SchemaInfo schemaInfo, FieldInfo fieldInfo) throws IOException {

        writer.append(fieldConstructorDef(fieldInfo)).append("(\n");
        indent(writer, 3).append("INSTANCE");
        indent(writer.append(",\n"), 3).append("\"").append(fieldInfo.getName()).append("\"");
        indent(writer.append(",\n"), 3).append(String.valueOf(fieldInfo.isNullable()));
        indent(writer.append(",\n"), 3).append(String.valueOf(fieldInfo.isMutable()));

        fieldInfo.accept(new FieldInfo.Visitor<Void, IOException>() {
            @Override
            public Void visit(StringFieldInfo fieldInfo) throws IOException {
                appendObjectReferenceField(writer, schemaInfo, fieldInfo);
                return null;
            }

            @Override
            public Void visit(IntegerFieldInfo fieldInfo) throws IOException {
                appendPrimitiveField(writer, schemaInfo, fieldInfo);
                return null;
            }

            @Override
            public Void visit(LongFieldInfo fieldInfo) throws IOException {
                appendPrimitiveField(writer, schemaInfo, fieldInfo);
                return null;
            }

            @Override
            public Void visit(BooleanFieldInfo fieldInfo) throws IOException {
                appendPrimitiveField(writer, schemaInfo, fieldInfo);
                return null;
            }

            @Override
            public Void visit(InstantFieldInfo fieldInfo) throws IOException {
                appendObjectReferenceField(writer, schemaInfo, fieldInfo);
                return null;
            }

            @Override
            public Void visit(IdFieldInfo fieldInfo) throws IOException {
                indent(writer.append(",\n"), 3).append("codes.writeonce.deltastore.api.Id::of");
                appendObjectReferenceField(writer, schemaInfo, fieldInfo);
                return null;
            }

            @Override
            public Void visit(EnumFieldInfo fieldInfo) throws IOException {
                indent(writer.append(",\n"), 3).append(fieldInfo.getType()).append(".class");
                appendObjectReferenceField(writer, schemaInfo, fieldInfo);
                return null;
            }

            @Override
            public Void visit(BigDecimalFieldInfo fieldInfo) throws IOException {
                appendObjectReferenceField(writer, schemaInfo, fieldInfo);
                return null;
            }
        });
    }

    private void appendPrimitiveField(Appendable writer, SchemaInfo schemaInfo, FieldInfo fieldInfo)
            throws IOException {
        appendObjectReferenceField(writer, schemaInfo, fieldInfo);
        appendGetSetMethods(writer, schemaInfo, fieldInfo);
    }

    private void appendObjectReferenceField(Appendable writer, SchemaInfo schemaInfo, FieldInfo fieldInfo)
            throws IOException {
        indent(writer.append(",\n"), 3).append(getDefault(fieldInfo));
        appendGetSetMethods(writer, schemaInfo, fieldInfo);

        appendMethod(writer, schemaInfo, fieldInfo, changeTesterName(fieldInfo));

        if (fieldInfo.isMutable()) {
            appendMethod(writer, schemaInfo, fieldInfo, testerName(fieldInfo));
            appendMethod(writer, schemaInfo, fieldInfo, clearerName(fieldInfo));

            if (fieldInfo.isNullable()) {
                indent(writer.append(",\n"), 3).append("e -> e.").append(getterName(fieldInfo))
                        .append("() == null");
                indent(writer.append(",\n"), 3).append("e -> e.").append(setterName(fieldInfo))
                        .append("(null)");
            } else {
                indent(writer.append(",\n"), 3).append("e -> false");
                appendUnsupportedNoArgMethod(writer);
            }
        } else {
            indent(writer.append(",\n"), 3).append("e -> true");
            appendUnsupportedNoArgMethod(writer);

            if (fieldInfo.isNullable()) {
                indent(writer.append(",\n"), 3).append("e -> e.").append(getterName(fieldInfo))
                        .append("() == null");
                appendUnsupportedNoArgMethod(writer);
            } else {
                indent(writer.append(",\n"), 3).append("e -> false");
                appendUnsupportedNoArgMethod(writer);
            }
        }
    }

    private void appendGetSetMethods(Appendable writer, SchemaInfo schemaInfo, FieldInfo fieldInfo) throws IOException {

        appendMethod(writer, schemaInfo, fieldInfo, getterName(fieldInfo));

        if (fieldInfo.isMutable()) {
            appendMethod(writer, schemaInfo, fieldInfo, setterName(fieldInfo));
        } else {
            appendUnsupportedValueArgMethod(writer);
        }
    }

    private void appendMethod(Appendable writer, SchemaInfo schemaInfo, FieldInfo fieldInfo, String name)
            throws IOException {
        final EntityTypeInfo entityType = fieldInfo.getEntityType();
        final String entityTypeName = entityType.getName();
        appendPackagePrefix(indent(writer.append(",\n"), 3), schemaInfo, entityTypeName).append(entityTypeName)
                .append("::").append(name);
    }

    private void appendUnsupportedNoArgMethod(Appendable writer) throws IOException {
        indent(writer.append(",\n"), 3).append("e -> {\n");
        indent(writer, 4).append("throw new UnsupportedOperationException();\n");
        indent(writer, 3).append("}");
    }

    private void appendUnsupportedValueArgMethod(Appendable writer) throws IOException {
        indent(writer.append(",\n"), 3).append("(e, v) -> {\n");
        indent(writer, 4).append("throw new UnsupportedOperationException();\n");
        indent(writer, 3).append("}");
    }

    private String fieldTypeDef(SchemaInfo schemaInfo, EntityTypeInfo entityTypeInfo, FieldInfo fieldInfo) {
        return fieldInfo.accept(new FieldInfo.Visitor<String, RuntimeException>() {
            @Override
            public String visit(StringFieldInfo fieldInfo) {
                return "StringField<" + entityTypeInfo.getName() + ">";
            }

            @Override
            public String visit(IntegerFieldInfo fieldInfo) {
                return "IntegerField<" + entityTypeInfo.getName() + ">";
            }

            @Override
            public String visit(LongFieldInfo fieldInfo) {
                return "LongField<" + entityTypeInfo.getName() + ">";
            }

            @Override
            public String visit(BooleanFieldInfo fieldInfo) {
                return "BooleanField<" + entityTypeInfo.getName() + ">";
            }

            @Override
            public String visit(InstantFieldInfo fieldInfo) {
                return "InstantField<" + entityTypeInfo.getName() + ">";
            }

            @Override
            public String visit(IdFieldInfo fieldInfo) {
                final String type = fieldInfo.getType();
                return "IdField<" + entityTypeInfo.getName() + ", codes.writeonce.deltastore.api.Id<" +
                       selfIdType("this".equals(type) ? entityTypeInfo : schemaInfo.getTypeMap().get(type)) + ">>";
            }

            @Override
            public String visit(EnumFieldInfo fieldInfo) {
                return "EnumField<" + entityTypeInfo.getName() + ", " + fieldInfo.getType() + ">";
            }

            @Override
            public String visit(BigDecimalFieldInfo fieldInfo) {
                return "BigDecimalField<" + entityTypeInfo.getName() + ">";
            }
        });
    }

    private String fieldConstructorDef(FieldInfo fieldInfo) {
        return fieldInfo.accept(new FieldInfo.Visitor<String, RuntimeException>() {
            @Override
            public String visit(StringFieldInfo fieldInfo) {
                return "StringField<>";
            }

            @Override
            public String visit(IntegerFieldInfo fieldInfo) {
                return "IntegerField<>";
            }

            @Override
            public String visit(LongFieldInfo fieldInfo) {
                return "LongField<>";
            }

            @Override
            public String visit(BooleanFieldInfo fieldInfo) {
                return "BooleanField<>";
            }

            @Override
            public String visit(InstantFieldInfo fieldInfo) {
                return "InstantField<>";
            }

            @Override
            public String visit(IdFieldInfo fieldInfo) {
                return "IdField<>";
            }

            @Override
            public String visit(EnumFieldInfo fieldInfo) {
                return "EnumField<>";
            }

            @Override
            public String visit(BigDecimalFieldInfo fieldInfo) {
                return "BigDecimalField<>";
            }
        });
    }

    private void generateTableClasses(SchemaInfo schemaInfo, Path path, Charset charset)
            throws IOException {

        for (final EntityTypeInfo entityTypeInfo : schemaInfo.getTypeMap().values()) {
            if (entityTypeInfo.isInstantiable()) {
                generateTableClass(schemaInfo, schemaInfo.getPackageName(), path, entityTypeInfo, charset);
            }
        }
    }

    private void generateTableInterfaces(SchemaInfo schemaInfo, Path path, Charset charset)
            throws IOException {

        for (final EntityTypeInfo entityTypeInfo : schemaInfo.getTypeMap().values()) {
            if (entityTypeInfo.isInstantiable() && entityTypeInfo.getSchemaName().equals(schemaInfo.getName())) {
                generateTableInterface(schemaInfo, schemaInfo.getPackageName(), path, entityTypeInfo, charset);
            }
        }
    }

    private void generateTableClass(SchemaInfo schemaInfo, String packageName, Path path, EntityTypeInfo entityTypeInfo,
            Charset charset) throws IOException {

        final String tableName = entityTypeInfo.getName() + "TableImpl";
        final String recordName = entityTypeInfo.getName() + "Record";
        final String entityTypeName = entityTypeInfo.getName() + "EntityType";
        final String storeName = getStoreName(schemaInfo);
        final List<FieldInfo> fields = getConstructorFields(schemaInfo, entityTypeInfo);

        try (FileOutputStream out = new FileOutputStream(path.resolve(tableName + ".java").toFile());
             OutputStreamWriter writer = new OutputStreamWriter(out, charset)) {

            writer.append("package ").append(packageName).append(";\n");
            writer.append("\n");
            writer.append("import codes.writeonce.deltastore.api.AbstractTable;\n");
            writer.append("import codes.writeonce.deltastore.api.FieldValueSupplierVisitor;\n");
            writer.append("import codes.writeonce.deltastore.api.Index;\n");

            if (!entityTypeInfo.getSchemaName().equals(schemaInfo.getName())) {
                writer.append("\n");
                final String name =
                        schemaInfo.getParentSchemaMap().get(entityTypeInfo.getSchemaName()).getPackageName();
                writer.append("import ").append(name).append('.').append(entityTypeInfo.getName()).append(";\n");
                writer.append("import ").append(name).append('.').append(entityTypeInfo.getName()).append("Table;\n");
            }

            for (final SchemaInfo parentSchemaInfo : schemaInfo.getParentSchemaMap().values()) {
                for (final EntityTypeInfo eti : parentSchemaInfo.getTypeMap().values()) {
                    if (eti.getSchemaName().equals(parentSchemaInfo.getName())) {
                        writer.append("import ").append(parentSchemaInfo.getPackageName()).append('.')
                                .append(eti.getName()).append(";\n");
                    }
                }
            }

            writer.append("\n");
            writer.append("public class ").append(tableName).append(" extends AbstractTable<").append(storeName)
                    .append(", ").append(entityTypeInfo.getName()).append("> implements ")
                    .append(entityTypeInfo.getName()).append("Table {\n");

            writer.append("\n");
            indent(writer, 1).append("public ").append(tableName).append("(").append(storeName).append(" store) {\n");
            indent(writer, 2).append("super(store, ").append(entityTypeName).append(".INSTANCE);\n");
            indent(writer, 1).append("}\n");

            final KeyInfo key = getKey(schemaInfo, entityTypeInfo);

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            indent(writer, 1).append("public Index<?> getKeyIndex() {\n");

            indent(writer, 2).append("return store.").append(withSmallLetter(key.getName())).append("();\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            appendTypeName(indent(writer, 1).append("public "), entityTypeInfo).append(" create(");
            appendParamDefs(schemaInfo, writer, entityTypeInfo, fields);
            writer.append(") {\n");

            indent(writer, 2).append("return ").append(recordName).append(".create(\n");
            indent(writer, 4).append("store");

            for (final FieldInfo field : fields) {
                indent(writer.append(",\n"), 4).append(field.getName());
            }

            indent(writer.append("\n"), 2).append(");\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            appendTypeName(indent(writer, 1).append("public <X extends Throwable> "), entityTypeInfo)
                    .append(" create(FieldValueSupplierVisitor<X, ").append(entityTypeInfo.getName())
                    .append("> visitor) throws X {\n");

            indent(writer, 2).append("return ").append(recordName).append(".create(\n");
            indent(writer, 4).append("store");

            for (final FieldInfo field : fields) {
                indent(writer.append(",\n"), 4).append(chooseVisitor(field)).append(entityTypeInfo.getName())
                        .append("EntityType.FIELD_").append(toUpperCase(field.getName())).append(")");
            }

            indent(writer.append("\n"), 2).append(");\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            indent(writer, 1).append("@Override\n");
            appendTypeName(indent(writer, 1).append("public <X extends Throwable> "), entityTypeInfo)
                    .append(" get(FieldValueSupplierVisitor<X, ").append(entityTypeInfo.getName())
                    .append("> visitor) throws X {\n");

            indent(writer, 2).append("return (").append(entityTypeInfo.getName()).append(") store.")
                    .append(withSmallLetter(key.getName())).append("().get(");

            boolean first = true;

            for (final FieldInfo field : getKeyFields(schemaInfo, key)) {
                if (first) {
                    first = false;
                } else {
                    writer.append(",");
                }
                indent(writer.append("\n"), 4).append(chooseVisitor(field)).append(entityTypeInfo.getName())
                        .append("EntityType.FIELD_").append(toUpperCase(field.getName())).append(")");
            }

            indent(writer.append("\n"), 2).append(");\n");
            indent(writer, 1).append("}\n");

            writer.append("}\n");
        }
    }

    private void generateTableInterface(SchemaInfo schemaInfo, String packageName, Path path,
            EntityTypeInfo entityTypeInfo,
            Charset charset) throws IOException {

        final String tableName = entityTypeInfo.getName() + "Table";
        final List<FieldInfo> fields = getConstructorFields(schemaInfo, entityTypeInfo);

        try (FileOutputStream out = new FileOutputStream(path.resolve(tableName + ".java").toFile());
             OutputStreamWriter writer = new OutputStreamWriter(out, charset)) {

            writer.append("package ").append(packageName).append(";\n");
            writer.append("\n");
            writer.append("import codes.writeonce.deltastore.api.Table;\n");

            for (final SchemaInfo parentSchemaInfo : schemaInfo.getParentSchemaMap().values()) {
                for (final EntityTypeInfo eti : parentSchemaInfo.getTypeMap().values()) {
                    if (eti.getSchemaName().equals(parentSchemaInfo.getName())) {
                        writer.append("import ").append(parentSchemaInfo.getPackageName()).append('.')
                                .append(eti.getName()).append(";\n");
                    }
                }
            }

            writer.append("\n");
            writer.append("public interface ").append(tableName).append(" extends Table<")
                    .append(entityTypeInfo.getName()).append("> {\n");

            writer.append("\n");
            appendTypeName(indent(writer, 1).append("public "), entityTypeInfo).append(" create(");
            appendParamDefs(schemaInfo, writer, entityTypeInfo, fields);
            writer.append(");\n");

            writer.append("}\n");
        }
    }

    private String chooseVisitor(FieldInfo field) {
        return field.accept(new FieldInfo.Visitor<String, RuntimeException>() {
            @Override
            public String visit(StringFieldInfo fieldInfo) {
                return "visitor.visit(";
            }

            @Override
            public String visit(IntegerFieldInfo fieldInfo) {
                return fieldInfo.isNullable() ? "visitor.visit(" : "visitor.visitAsInt(";
            }

            @Override
            public String visit(LongFieldInfo fieldInfo) {
                return fieldInfo.isNullable() ? "visitor.visit(" : "visitor.visitAsLong(";
            }

            @Override
            public String visit(BooleanFieldInfo fieldInfo) {
                return fieldInfo.isNullable() ? "visitor.visit(" : "visitor.visitAsBoolean(";
            }

            @Override
            public String visit(InstantFieldInfo fieldInfo) {
                return "visitor.visit(";
            }

            @Override
            public String visit(IdFieldInfo fieldInfo) {
                return "visitor.visit(";
            }

            @Override
            public String visit(EnumFieldInfo fieldInfo) {
                return "visitor.visit(";
            }

            @Override
            public String visit(BigDecimalFieldInfo fieldInfo) {
                return "visitor.visit(";
            }
        });
    }

    private static <T extends Appendable> T appendTypeName(T appendable, EntityTypeInfo entityTypeInfo)
            throws IOException {
        appendable.append(entityTypeInfo.getName());
        if (!entityTypeInfo.isInstantiable()) {
            appendable.append("<?>");
        }
        return appendable;
    }

    private void appendParamDefs(
            SchemaInfo schemaInfo,
            Appendable writer,
            EntityTypeInfo entityTypeInfo,
            List<FieldInfo> fields
    ) throws IOException {

        boolean first = true;

        for (final FieldInfo field : fields) {
            if (first) {
                first = false;
            } else {
                writer.append(",");
            }
            indent(writer.append("\n"), 3);
            appendParamDef(writer, entityTypeInfo, field, schemaInfo);
        }

        if (!first) {
            indent(writer.append("\n"), 1);
        }
    }

    private void appendParamDefs2(
            SchemaInfo schemaInfo,
            Appendable writer,
            EntityTypeInfo entityTypeInfo,
            List<FieldInfo> fields
    ) throws IOException {

        indent(writer.append("\n"), 3);

        for (final FieldInfo field : fields) {
            appendParamDef(writer, entityTypeInfo, field, schemaInfo);
            indent(writer.append(",\n"), 3);
        }

        appendTypeName(writer.append("ArrayList<? super "), entityTypeInfo);

        indent(writer.append("> destinationList\n"), 1);
    }

    private void appendParamDef(
            Appendable writer,
            EntityTypeInfo entityTypeInfo,
            FieldInfo field,
            SchemaInfo schemaInfo
    ) throws IOException {
        writer.append(typeName(schemaInfo, field, selfIdType(entityTypeInfo), false)).append(" ")
                .append(field.getName());
    }

    @Nonnull
    private String selfIdType(EntityTypeInfo entityTypeInfo) {
        return entityTypeInfo.isInstantiable()
                ? entityTypeInfo.getName()
                : "? extends " + entityTypeInfo.getName() + "<?>";
    }

    private List<FieldInfo> getConstructorFields(SchemaInfo schemaInfo, EntityTypeInfo entityTypeInfo) {
        final List<FieldInfo> fields = new ArrayList<>();
        for (final EntityTypeInfo typeInfo : getTypes(schemaInfo, entityTypeInfo)) {
            for (final FieldInfo fieldInfo : typeInfo.getFieldMap().values()) {
                if (!fieldInfo.isMutable() || !hasValidDefaultValue(fieldInfo)) {
                    fields.add(fieldInfo);
                }
            }
        }
        return fields;
    }

    private List<FieldInfo> getKeyFields(SchemaInfo schemaInfo, KeyInfo keyInfo) {
        final List<FieldInfo> fields = new ArrayList<>();
        for (final String fieldName : keyInfo.getFields()) {
            fields.add(getField(schemaInfo, keyInfo.getEntityType(), fieldName));
        }
        return fields;
    }

    private boolean hasValidDefaultValue(FieldInfo fieldInfo) {
        return fieldInfo.isNullable();
    }

    private void generateKeyClasses(SchemaInfo schemaInfo, Path path, Charset charset) throws IOException {

        for (final KeyInfo keyInfo : schemaInfo.getKeyMap().values()) {
            if (keyInfo.getSchemaName().equals(schemaInfo.getName())) {
                generateKeyClass(schemaInfo, path, charset, keyInfo);
            }
        }
    }

    private void generateKeyClass(SchemaInfo schemaInfo, Path path, Charset charset, KeyInfo keyInfo)
            throws IOException {
        final EntityTypeInfo entityTypeInfo = keyInfo.getEntityType();

        try (FileOutputStream out = new FileOutputStream(path.resolve(keyInfo.getName() + ".java").toFile());
             OutputStreamWriter writer = new OutputStreamWriter(out, charset)) {

            writer.append("package ").append(schemaInfo.getPackageName()).append(";\n");
            writer.append("\n");
            writer.append("import codes.writeonce.deltastore.api.AbstractFilter;\n");
            writer.append("import codes.writeonce.deltastore.api.AbstractKey;\n");
            writer.append("import codes.writeonce.deltastore.api.map.LongTreeMap;\n");
            writer.append("import codes.writeonce.deltastore.api.map.IntegerTreeMap;\n");
            writer.append("import codes.writeonce.deltastore.api.map.IdTreeMap;\n");
            writer.append("import codes.writeonce.deltastore.api.map.InstantTreeMap;\n");
            writer.append("import codes.writeonce.deltastore.api.map.BigDecimalTreeMap;\n");
            writer.append("import codes.writeonce.deltastore.api.map.StringTreeMap;\n");
            writer.append("import codes.writeonce.deltastore.api.map.EnumTreeMap;\n");
            writer.append("import codes.writeonce.deltastore.api.map.BooleanTreeMap;\n");
            writer.append("import codes.writeonce.deltastore.api.NotUniqueException;\n");
            writer.append("import codes.writeonce.deltastore.api.SmartIterator;\n");
            writer.append("import codes.writeonce.deltastore.api.map.NestedIterator;\n");
            writer.append("\n");
            writer.append("import java.util.ArrayList;\n");
            writer.append("import java.util.Collections;\n");
            writer.append("import java.util.Comparator;\n");
            writer.append("import java.util.HashMap;\n");
            writer.append("import java.util.List;\n");
            writer.append("import java.util.Map;\n");
            writer.append("import java.util.Objects;\n");
            writer.append("import java.util.Spliterators;\n");
            writer.append("import java.util.stream.Collectors;\n");
            writer.append("import java.util.stream.Stream;\n");
            writer.append("import java.util.stream.StreamSupport;\n");
            writer.append("\n");
            writer.append("import static java.util.Spliterator.NONNULL;\n");
            writer.append("import static java.util.Spliterator.ORDERED;\n");
            writer.append("\n");

            for (final SchemaInfo parentSchemaInfo : schemaInfo.getParentSchemaMap().values()) {
                for (final EntityTypeInfo eti : parentSchemaInfo.getTypeMap().values()) {
                    if (eti.getSchemaName().equals(parentSchemaInfo.getName())) {
                        writer.append("import ").append(parentSchemaInfo.getPackageName()).append('.')
                                .append(eti.getName()).append(";\n");
                    }
                }
            }

            writer.append("\n");
            appendTypeName(writer.append("public class ").append(keyInfo.getName()).append(" extends AbstractKey<"),
                    entityTypeInfo).append("> {\n");

            final List<FieldInfo> keyFields = getKeyFields(schemaInfo, keyInfo);
            final Set<FieldInfo> allKeyFields = new HashSet<>(keyFields);

            final List<FieldInfo> idKeyFields =
                    getKeyFields(schemaInfo, getKey(schemaInfo, keyInfo.getEntityType()));

            final List<FieldInfo> uniqKeyFields =
                    idKeyFields.stream().filter(e -> !allKeyFields.contains(e)).collect(toList());

            if (!keyInfo.isUnique() && uniqKeyFields.isEmpty()) {
                throw new RuntimeException("Key " + keyInfo.getName() + " is effectively unique");
            }

            writer.append("\n");
            indent(writer, 1).append("private final ");

            appendMapType(writer, entityTypeInfo, keyInfo, keyFields, uniqKeyFields);

            indent(writer.append("\n"), 3).append("entityIndexMap = new ").append(treeMapPrefix(keyFields.get(0)))
                    .append("TreeMap<>();\n");

            if (keyInfo.isUnique()) {
                appendAddUnique(schemaInfo, writer, entityTypeInfo, keyFields, keyInfo);
                appendAddUniqueUnsafe(schemaInfo, writer, entityTypeInfo, keyFields);
                appendRemoveUnique(schemaInfo, writer, entityTypeInfo, keyFields);
                appendGet(schemaInfo, writer, entityTypeInfo, keyFields);
                appendIteratorAllUnique(writer, entityTypeInfo, keyFields);
            } else {
                appendAddNotUnique(schemaInfo, writer, entityTypeInfo, keyFields, uniqKeyFields);
                appendRemoveNotUnique(schemaInfo, writer, entityTypeInfo, keyFields, uniqKeyFields);
                appendIterator(schemaInfo, writer, entityTypeInfo, keyFields, uniqKeyFields);
                appendList(schemaInfo, writer, entityTypeInfo, keyFields);
                appendListReusable(schemaInfo, writer, entityTypeInfo, keyFields);
                appendStream(schemaInfo, writer, entityTypeInfo, keyFields);
                appendIteratorAllNotUnique(writer, entityTypeInfo, keyFields, uniqKeyFields);
            }

            writer.append("\n");
            indent(writer, 1).append("public Filter filter() {\n");
            indent(writer, 2).append("return new Filter();\n");
            indent(writer, 1).append("}\n");

            writer.append("\n");
            appendTypeName(indent(writer, 1).append("public final class Filter extends AbstractFilter<"),
                    entityTypeInfo).append("> {\n");

            for (final FieldInfo field : keyFields) {
                writer.append("\n");
                indent(writer, 2).append("private int ").append(field.getName()).append("Mode;\n");
                indent(writer, 2).append("private ")
                        .append(typeName(schemaInfo, field, selfIdType(entityTypeInfo), false)).append(" ")
                        .append(field.getName()).append(";\n");
                indent(writer, 2).append("private ")
                        .append(typeName(schemaInfo, field, selfIdType(entityTypeInfo), true)).append("[] ")
                        .append(field.getName()).append("List;\n");
                indent(writer, 2).append("private ")
                        .append(typeName(schemaInfo, field, selfIdType(entityTypeInfo), true)).append(" ")
                        .append(field.getName()).append("From;\n");
                indent(writer, 2).append("private boolean ").append(field.getName()).append("FromExclusive;\n");
                indent(writer, 2).append("private ")
                        .append(typeName(schemaInfo, field, selfIdType(entityTypeInfo), true)).append(" ")
                        .append(field.getName()).append("To;\n");
                indent(writer, 2).append("private boolean ").append(field.getName()).append("ToExclusive;\n");
                indent(writer, 2).append("private boolean ").append(field.getName()).append("Reversed;\n");
            }

            if (!keyInfo.isUnique()) {
                writer.append("\n");
                indent(writer, 2).append("private boolean reversed;\n");
            }

            writer.append("\n");
            indent(writer, 2).append("private Filter() {\n");
            indent(writer, 3).append("// empty\n");
            indent(writer, 2).append("}\n");

            for (final FieldInfo field : keyFields) {

                writer.append("\n");
                indent(writer, 2).append("public final Filter ").append(field.getName()).append("(")
                        .append(typeName(schemaInfo, field, selfIdType(entityTypeInfo), false)).append(" ")
                        .append(field.getName()).append(") {\n");
                indent(writer, 3).append(field.getName()).append("Mode(1);\n");
                indent(writer, 3).append("this.").append(field.getName()).append(" = ").append(field.getName())
                        .append(";\n");
                indent(writer, 3).append("return this;\n");
                indent(writer, 2).append("}\n");

                writer.append("\n");
                if (!isRefiable(field)) {
                    indent(writer, 2).append("@SafeVarargs\n");
                }
                indent(writer, 2).append("public final Filter ").append(field.getName()).append("(")
                        .append(typeName(schemaInfo, field, selfIdType(entityTypeInfo), true)).append("... ")
                        .append(field.getName()).append(") {\n");
                indent(writer, 3).append(field.getName()).append("Mode(2);\n");
                indent(writer, 3).append("if (").append(field.getName()).append(" == null) {\n");
                indent(writer, 4).append("throw new NullPointerException();\n");
                indent(writer, 3).append("}\n");
                indent(writer, 3).append("this.").append(field.getName()).append("List = ").append(field.getName())
                        .append(";\n");
                indent(writer, 3).append("return this;\n");
                indent(writer, 2).append("}\n");

                writer.append("\n");
                indent(writer, 2).append("public final Filter ").append(field.getName()).append("From(")
                        .append(typeName(schemaInfo, field, selfIdType(entityTypeInfo), false, true)).append(" ")
                        .append(field.getName()).append(") {\n");
                indent(writer, 3).append(field.getName()).append("Mode3();\n");
                if (!isPrimitive(field)) {
                    indent(writer, 3).append("if (").append(field.getName()).append(" == null) {\n");
                    indent(writer, 4).append("throw new NullPointerException();\n");
                    indent(writer, 3).append("}\n");
                }
                indent(writer, 3).append("if (this.").append(field.getName()).append("From != null) {\n");
                indent(writer, 4).append("throw new IllegalStateException();\n");
                indent(writer, 3).append("}\n");
                indent(writer, 3).append("this.").append(field.getName()).append("From = ").append(field.getName())
                        .append(";\n");
                indent(writer, 3).append("this.").append(field.getName()).append("FromExclusive = false;\n");
                indent(writer, 3).append("return this;\n");
                indent(writer, 2).append("}\n");

                writer.append("\n");
                indent(writer, 2).append("public final Filter ").append(field.getName()).append("After(")
                        .append(typeName(schemaInfo, field, selfIdType(entityTypeInfo), false, true)).append(" ")
                        .append(field.getName()).append(") {\n");
                indent(writer, 3).append(field.getName()).append("Mode3();\n");
                if (!isPrimitive(field)) {
                    indent(writer, 3).append("if (").append(field.getName()).append(" == null) {\n");
                    indent(writer, 4).append("throw new NullPointerException();\n");
                    indent(writer, 3).append("}\n");
                }
                indent(writer, 3).append("if (this.").append(field.getName()).append("From != null) {\n");
                indent(writer, 4).append("throw new IllegalStateException();\n");
                indent(writer, 3).append("}\n");
                indent(writer, 3).append("this.").append(field.getName()).append("From = ").append(field.getName())
                        .append(";\n");
                indent(writer, 3).append("this.").append(field.getName()).append("FromExclusive = true;\n");
                indent(writer, 3).append("return this;\n");
                indent(writer, 2).append("}\n");

                writer.append("\n");
                indent(writer, 2).append("public final Filter ").append(field.getName()).append("To(")
                        .append(typeName(schemaInfo, field, selfIdType(entityTypeInfo), false, true)).append(" ")
                        .append(field.getName()).append(") {\n");
                indent(writer, 3).append(field.getName()).append("Mode3();\n");
                if (!isPrimitive(field)) {
                    indent(writer, 3).append("if (").append(field.getName()).append(" == null) {\n");
                    indent(writer, 4).append("throw new NullPointerException();\n");
                    indent(writer, 3).append("}\n");
                }
                indent(writer, 3).append("if (this.").append(field.getName()).append("To != null) {\n");
                indent(writer, 4).append("throw new IllegalStateException();\n");
                indent(writer, 3).append("}\n");
                indent(writer, 3).append("this.").append(field.getName()).append("To = ").append(field.getName())
                        .append(";\n");
                indent(writer, 3).append("this.").append(field.getName()).append("ToExclusive = false;\n");
                indent(writer, 3).append("return this;\n");
                indent(writer, 2).append("}\n");

                writer.append("\n");
                indent(writer, 2).append("public final Filter ").append(field.getName()).append("Before(")
                        .append(typeName(schemaInfo, field, selfIdType(entityTypeInfo), false, true)).append(" ")
                        .append(field.getName()).append(") {\n");
                indent(writer, 3).append(field.getName()).append("Mode3();\n");
                if (!isPrimitive(field)) {
                    indent(writer, 3).append("if (").append(field.getName()).append(" == null) {\n");
                    indent(writer, 4).append("throw new NullPointerException();\n");
                    indent(writer, 3).append("}\n");
                }
                indent(writer, 3).append("if (this.").append(field.getName()).append("To != null) {\n");
                indent(writer, 4).append("throw new IllegalStateException();\n");
                indent(writer, 3).append("}\n");
                indent(writer, 3).append("this.").append(field.getName()).append("To = ").append(field.getName())
                        .append(";\n");
                indent(writer, 3).append("this.").append(field.getName()).append("ToExclusive = true;\n");
                indent(writer, 3).append("return this;\n");
                indent(writer, 2).append("}\n");

                writer.append("\n");
                indent(writer, 2).append("public final Filter ").append(field.getName()).append("Reversed() {\n");
                indent(writer, 3).append("if (this.").append(field.getName()).append("Mode != 0 && this.")
                        .append(field.getName()).append("Mode != 3) {\n");
                indent(writer, 4).append("throw new IllegalStateException();\n");
                indent(writer, 3).append("}\n");
                indent(writer, 3).append("this.").append(field.getName()).append("Reversed = true;\n");
                indent(writer, 3).append("return this;\n");
                indent(writer, 2).append("}\n");

                writer.append("\n");
                indent(writer, 2).append("private void ").append(field.getName()).append("Mode(int mode) {\n");
                indent(writer, 3).append("if (this.").append(field.getName()).append("Mode == 0 && !this.")
                        .append(field.getName()).append("Reversed) {\n");
                indent(writer, 4).append("this.").append(field.getName()).append("Mode = mode;\n");
                indent(writer, 3).append("} else {\n");
                indent(writer, 4).append("throw new IllegalStateException();\n");
                indent(writer, 3).append("}\n");
                indent(writer, 2).append("}\n");

                writer.append("\n");
                indent(writer, 2).append("private void ").append(field.getName()).append("Mode3() {\n");
                indent(writer, 3).append("if (this.").append(field.getName()).append("Mode == 0) {\n");
                indent(writer, 4).append("this.").append(field.getName()).append("Mode = 3;\n");
                indent(writer, 3).append("} else if (this.").append(field.getName()).append("Mode != 3) {\n");
                indent(writer, 4).append("throw new IllegalStateException();\n");
                indent(writer, 3).append("}\n");
                indent(writer, 2).append("}\n");
            }

            if (!keyInfo.isUnique()) {
                writer.append("\n");
                indent(writer, 2).append("public final Filter reversed() {\n");
                indent(writer, 3).append("this.reversed = true;\n");
                indent(writer, 3).append("return this;\n");
                indent(writer, 2).append("}\n");
            }

            appendFilterIterator(writer, entityTypeInfo, keyInfo, keyFields, uniqKeyFields);

            for (int i = 0; i < keyFields.size(); i++) {
                final FieldInfo field = keyFields.get(i);

                writer.append("\n");

                indent(writer, 2).append("private NestedIterator<");
                appendMapType(writer, entityTypeInfo, keyInfo, keyFields.subList(i, keyFields.size()), uniqKeyFields);
                writer.append(", ");
                appendTypeName(writer, entityTypeInfo);
                writer.append("> ").append(field.getName()).append("Query(");

                final boolean simple = keyInfo.isUnique() && i == keyFields.size() - 1;
                if (!simple) {
                    writer.append("NestedIterator<");
                    appendMapType(writer, entityTypeInfo, keyInfo, keyFields.subList(i + 1, keyFields.size()),
                            uniqKeyFields);
                    writer.append(", ");
                    appendTypeName(writer, entityTypeInfo);
                    writer.append("> iterator");
                }

                writer.append(") {\n");

                final String mapName = i == 0 ? "entityIndexMap.i" : treeMapPrefix(field) + "TreeMap.newI";

                indent(writer, 3).append("switch (this.").append(field.getName()).append("Mode) {\n");
                indent(writer, 4).append("case 0:\n");
                indent(writer, 5).append("return ").append(mapName).append("terator(");
                if (!simple) {
                    writer.append("iterator, ");
                }
                writer.append(field.getName()).append("Reversed);\n");
                indent(writer, 4).append("case 1:\n");
                indent(writer, 5).append("return ").append(mapName).append("teratorByKey(");
                if (!simple) {
                    writer.append("iterator, ");
                }
                writer.append(field.getName()).append(");\n");
                indent(writer, 4).append("case 2:\n");
                indent(writer, 5).append("return ").append(mapName).append("teratorByKeys(");
                if (!simple) {
                    writer.append("iterator, ");
                }
                writer.append(field.getName()).append("List);\n");
                indent(writer, 4).append("case 3:\n");
                indent(writer, 5).append("return ").append(mapName).append("teratorByRange(");
                if (!simple) {
                    writer.append("iterator, ");
                }
                writer.append(field.getName()).append("From, ")
                        .append(field.getName()).append("FromExclusive, ")
                        .append(field.getName()).append("To, ")
                        .append(field.getName()).append("ToExclusive, ")
                        .append(field.getName()).append("Reversed);\n");
                indent(writer, 4).append("default:\n");
                indent(writer, 5).append("throw new IllegalArgumentException();\n");
                indent(writer, 3).append("}\n");
                indent(writer, 2).append("}\n");
            }

            indent(writer, 1).append("}\n");

            writer.append("}\n");
        }
    }

    private void appendMapType(OutputStreamWriter writer, EntityTypeInfo entityTypeInfo, KeyInfo keyInfo,
            List<FieldInfo> keyFields, List<FieldInfo> uniqKeyFields)
            throws IOException {

        int c = 0;

        for (final FieldInfo field : keyFields) {
            writer.append(treeMapPrefix(field)).append("TreeMap<");
            c++;
        }

        if (!keyInfo.isUnique()) {
            for (final FieldInfo field : uniqKeyFields) {
                writer.append(treeMapPrefix(field)).append("TreeMap<");
                c++;
            }
        }

        appendTypeName(writer, entityTypeInfo);

        for (int i = 0; i < c; i++) {
            writer.append(">");
        }
    }

    @Nonnull
    private String treeMapPrefix(FieldInfo field) {
        return field.accept(new FieldInfo.Visitor<String, RuntimeException>() {
            @Override
            public String visit(StringFieldInfo fieldInfo) {
                return "String";
            }

            @Override
            public String visit(IntegerFieldInfo fieldInfo) {
                return "Integer";
            }

            @Override
            public String visit(LongFieldInfo fieldInfo) {
                return "Long";
            }

            @Override
            public String visit(BooleanFieldInfo fieldInfo) {
                return "Boolean";
            }

            @Override
            public String visit(InstantFieldInfo fieldInfo) {
                return "Instant";
            }

            @Override
            public String visit(IdFieldInfo fieldInfo) {
                return "Id";
            }

            @Override
            public String visit(EnumFieldInfo fieldInfo) {
                return "Enum";
            }

            @Override
            public String visit(BigDecimalFieldInfo fieldInfo) {
                return "BigDecimal";
            }
        });
    }

    private void appendAddUnique(
            SchemaInfo schemaInfo,
            Appendable writer,
            EntityTypeInfo entityTypeInfo,
            List<FieldInfo> keyFields,
            KeyInfo keyInfo
    ) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("public void add(\n");
        appendTypeName(indent(writer, 3), entityTypeInfo).append(" entity,");
        appendParamDefs(schemaInfo, writer, entityTypeInfo, keyFields);
        writer.append(") {\n");
        indent(writer, 2).append("with(\n");
        indent(writer, 4).append("entityIndexMap");

        final Iterator<FieldInfo> iterator = keyFields.iterator();
        FieldInfo fieldInfo = iterator.next();
        while (true) {
            if (iterator.hasNext()) {
                final FieldInfo fieldInfo2 = iterator.next();
                indent(writer.append("\n"), 6).append(".computeIfAbsent(").append(fieldInfo.getName())
                        .append(", k -> new ").append(treeMapPrefix(fieldInfo2)).append("TreeMap<>())");
                fieldInfo = fieldInfo2;
            } else {
                indent(writer.append(",\n"), 4).append("map -> {\n");
                appendTypeName(indent(writer, 5).append("final "), entityTypeInfo).append(" prev = map.get(")
                        .append(fieldInfo.getName()).append(");\n");
                writer.append("\n");
                indent(writer, 5).append("if (prev != entity) {\n");

                indent(writer, 6).append("if (prev != null");

                for (final FieldInfo field : keyFields) {
                    indent(writer.append(" &&\n"), 7).append("Objects.equals(prev.").append(getterName(field))
                            .append("(), ").append(field.getName()).append(")");
                }

                writer.append(") {\n");
                writer.append("\n");
                indent(writer, 7).append("throw new NotUniqueException(\"Duplicate key \\\"").append(keyInfo.getName())
                        .append("\\\" [")
                        .append(keyFields.stream().map(e -> e.getName() + "=\" + " + e.getName() + " + \"")
                                .collect(Collectors.joining(", ")))
                        .append("]\");\n");

                indent(writer, 6).append("}\n");
                indent(writer, 6).append("map.put(").append(fieldInfo.getName()).append(", entity);\n");

                indent(writer, 5).append("}\n");
                indent(writer, 4).append("}\n");
                break;
            }
        }

        indent(writer, 2).append(");\n");
        indent(writer, 1).append("}\n");
    }

    private void appendAddUniqueUnsafe(
            SchemaInfo schemaInfo,
            Appendable writer,
            EntityTypeInfo entityTypeInfo,
            List<FieldInfo> keyFields
    ) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("public void addUnsafe(\n");
        appendTypeName(indent(writer, 3), entityTypeInfo).append(" entity,");
        appendParamDefs(schemaInfo, writer, entityTypeInfo, keyFields);
        writer.append(") {\n");
        indent(writer, 2).append("with(\n");
        indent(writer, 4).append("entityIndexMap");

        final Iterator<FieldInfo> iterator = keyFields.iterator();
        FieldInfo fieldInfo = iterator.next();
        while (true) {
            if (iterator.hasNext()) {
                final FieldInfo fieldInfo2 = iterator.next();
                indent(writer.append("\n"), 6).append(".computeIfAbsent(").append(fieldInfo.getName())
                        .append(", k -> new ").append(treeMapPrefix(fieldInfo2)).append("TreeMap<>())");
                fieldInfo = fieldInfo2;
            } else {
                indent(writer.append(",\n"), 4).append("map -> map.put(").append(fieldInfo.getName())
                        .append(", entity)\n");
                break;
            }
        }

        indent(writer, 2).append(");\n");
        indent(writer, 1).append("}\n");
    }

    private void appendAddNotUnique(
            SchemaInfo schemaInfo,
            Appendable writer,
            EntityTypeInfo entityTypeInfo,
            List<FieldInfo> keyFields,
            List<FieldInfo> uniqKeyFields
    ) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("public void add(\n");
        appendTypeName(indent(writer, 3), entityTypeInfo).append(" entity,");
        appendParamDefs(schemaInfo, writer, entityTypeInfo, keyFields);
        writer.append(") {\n");
        indent(writer, 2).append("entityIndexMap");

        final Iterator<FieldInfo> keyIterator = keyFields.iterator();
        final Iterator<FieldInfo> iterator = uniqKeyFields.iterator();

        FieldInfo fieldInfo = keyIterator.next();
        while (true) {
            if (keyIterator.hasNext()) {
                final FieldInfo fieldInfo2 = keyIterator.next();
                indent(writer.append("\n"), 4).append(".computeIfAbsent(").append(fieldInfo.getName())
                        .append(", k -> new ").append(treeMapPrefix(fieldInfo2)).append("TreeMap<>())");
                fieldInfo = fieldInfo2;
            } else {
                final FieldInfo fieldInfo2 = iterator.next();
                indent(writer.append("\n"), 4).append(".computeIfAbsent(").append(fieldInfo.getName())
                        .append(", k -> new ").append(treeMapPrefix(fieldInfo2)).append("TreeMap<>())");
                fieldInfo = fieldInfo2;
                break;
            }
        }

        while (true) {
            if (iterator.hasNext()) {
                final FieldInfo fieldInfo2 = iterator.next();
                indent(writer.append("\n"), 4).append(".computeIfAbsent(entity.").append(getterName(fieldInfo))
                        .append("(), k -> new ").append(treeMapPrefix(fieldInfo2)).append("TreeMap<>())");
                fieldInfo = fieldInfo2;
            } else {
                indent(writer.append("\n"), 4).append(".put(entity.").append(getterName(fieldInfo))
                        .append("(), entity);\n");
                break;
            }
        }

        indent(writer, 1).append("}\n");
    }

    private void appendRemoveUnique(
            SchemaInfo schemaInfo,
            Appendable writer,
            EntityTypeInfo entityTypeInfo,
            List<FieldInfo> keyFields
    ) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("public void remove(\n");
        appendTypeName(indent(writer, 3), entityTypeInfo).append(" entity,");
        appendParamDefs(schemaInfo, writer, entityTypeInfo, keyFields);
        writer.append(") {\n");

        String mapName = "entityIndexMap";
        int ind = 0;

        final Iterator<FieldInfo> iterator = keyFields.iterator();
        while (iterator.hasNext()) {
            final FieldInfo fieldInfo = iterator.next();
            if (iterator.hasNext()) {
                ind++;
                final String nextMapName = "map" + ind;
                indent(writer, 1 + ind).append("remove(").append(mapName).append(", ")
                        .append(fieldInfo.getName()).append(", ").append(nextMapName).append(" -> {\n");
                mapName = nextMapName;
            } else {
                appendTypeName(indent(writer, 2 + ind).append("final "), entityTypeInfo).append(" prev = ")
                        .append(mapName).append(".get(").append(fieldInfo.getName()).append(");\n");
                indent(writer, 2 + ind).append("if (prev == entity) {\n");
                indent(writer, 3 + ind).append(mapName).append(".remove(").append(fieldInfo.getName())
                        .append(");\n");
                indent(writer, 2 + ind).append("}\n");
            }
        }

        while (ind > 0) {
            indent(writer, 1 + ind).append("});\n");
            ind--;
        }

        indent(writer, 1).append("}\n");
    }

    private void appendRemoveNotUnique(
            SchemaInfo schemaInfo,
            Appendable writer,
            EntityTypeInfo entityTypeInfo,
            List<FieldInfo> keyFields,
            List<FieldInfo> uniqKeyFields
    ) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("public void remove(\n");
        appendTypeName(indent(writer, 3), entityTypeInfo).append(" entity,");
        appendParamDefs(schemaInfo, writer, entityTypeInfo, keyFields);
        writer.append(") {\n");

        String mapName = "entityIndexMap";
        int ind = 0;

        for (FieldInfo fieldInfo : keyFields) {
            ind++;
            final String nextMapName = "map" + ind;
            indent(writer, 1 + ind).append("remove(").append(mapName).append(", ").append(fieldInfo.getName())
                    .append(", ").append(nextMapName).append(" -> {\n");
            mapName = nextMapName;
        }

        final Iterator<FieldInfo> iterator = uniqKeyFields.iterator();
        while (iterator.hasNext()) {
            final FieldInfo fieldInfo = iterator.next();
            if (iterator.hasNext()) {
                ind++;
                final String nextMapName = "map" + ind;
                indent(writer, 1 + ind).append("remove(").append(mapName).append(", entity.")
                        .append(getterName(fieldInfo)).append("(), ").append(nextMapName).append(" -> {\n");
                mapName = nextMapName;
            } else {
                appendTypeName(indent(writer, 2 + ind).append("final "), entityTypeInfo).append(" prev = ")
                        .append(mapName).append(".get(entity.").append(getterName(fieldInfo)).append("());\n");
                indent(writer, 2 + ind).append("if (prev == entity) {\n");
                indent(writer, 3 + ind).append(mapName).append(".remove(entity.").append(getterName(fieldInfo))
                        .append("());\n");
                indent(writer, 2 + ind).append("}\n");
            }
        }

        while (ind > 0) {
            indent(writer, 1 + ind).append("});\n");
            ind--;
        }

        indent(writer, 1).append("}\n");
    }

    private void appendGet(
            SchemaInfo schemaInfo,
            Appendable writer,
            EntityTypeInfo entityTypeInfo,
            List<FieldInfo> keyFields
    ) throws IOException {

        writer.append("\n");
        appendTypeName(indent(writer, 1).append("public "), entityTypeInfo).append(" get(");
        appendParamDefs(schemaInfo, writer, entityTypeInfo, keyFields);
        writer.append(") {\n");

        indent(writer, 2).append("return entityIndexMap");

        final Iterator<FieldInfo> iterator = keyFields.iterator();
        FieldInfo field = iterator.next();
        while (true) {
            if (iterator.hasNext()) {
                final FieldInfo field2 = iterator.next();
                indent(writer.append("\n"), 4).append(".getOrDefault(").append(field.getName())
                        .append(", ").append(treeMapPrefix(field2)).append("TreeMap.empty())");
                field = field2;
            } else {
                indent(writer.append("\n"), 4).append(".get(").append(field.getName()).append(")");
                break;
            }
        }

        writer.append(";\n");

        indent(writer, 1).append("}\n");
    }

    private void appendList(
            SchemaInfo schemaInfo,
            Appendable writer,
            EntityTypeInfo entityTypeInfo,
            List<FieldInfo> keyFields
    ) throws IOException {

        writer.append("\n");
        appendTypeName(indent(writer, 1).append("public List<"), entityTypeInfo).append("> list(");
        appendParamDefs(schemaInfo, writer, entityTypeInfo, keyFields);
        writer.append(") {\n");
        appendTypeName(indent(writer, 2).append("final List<"), entityTypeInfo).append("> list = new ArrayList<>();\n");
        indent(writer, 2).append("try (var iterator = iterator(");

        appendParamNames(writer, keyFields);

        writer.append(")) {\n");
        indent(writer, 3).append("while (true) {\n");
        indent(writer, 4).append("final var value = iterator.get();\n");
        indent(writer, 4).append("if (value == null) {\n");
        indent(writer, 5).append("break;\n");
        indent(writer, 4).append("}\n");
        indent(writer, 4).append("list.add(value);\n");
        indent(writer, 3).append("}\n");
        indent(writer, 2).append("}\n");
        indent(writer, 2).append("return list;\n");
        indent(writer, 1).append("}\n");
    }

    private void appendListReusable(
            SchemaInfo schemaInfo,
            Appendable writer,
            EntityTypeInfo entityTypeInfo,
            List<FieldInfo> keyFields
    ) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("public void list(");
        appendParamDefs2(schemaInfo, writer, entityTypeInfo, keyFields);
        writer.append(") {\n");
        indent(writer, 2).append("destinationList.clear();\n");
        indent(writer, 2).append("try (var iterator = iterator(");

        appendParamNames(writer, keyFields);

        writer.append(")) {\n");
        indent(writer, 3).append("while (true) {\n");
        indent(writer, 4).append("final var value = iterator.get();\n");
        indent(writer, 4).append("if (value == null) {\n");
        indent(writer, 5).append("break;\n");
        indent(writer, 4).append("}\n");
        indent(writer, 4).append("destinationList.add(value);\n");
        indent(writer, 3).append("}\n");
        indent(writer, 2).append("}\n");
        indent(writer, 1).append("}\n");
    }

    private void appendStream(
            SchemaInfo schemaInfo,
            Appendable writer,
            EntityTypeInfo entityTypeInfo,
            List<FieldInfo> keyFields
    ) throws IOException {

        writer.append("\n");
        appendTypeName(indent(writer, 1).append("public Stream<"), entityTypeInfo).append("> stream(");
        appendParamDefs(schemaInfo, writer, entityTypeInfo, keyFields);
        writer.append(") {\n");
        indent(writer, 2).append("final var iterator = iterator(");
        appendParamNames(writer, keyFields);
        writer.append(");\n");
        indent(writer, 2).append("return StreamSupport.stream(iterator, false).onClose(iterator::close);\n");
        indent(writer, 1).append("}\n");
    }

    private void appendParamNames(Appendable writer, List<FieldInfo> keyFields) throws IOException {

        boolean first = true;

        for (final FieldInfo field : keyFields) {
            if (first) {
                first = false;
            } else {
                writer.append(",");
            }
            indent(writer.append("\n"), 4).append(field.getName());
        }

        if (!first) {
            writer.append("\n");
            indent(writer, 2);
        }
    }

    private void appendIteratorAllUnique(
            Appendable writer,
            EntityTypeInfo entityTypeInfo,
            List<FieldInfo> keyFields
    ) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        appendTypeName(indent(writer, 1).append("public SmartIterator<"), entityTypeInfo)
                .append("> iterator() {\n");

        indent(writer, 2).append("return entityIndexMap.iterator(");

        final Iterator<FieldInfo> iterator = keyFields.iterator();
        iterator.next();

        if (iterator.hasNext()) {
            appendIteratorAll(writer, iterator, "false");
            writer.append(", false);\n");
        } else {
            writer.append("false);\n");
        }

        indent(writer, 1).append("}\n");
    }

    private void appendIteratorAllNotUnique(
            Appendable writer,
            EntityTypeInfo entityTypeInfo,
            List<FieldInfo> keyFields,
            List<FieldInfo> uniqKeyFields
    ) throws IOException {

        writer.append("\n");
        indent(writer, 1).append("@Override\n");
        appendTypeName(indent(writer, 1).append("public SmartIterator<"), entityTypeInfo)
                .append("> iterator() {\n");

        indent(writer, 2).append("return entityIndexMap.iterator(");

        final List<FieldInfo> fields = new ArrayList<>(keyFields.size() + uniqKeyFields.size());
        fields.addAll(keyFields);
        fields.addAll(uniqKeyFields);
        final Iterator<FieldInfo> iterator = fields.iterator();
        iterator.next();

        appendIteratorAll(writer, iterator, "false");
        writer.append(", false);\n");

        indent(writer, 1).append("}\n");
    }

    private void appendIterator(
            SchemaInfo schemaInfo,
            Appendable writer,
            EntityTypeInfo entityTypeInfo,
            List<FieldInfo> keyFields,
            List<FieldInfo> uniqKeyFields
    ) throws IOException {

        writer.append("\n");
        appendTypeName(indent(writer, 1).append("public SmartIterator<"), entityTypeInfo).append("> iterator(");
        appendParamDefs(schemaInfo, writer, entityTypeInfo, keyFields);
        writer.append(") {\n");

        indent(writer, 2).append("return entityIndexMap");

        final Iterator<FieldInfo> keyIterator = keyFields.iterator();
        final Iterator<FieldInfo> iterator = uniqKeyFields.iterator();

        FieldInfo field = keyIterator.next();
        while (true) {
            if (keyIterator.hasNext()) {
                final FieldInfo field2 = keyIterator.next();
                indent(writer.append("\n"), 4).append(".getOrDefault(").append(field.getName()).append(", ")
                        .append(treeMapPrefix(field2)).append("TreeMap.empty())");
                field = field2;
            } else {
                final FieldInfo field2 = iterator.next();
                indent(writer.append("\n"), 4).append(".getOrDefault(").append(field.getName()).append(", ")
                        .append(treeMapPrefix(field2)).append("TreeMap.empty())");
                // field = field2;
                break;
            }
        }

        indent(writer.append("\n"), 4).append(".iterator(");

        if (iterator.hasNext()) {
            appendIteratorAll(writer, iterator, "false");
            writer.append(", false);\n");
        } else {
            writer.append("false);\n");
        }

        indent(writer, 1).append("}\n");
    }

    private void appendFilterIterator(
            Appendable writer,
            EntityTypeInfo entityTypeInfo,
            KeyInfo keyInfo,
            List<FieldInfo> keyFields,
            List<FieldInfo> uniqKeyFields
    ) throws IOException {

        writer.append("\n");
        appendTypeName(indent(writer, 2).append("public final SmartIterator<"), entityTypeInfo)
                .append("> iterator() {\n");

        indent(writer, 3).append("return ");

        appendIteratorQuery(writer, keyFields.size(), keyFields.iterator(), () -> {
            if (!keyInfo.isUnique()) {
                appendIteratorAll(writer, uniqKeyFields.iterator(), "reversed");
            }
        });

        writer.append(";\n");

        indent(writer, 2).append("}\n");
    }

    private void appendIteratorQuery(Appendable writer, int count, Iterator<FieldInfo> iterator, Callback runnable)
            throws IOException {

        writer.append(iterator.next().getName()).append("Query(");

        count--;

        if (count > 0) {
            appendIteratorQuery(writer, count, iterator, runnable);
        } else {
            runnable.run();
        }

        writer.append(")");
    }

    private void appendIteratorAll(Appendable writer, Iterator<FieldInfo> iterator, String reversed)
            throws IOException {

        if (iterator.hasNext()) {
            writer.append(treeMapPrefix(iterator.next())).append("TreeMap.newIterator(");
            if (iterator.hasNext()) {
                appendIteratorAll(writer, iterator, reversed);
                writer.append(", ");
            }
            writer.append(reversed).append(")");
        } else {
            writer.append(reversed);
        }
    }

    private KeyInfo getKey(SchemaInfo schemaInfo, EntityTypeInfo entityTypeInfo) {
        final String keyName =
                getTypes(schemaInfo, entityTypeInfo).stream().map(EntityTypeInfo::getKey).filter(Objects::nonNull)
                        .findAny().orElseThrow(RuntimeException::new);
        final KeyInfo keyInfo = schemaInfo.getKeyMap().get(keyName);
        if (keyInfo == null) {
            throw new IllegalArgumentException("Key " + keyName + " not found");
        }
        return keyInfo;
    }

    private static <T extends Appendable> T indent(T appendable, int n) throws IOException {
        for (int i = 0; i < n; i++) {
            appendable.append("    ");
        }
        return appendable;
    }

    private static FieldInfo getField(SchemaInfo schemaInfo, EntityTypeInfo entityTypeInfo, String fieldName) {
        for (final EntityTypeInfo typeInfo : getTypes(schemaInfo, entityTypeInfo)) {
            final FieldInfo fieldInfo = typeInfo.getFieldMap().get(fieldName);
            if (fieldInfo != null) {
                return fieldInfo;
            }
        }
        throw new IllegalArgumentException(
                "Failed to find the field " + fieldName + " for entity type " + entityTypeInfo.getName());
    }

    private static LinkedHashSet<EntityTypeInfo> getTypes(SchemaInfo schemaInfo, EntityTypeInfo entityTypeInfo) {
        final LinkedHashSet<EntityTypeInfo> types = new LinkedHashSet<>();
        for (final String parentTypeName : entityTypeInfo.getParents()) {
            final EntityTypeInfo parentType = schemaInfo.getTypeMap().get(parentTypeName);
            types.addAll(getTypes(schemaInfo, parentType));
        }
        types.add(entityTypeInfo);
        return types;
    }

    private static String setterName(FieldInfo fieldInfo) {
        return "set" + withCapitalLetter(fieldInfo.getName());
    }

    private static String testerName(FieldInfo fieldInfo) {
        return "has" + withCapitalLetter(fieldInfo.getName());
    }

    private static String changeTesterName(FieldInfo fieldInfo) {
        return "isChanged" + withCapitalLetter(fieldInfo.getName());
    }

    private static String clearerName(FieldInfo fieldInfo) {
        return "clear" + withCapitalLetter(fieldInfo.getName());
    }

    private static String typeName(SchemaInfo schemaInfo, FieldInfo fieldInfo, String self, boolean forCollection) {
        return typeName(schemaInfo, fieldInfo, self, forCollection, false);
    }

    private static String typeName(SchemaInfo schemaInfo, FieldInfo fieldInfo, String self, boolean forCollection,
            boolean suppressNullability) {
        return fieldInfo.accept(new FieldInfo.Visitor<String, RuntimeException>() {
            @Override
            public String visit(StringFieldInfo fieldInfo) {
                return "String";
            }

            @Override
            public String visit(IntegerFieldInfo fieldInfo) {
                return !suppressNullability && fieldInfo.isNullable() || forCollection ? "Integer" : "int";
            }

            @Override
            public String visit(LongFieldInfo fieldInfo) {
                return !suppressNullability && fieldInfo.isNullable() || forCollection ? "Long" : "long";
            }

            @Override
            public String visit(BooleanFieldInfo fieldInfo) {
                return !suppressNullability && fieldInfo.isNullable() || forCollection ? "Boolean" : "boolean";
            }

            @Override
            public String visit(InstantFieldInfo fieldInfo) {
                return "java.time.Instant";
            }

            @Override
            public String visit(IdFieldInfo fieldInfo) {
                if (fieldInfo.getType().equals("this")) {
                    return "codes.writeonce.deltastore.api.Id<" + self + ">";
                } else {
                    final EntityTypeInfo typeInfo = schemaInfo.getTypeMap().get(fieldInfo.getType());
                    if (typeInfo == null) {
                        throw new NullPointerException("Type " + fieldInfo.getType() + " not found");
                    }
                    if (typeInfo.isInstantiable()) {
                        return "codes.writeonce.deltastore.api.Id<" + fieldInfo.getType() + ">";
                    } else {
                        return "codes.writeonce.deltastore.api.Id<" + ("? extends " + fieldInfo.getType() + "<?>") +
                               ">";
                    }
                }
            }

            @Override
            public String visit(EnumFieldInfo fieldInfo) {
                return fieldInfo.getType();
            }

            @Override
            public String visit(BigDecimalFieldInfo fieldInfo) {
                return "java.math.BigDecimal";
            }
        });
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean isPrimitive(FieldInfo fieldInfo) {
        return fieldInfo.accept(new FieldInfo.Visitor<Boolean, RuntimeException>() {
            @Override
            public Boolean visit(StringFieldInfo fieldInfo) {
                return false;
            }

            @Override
            public Boolean visit(IntegerFieldInfo fieldInfo) {
                return true;
            }

            @Override
            public Boolean visit(LongFieldInfo fieldInfo) {
                return true;
            }

            @Override
            public Boolean visit(BooleanFieldInfo fieldInfo) {
                return true;
            }

            @Override
            public Boolean visit(InstantFieldInfo fieldInfo) {
                return false;
            }

            @Override
            public Boolean visit(IdFieldInfo fieldInfo) {
                return false;
            }

            @Override
            public Boolean visit(EnumFieldInfo fieldInfo) {
                return false;
            }

            @Override
            public Boolean visit(BigDecimalFieldInfo fieldInfo) {
                return false;
            }
        });
    }

    private static boolean isRefiable(FieldInfo fieldInfo) {
        return fieldInfo.accept(new FieldInfo.Visitor<Boolean, RuntimeException>() {
            @Override
            public Boolean visit(StringFieldInfo fieldInfo) {
                return true;
            }

            @Override
            public Boolean visit(IntegerFieldInfo fieldInfo) {
                return true;
            }

            @Override
            public Boolean visit(LongFieldInfo fieldInfo) {
                return true;
            }

            @Override
            public Boolean visit(BooleanFieldInfo fieldInfo) {
                return true;
            }

            @Override
            public Boolean visit(InstantFieldInfo fieldInfo) {
                return true;
            }

            @Override
            public Boolean visit(IdFieldInfo fieldInfo) {
                return false;
            }

            @Override
            public Boolean visit(EnumFieldInfo fieldInfo) {
                return true;
            }

            @Override
            public Boolean visit(BigDecimalFieldInfo fieldInfo) {
                return true;
            }
        });
    }

    private static String getterName(FieldInfo fieldInfo) {
        return fieldInfo.accept(new FieldInfo.Visitor<String, RuntimeException>() {
            @Override
            public String visit(StringFieldInfo fieldInfo) {
                return usualGetterName(fieldInfo);
            }

            @Override
            public String visit(IntegerFieldInfo fieldInfo) {
                return usualGetterName(fieldInfo);
            }

            @Override
            public String visit(LongFieldInfo fieldInfo) {
                return usualGetterName(fieldInfo);
            }

            @Override
            public String visit(BooleanFieldInfo fieldInfo) {
                if (fieldInfo.isNullable()) {
                    return usualGetterName(fieldInfo);
                } else {
                    return booleanGetterName(fieldInfo);
                }
            }

            @Override
            public String visit(InstantFieldInfo fieldInfo) {
                return usualGetterName(fieldInfo);
            }

            @Override
            public String visit(IdFieldInfo fieldInfo) {
                return usualGetterName(fieldInfo);
            }

            @Override
            public String visit(EnumFieldInfo fieldInfo) {
                return usualGetterName(fieldInfo);
            }

            @Override
            public String visit(BigDecimalFieldInfo fieldInfo) {
                return usualGetterName(fieldInfo);
            }
        });
    }

    @Nonnull
    private static String usualGetterName(@Nonnull FieldInfo fieldInfo) {
        return "get" + withCapitalLetter(fieldInfo.getName());
    }

    @Nonnull
    private static String booleanGetterName(@Nonnull FieldInfo fieldInfo) {
        return "is" + withCapitalLetter(fieldInfo.getName());
    }

    @Nonnull
    private static String withCapitalLetter(@Nonnull String value) {
        return Character.toUpperCase(value.charAt(0)) + value.substring(1);
    }

    @Nonnull
    private static String withSmallLetter(@Nonnull String value) {
        return Character.toLowerCase(value.charAt(0)) + value.substring(1);
    }

    @Nonnull
    private static String toUpperCase(@Nonnull String value) {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            if (i > 0 &&
                i + 1 < value.length() &&
                builder.charAt(builder.length() - 1) != '_' &&
                Character.isUpperCase(value.charAt(i)) &&
                Character.isLowerCase(value.charAt(i + 1))) {

                builder.append('_');
            }
            builder.append(Character.toUpperCase(value.charAt(i)));
            if (i + 1 < value.length() &&
                Character.isLowerCase(value.charAt(i)) &&
                Character.isUpperCase(value.charAt(i + 1))) {

                builder.append('_');
            }
        }
        return builder.toString();
    }

    private static class IdentifiableEntityVisitor implements FieldInfo.Visitor<String, RuntimeException> {

        private static final IdentifiableEntityVisitor INSTANCE = new IdentifiableEntityVisitor();

        @Override
        public String visit(StringFieldInfo fieldInfo) {
            return null;
        }

        @Override
        public String visit(IntegerFieldInfo fieldInfo) {
            return null;
        }

        @Override
        public String visit(LongFieldInfo fieldInfo) {
            return null;
        }

        @Override
        public String visit(BooleanFieldInfo fieldInfo) {
            return null;
        }

        @Override
        public String visit(InstantFieldInfo fieldInfo) {
            return null;
        }

        @Override
        public String visit(IdFieldInfo fieldInfo) {
            final String type = fieldInfo.getType();
            if ("this".equals(type)) {
                return fieldInfo.getEntityType().getName();
            } else {
                return type;
            }
        }

        @Override
        public String visit(EnumFieldInfo fieldInfo) {
            return null;
        }

        @Override
        public String visit(BigDecimalFieldInfo fieldInfo) {
            return null;
        }
    }

    private interface Callback {

        void run() throws IOException;
    }
}
