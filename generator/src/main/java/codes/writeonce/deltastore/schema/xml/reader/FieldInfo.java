package codes.writeonce.deltastore.schema.xml.reader;

public abstract class FieldInfo {

    private final String name;
    private final boolean mutable;
    private final boolean nullable;
    private final EntityTypeInfo entityType;

    public FieldInfo(String name, boolean mutable, boolean nullable, EntityTypeInfo entityType) {
        this.name = name;
        this.mutable = mutable;
        this.nullable = nullable;
        this.entityType = entityType;
    }

    public String getName() {
        return name;
    }

    public boolean isMutable() {
        return mutable;
    }

    public boolean isNullable() {
        return nullable;
    }

    public EntityTypeInfo getEntityType() {
        return entityType;
    }

    public abstract <U, X extends Throwable> U accept(Visitor<U, X> visitor) throws X;

    public interface Visitor<U, X extends Throwable> {

        U visit(StringFieldInfo fieldInfo) throws X;

        U visit(IntegerFieldInfo fieldInfo) throws X;

        U visit(LongFieldInfo fieldInfo) throws X;

        U visit(BooleanFieldInfo fieldInfo) throws X;

        U visit(InstantFieldInfo fieldInfo) throws X;

        U visit(IdFieldInfo fieldInfo) throws X;

        U visit(EnumFieldInfo fieldInfo) throws X;

        U visit(BigDecimalFieldInfo fieldInfo) throws X;
    }
}
