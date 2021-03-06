package codes.writeonce.deltastore.schema.xml.reader;

public class IdFieldInfo extends FieldInfo {

    private final String type;

    public IdFieldInfo(String name, boolean mutable, boolean nullable, EntityTypeInfo entityType, String type) {
        super(name, mutable, nullable, entityType);
        this.type = type;
    }

    @Override
    public <U, X extends Throwable> U accept(Visitor<U, X> visitor) throws X {
        return visitor.visit(this);
    }

    public String getType() {
        return type;
    }
}
