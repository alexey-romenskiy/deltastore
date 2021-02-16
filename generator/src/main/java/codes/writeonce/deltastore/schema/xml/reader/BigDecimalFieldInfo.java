package codes.writeonce.deltastore.schema.xml.reader;

public class BigDecimalFieldInfo extends FieldInfo {

    public BigDecimalFieldInfo(String name, boolean mutable, boolean nullable, EntityTypeInfo entityType) {
        super(name, mutable, nullable, entityType);
    }

    @Override
    public <U, X extends Throwable> U accept(Visitor<U, X> visitor) throws X {
        return visitor.visit(this);
    }
}
