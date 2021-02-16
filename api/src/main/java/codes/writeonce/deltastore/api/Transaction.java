package codes.writeonce.deltastore.api;

public interface Transaction extends AutoCloseable {

    @Override
    void close();

    void rollback();

    void commit();
}
