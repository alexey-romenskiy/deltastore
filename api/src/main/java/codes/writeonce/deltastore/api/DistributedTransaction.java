package codes.writeonce.deltastore.api;

public interface DistributedTransaction extends Transaction {

    void prepareCommit();

    void finalCommit();
}
