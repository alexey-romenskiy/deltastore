package codes.writeonce.deltastore.api;

import bak.pcj.hash.LongHashFunction;
import bak.pcj.map.LongKeyOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Serial;
import java.io.Serializable;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@SuppressWarnings("rawtypes")
public final class Id<E extends IdentifiableEntity<?>> implements Comparable<Id<?>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Id.class);

    private static final LongHashFunction HASH_FUNCTION = new HashFunction();

    private static final int INTERNER_SHIFT = Integer.getInteger("codes.writeonce.deltastore.api.Id.table.size", 23);

    private static final int INTERNER_SIZE = 1 << INTERNER_SHIFT;

    private static final int INTERNER_MASK = INTERNER_SIZE - 1;

    private static final AtomicReference[] INTERNER =
            Stream.generate(AtomicReference::new).limit(INTERNER_SIZE).toArray(AtomicReference[]::new);

    private static final int RING_BUFFER_SHIFT =
            Integer.getInteger("codes.writeonce.deltastore.api.Id.ringBuffer.size", 18);

    private static final int RING_BUFFER_SIZE = 1 << RING_BUFFER_SHIFT;

    private static final int RING_BUFFER_MASK = RING_BUFFER_SIZE - 1;

    private static final Id[] RING_BUFFER1 = new Id[RING_BUFFER_SIZE];

    private static final IdRef[] RING_BUFFER2 = new IdRef[RING_BUFFER_SIZE];

    private static final int UNPARK_WATERMARK_SHIFT =
            Integer.getInteger("codes.writeonce.deltastore.api.Id.ringBuffer.watermark", 12);

    private static final int UNPARK_WATERMARK_MASK = (1 << UNPARK_WATERMARK_SHIFT) - 1;

    private static final AtomicInteger LAST_READ_ENTRY = new AtomicInteger();

    private static volatile int firstEmptyEntry;

    private static final AtomicInteger NEXT_READ_ENTRY = new AtomicInteger();

    private static final int POOL_SHIFT =
            Integer.getInteger("codes.writeonce.deltastore.api.Id.pool.size", 14);

    private static final int POOL_SIZE = 1 << POOL_SHIFT;

    private static final int POOL_MASK = POOL_SIZE - 1;

    private static final LongKeyOpenHashMap[] POOL = new LongKeyOpenHashMap[POOL_SIZE];

    private static final AtomicInteger LAST_READ_POOL_ENTRY = new AtomicInteger();

    private static volatile int firstEmptyPoolEntry;

    private static final AtomicInteger NEXT_READ_POOL_ENTRY = new AtomicInteger();

    private static final Id[] FIXED = LongStream.range(0, 100).mapToObj(Id::new).toArray(Id[]::new);

    private static final ReferenceQueue<Id> REFERENCE_QUEUE = new ReferenceQueue<>();

    private static final Thread COLLECTOR_THREAD = new Thread(
            () -> {
                try {
                    while (true) {
                        remove((IdRef) REFERENCE_QUEUE.remove());
                    }
                } catch (Exception e) {
                    LOGGER.error("Id collector thread interrupted", e);
                }
            },
            "id-collector"
    );

    private static final Thread PREALLOCATOR_THREAD = new Thread(
            () -> {
                try {
                    int last = firstEmptyEntry;
                    while (true) {
                        final int next = last + 1 & RING_BUFFER_MASK;
                        while (next == LAST_READ_ENTRY.get()) {
                            LockSupport.park();
                        }
                        final Id id = new Id();
                        RING_BUFFER1[last] = id;
                        RING_BUFFER2[last] = new IdRef(id, REFERENCE_QUEUE);
                        firstEmptyEntry = next;
                        last = next;
                    }
                } catch (Exception e) {
                    LOGGER.error("Id preallocator thread interrupted", e);
                }
            },
            "id-preallocator"
    );

    static {
        LOGGER.info("Id table size: {}", INTERNER_SHIFT);
        LOGGER.info("Id ring buffer size: {}", RING_BUFFER_SHIFT);
        LOGGER.info("Id ring buffer watermark: {}", UNPARK_WATERMARK_SHIFT);

        for (int i = 0; i < RING_BUFFER_MASK; i++) {
            final Id id = new Id();
            RING_BUFFER1[i] = id;
            RING_BUFFER2[i] = new IdRef(id, REFERENCE_QUEUE);
        }

        firstEmptyEntry = RING_BUFFER_MASK;

        COLLECTOR_THREAD.setDaemon(true);
        COLLECTOR_THREAD.start();
        PREALLOCATOR_THREAD.setDaemon(true);
        PREALLOCATOR_THREAD.start();

        LOGGER.info("Id pooling initialized");
    }

    private long value;

    private int hash;

    private String stringValue;

    private Id(long value) {
        this(value, hash(value));
    }

    private Id(long value, int hash) {
        this.value = value;
        this.hash = hash;
    }

    private Id() {
        // empty
    }

    private void init(long value, int hash) {
        this.value = value;
        this.hash = hash;
    }

    public final long value() {
        return value;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public final <E2 extends E> Id<E2> downcast() {
        return (Id<E2>) this;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public final <E2 extends IdentifiableEntity<?>> Id<E2> recast() {
        return (Id<E2>) this;
    }

    @Override
    public final boolean equals(Object o) {

        if (o instanceof Id<?>) {
            final Id<?> id = (Id<?>) o;
            return value == id.value;
        } else {
            return false;
        }
    }

    @Override
    public final int hashCode() {
        return hash;
    }

    @Nonnull
    @Override
    public final String toString() {
        if (stringValue == null) {
            stringValue = String.valueOf(value);
        }
        return stringValue;
    }

    @Override
    @SuppressWarnings("UseCompareMethod")
    public final int compareTo(@Nonnull Id<?> other) {
        final long a = value;
        final long b = other.value;
        return (a < b) ? -1 : ((a == b) ? 0 : 1);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "SynchronizationOnLocalVariableOrMethodParameter"})
    public static <E extends IdentifiableEntity<?>> Id<E> of(long value) {

        if (value < FIXED.length) {
            return FIXED[(int) value];
        }

        Id newId = null;
        IdRef newIdRef = null;

        final int hash = hash(value);
        final AtomicReference atomic = INTERNER[hash & INTERNER_MASK];

        Object o = atomic.get();

        while (true) {

            if (o == null) {

                if (newId == null) {
                    final int sequence = borrow();
                    if (sequence < 0) {
                        newId = new Id(value, hash);
                        newIdRef = new IdRef(newId, REFERENCE_QUEUE);
                    } else {
                        newId = RING_BUFFER1[sequence];
                        RING_BUFFER1[sequence] = null;
                        newIdRef = RING_BUFFER2[sequence];
                        RING_BUFFER2[sequence] = null;
                        reclaim(sequence);
                        newId.init(value, hash);
                        newIdRef.value = value;
                    }
                }

                o = atomic.compareAndExchange(null, newIdRef);

                if (o == null) {
                    return newId;
                }
            } else if (o instanceof IdRef) {
                final IdRef idRef = (IdRef) o;
                final Id id = idRef.get();

                if (id == null) {

                    if (newId == null) {
                        final int sequence = borrow();
                        if (sequence < 0) {
                            newId = new Id(value, hash);
                            newIdRef = new IdRef(newId, REFERENCE_QUEUE);
                        } else {
                            newId = RING_BUFFER1[sequence];
                            RING_BUFFER1[sequence] = null;
                            newIdRef = RING_BUFFER2[sequence];
                            RING_BUFFER2[sequence] = null;
                            reclaim(sequence);
                            newId.init(value, hash);
                            newIdRef.value = value;
                        }
                    }

                    final Object o2 = atomic.compareAndExchange(o, newIdRef);

                    if (o2 == o) {
                        return newId;
                    }

                    o = o2;
                } else {
                    final long otherValue = id.value;

                    if (otherValue == value) {
                        return id;
                    }

                    final LongKeyOpenHashMap map = borrowMap();
                    final Object o2;

                    synchronized (map) {
                        o2 = atomic.compareAndExchange(o, map);
                        if (o2 == o) {
                            map.put(otherValue, idRef);

                            if (newId == null) {
                                final int sequence = borrow();
                                if (sequence < 0) {
                                    newId = new Id(value, hash);
                                    newIdRef = new IdRef(newId, REFERENCE_QUEUE);
                                } else {
                                    newId = RING_BUFFER1[sequence];
                                    RING_BUFFER1[sequence] = null;
                                    newIdRef = RING_BUFFER2[sequence];
                                    RING_BUFFER2[sequence] = null;
                                    reclaim(sequence);
                                    newId.init(value, hash);
                                    newIdRef.value = value;
                                }
                            }

                            map.put(value, newIdRef);
                            return newId;
                        }
                    }

                    o = o2;
                }
            } else {
                final LongKeyOpenHashMap map = (LongKeyOpenHashMap) o;

                synchronized (map) {

                    final IdRef idRef = (IdRef) map.get(value);
                    final Id id;

                    if (idRef != null) {
                        id = idRef.get();
                        if (id != null) {
                            return id;
                        }
                    }

                    final Object o2 = atomic.get();

                    if (o2 == o) {

                        if (newId == null) {
                            final int sequence = borrow();
                            if (sequence < 0) {
                                newId = new Id(value, hash);
                                newIdRef = new IdRef(newId, REFERENCE_QUEUE);
                            } else {
                                newId = RING_BUFFER1[sequence];
                                RING_BUFFER1[sequence] = null;
                                newIdRef = RING_BUFFER2[sequence];
                                RING_BUFFER2[sequence] = null;
                                reclaim(sequence);
                                newId.init(value, hash);
                                newIdRef.value = value;
                            }
                        }

                        map.put(value, newIdRef);
                        return newId;
                    }

                    o = o2;
                }
            }
        }
    }

    @SuppressWarnings({"unchecked", "SynchronizationOnLocalVariableOrMethodParameter"})
    private static void remove(@Nonnull IdRef reference) {

        final long value = reference.value;
        final int hash = hash(value);
        final AtomicReference atomic = INTERNER[hash & INTERNER_MASK];

        final Object o = atomic.compareAndExchange(reference, null);

        if (o instanceof LongKeyOpenHashMap) {
            final LongKeyOpenHashMap map = (LongKeyOpenHashMap) o;
            synchronized (map) {
                final IdRef idRef = (IdRef) map.get(value);
                if (idRef == reference) {
                    map.remove(value);
                    if (map.size() == 1) {
                        atomic.set(map.values().iterator().next());
                        map.clear();
                        reclaimMap(map);
                    }
                }
            }
        }
    }

    private static int hash(long value) {
        return (int) (value ^ (value >>> 32));
    }

    private static int borrow() {

        int reading = NEXT_READ_ENTRY.get();

        while (true) {

            if (reading == firstEmptyEntry) {
                return -1;
            }

            final int reading2 = NEXT_READ_ENTRY.compareAndExchange(reading, reading + 1 & RING_BUFFER_MASK);

            if (reading2 == reading) {
                return reading;
            }

            reading = reading2;
        }
    }

    private static void reclaim(int sequence) {

        final int next = sequence + 1 & RING_BUFFER_MASK;

        while (true) {
            if (LAST_READ_ENTRY.compareAndSet(sequence, next)) {
                if ((next & UNPARK_WATERMARK_MASK) == 0) {
                    LockSupport.unpark(PREALLOCATOR_THREAD);
                }
                break;
            }
        }
    }

    @Nonnull
    private static LongKeyOpenHashMap borrowMap() {

        int reading = NEXT_READ_POOL_ENTRY.get();

        while (true) {

            if (reading == firstEmptyPoolEntry) {
                return new LongKeyOpenHashMap(HASH_FUNCTION, 2);
            }

            final int next = reading + 1 & POOL_MASK;

            final int reading2 = NEXT_READ_POOL_ENTRY.compareAndExchange(reading, next);

            if (reading2 == reading) {

                final LongKeyOpenHashMap map = POOL[reading];
                POOL[reading] = null;

                while (true) {
                    if (LAST_READ_POOL_ENTRY.compareAndSet(reading, next)) {
                        break;
                    }
                }

                return map;
            }

            reading = reading2;
        }
    }

    private static void reclaimMap(@Nonnull LongKeyOpenHashMap map) {

        final int last = firstEmptyPoolEntry;
        final int next = last + 1 & POOL_MASK;

        if (next == LAST_READ_POOL_ENTRY.get()) {
            return;
        }

        POOL[last] = map;
        firstEmptyPoolEntry = next;
    }

    private static class IdRef extends WeakReference<Id> {

        private long value;

        public IdRef(@Nonnull Id referent, @Nonnull ReferenceQueue<? super Id> referenceQueue) {
            super(referent, referenceQueue);
            value = referent.value();
        }
    }

    private static class HashFunction implements LongHashFunction, Serializable {

        @Serial
        private static final long serialVersionUID = 5881311561566223843L;

        public int hash(long v) {
            final var hash = (int) (v ^ (v >>> 32));
            return hash == Integer.MIN_VALUE
                    ? 0 // this is a workaround for PCJ 1.2 LongKeyOpenHashMap to prevent abs(-2147483648) = -2147483648
                    : hash;
        }
    }
}
