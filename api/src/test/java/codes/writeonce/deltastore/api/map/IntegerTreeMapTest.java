package codes.writeonce.deltastore.api.map;

import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toCollection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IntegerTreeMapTest {

    @Test
    public void test1() {

        final var all = Stream.iterate(1, n -> n + 1).limit(5000).collect(toCollection(ArrayList::new));
        final var map = new IntegerTreeMap<Integer>();

        for (final Integer value : all) {
            assertNull(map.put(value, nvl(value)));
        }

        check(map, emptySet(), all);
    }

    @Test
    public void test2() {

        final var all = Stream.iterate(1, n -> n + 1).limit(5000).collect(toCollection(ArrayList::new));
        final var map = new IntegerTreeMap<Integer>();

        Collections.reverse(all);

        for (final Integer value : all) {
            assertNull(map.put(value, nvl(value)));
        }

        check(map, emptySet(), all);
    }

    @Test
    public void test3() {

        final var all = Stream.iterate(1, n -> n + 1).limit(6).collect(toCollection(HashSet::new));
        final var map = new IntegerTreeMap<Integer>();

        check(map, all, emptySet());
        recurAddRemove(map, all, emptySet());
    }

    @Test
    public void test4() {

        final var map = new IntegerTreeMap<Integer>();
        map.put(null, null);
        for (int i = 0; i < 18; i++) {
            map.put(i, (Integer) i);
        }
        map.remove(17);
        map.remove(1);
        map.put(17, (Integer) 17);
        map.put(18, (Integer) 18);
    }

    private void recurAddRemove(
            @Nonnull IntegerTreeMap<Integer> map,
            @Nonnull Set<Integer> notAdded,
            @Nonnull Set<Integer> added
    ) {
        for (final Integer value : notAdded) {

            final var subsetNotAdded = new HashSet<Integer>(notAdded);
            final var subsetAdded = new HashSet<Integer>(added);
            subsetNotAdded.remove(value);
            subsetAdded.add(value);
            final var clone = map.clone();
            assertNull(clone.put(value, nvl(value)));
            check(map, notAdded, added);
            recurAddRemove(clone, subsetNotAdded, subsetAdded);
            recurRemove(clone, subsetNotAdded, subsetAdded);
        }
    }

    private void recurRemove(
            @Nonnull IntegerTreeMap<Integer> map,
            @Nonnull Set<Integer> notAdded,
            @Nonnull Set<Integer> added
    ) {
        for (final Integer value : added) {

            final var subsetNotAdded = new HashSet<Integer>(notAdded);
            final var subsetAdded = new HashSet<Integer>(added);
            subsetNotAdded.add(value);
            subsetAdded.remove(value);
            final var clone = map.clone();
            assertEquals(nvl(value), clone.remove(value));
            check(map, notAdded, added);
            recurRemove(clone, subsetNotAdded, subsetAdded);
        }
    }

    private void check(@Nonnull IntegerTreeMap<Integer> map, @Nonnull Collection<Integer> notAdded,
            @Nonnull Collection<Integer> added) {

        assertEquals(added.size(), map.size());

        for (final Integer value : added) {
            assertEquals(nvl(value), map.get(value));
        }

        for (final Integer value : notAdded) {
            assertNull(map.get(value));
        }

        final var nodes = added.contains(null) ? map.size() - 1 : map.size();
        count = 0;
        maxDepth = 0;
        if (map.root != 0) {
            assertEquals(0, map.flags[map.root * 3 + 2] & AbstractTreeMap.RED);
            traverse(map, map.root, 0);
        }
        assertEquals(map.size(), count);
        final var expectedMaxDepth = Math.ceil(2 * Math.log(nodes + 1) / Math.log(2));
        assertTrue("depth: " + maxDepth + " expected: " + expectedMaxDepth, maxDepth <= expectedMaxDepth);
    }

    private int count;

    private int maxDepth;

    private int traverse(IntegerTreeMap<Integer> map, int p, int depth) {

        int black;

        if ((map.flags[p * 3 + 2] & AbstractTreeMap.RED) == 0) {
            black = 1;
        } else {
            black = 0;
        }

        count++;
        depth++;

        if (maxDepth < depth) {
            maxDepth = depth;
        }

        final var left = map.flags[p * 3];
        final var right = map.flags[p * 3 + 1];

        if ((map.flags[p * 3 + 2] & AbstractTreeMap.RED) != 0) {
            int blacks = 0;
            if (blackChild(map, left)) {
                blacks++;
            }
            if (blackChild(map, right)) {
                blacks++;
            }
            assertTrue(blacks == 0 || blacks == 2);
        }

        final int b;
        if (left != 0) {
            b = traverse(map, left, depth);
        } else {
            b = 0;
        }

        if (right != 0) {
            final var b2 = traverse(map, right, depth);
            if (left != 0) {
                assertEquals(b2, b);
            }
            black += b2;
        } else if (left != 0) {
            black += b;
        }

        return black;
    }

    private boolean blackChild(IntegerTreeMap<Integer> map, int p) {
        return p != 0 && (map.flags[p * 3 + 2] & AbstractTreeMap.RED) == 0;
    }

    private static Integer nvl(Integer v) {
        if (v == null) {
            return -1;
        }
        return v;
    }
}
