package edu.berkeley.cs186.database.index;

import java.nio.ByteBuffer;
import java.util.*;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.RecordId;

/**
 * A inner node of a B+ tree. Every inner node in a B+ tree of order d stores
 * between d and 2d keys. An inner node with n keys stores n + 1 "pointers" to
 * children nodes (where a pointer is just a page number). Moreover, every
 * inner node is serialized and persisted on a single page; see toBytes and
 * fromBytes for details on how an inner node is serialized. For example, here
 * is an illustration of an order 2 inner node:
 * <p>
 * +----+----+----+----+
 * | 10 | 20 | 30 |    |
 * +----+----+----+----+
 * /     |    |     \
 */
class InnerNode extends BPlusNode {
    // Metadata about the B+ tree that this node belongs to.
    private BPlusTreeMetadata metadata;

    // Buffer manager
    private BufferManager bufferManager;

    // Lock context of the B+ tree
    private LockContext treeContext;

    // The page on which this leaf is serialized.
    private Page page;

    // The keys and child pointers of this inner node. See the comment above
    // LeafNode.keys and LeafNode.rids in LeafNode.java for a warning on the
    // difference between the keys and children here versus the keys and children
    // stored on disk.
    private List<DataBox> keys;
    private List<Long> children;

    // Constructors //////////////////////////////////////////////////////////////

    /**
     * Construct a brand new inner node.
     */
    InnerNode(BPlusTreeMetadata metadata, BufferManager bufferManager, List<DataBox> keys,
              List<Long> children, LockContext treeContext) {
        this(metadata, bufferManager, bufferManager.fetchNewPage(treeContext, metadata.getPartNum(), false),
                keys, children, treeContext);
    }

    /**
     * Construct an inner node that is persisted to page `page`.
     */
    private InnerNode(BPlusTreeMetadata metadata, BufferManager bufferManager, Page page,
                      List<DataBox> keys, List<Long> children, LockContext treeContext) {
        assert (keys.size() <= 2 * metadata.getOrder());
        assert (keys.size() + 1 == children.size());

        this.metadata = metadata;
        this.bufferManager = bufferManager;
        this.treeContext = treeContext;
        this.page = page;
        this.keys = new ArrayList<>(keys);
        this.children = new ArrayList<>(children);

        sync();
        page.unpin();
    }

    // Core API //////////////////////////////////////////////////////////////////
    // See BPlusNode.get.
    @Override
    public LeafNode get(DataBox key) {
        // TODO(proj2): implement
        switch (key.type().getTypeId()) {
            case BOOL:
                throw new BPlusTreeException("Bool is not supported as key");
            case INT:
                for (int i = 0; i < keys.size(); i++) {
                    if (i == (keys.size() - 1)) {
                        if (key.getInt() < keys.get(i).getInt()) {
                            return getLeafNode(key, i);
                        } else if ((key.getInt() >= keys.get(i).getInt())) {
                            return getLeafNode(key, i + 1);
                        }
                    } else {
                        if (key.getInt() < keys.get(i).getInt()) {
                            return getLeafNode(key, i);
                        } else if ((key.getInt() >= keys.get(i).getInt()) && (key.getInt() < keys.get(i + 1).getInt())) {
                            getLeafNode(key, i + 1);
                        }
                    }
                }
            case FLOAT:
                for (int i = 0; i < keys.size(); i++) {
                    if (i == (keys.size() - 1)) {
                        if (key.getFloat() < keys.get(i).getFloat()) {
                            return LeafNode.fromBytes(this.metadata, this.bufferManager, this.treeContext, children.get(i));
                        } else if ((key.getFloat() >= keys.get(i).getFloat())) {
                            return LeafNode.fromBytes(this.metadata, this.bufferManager, this.treeContext, children.get(i + 1));
                        }
                    } else {
                        if (key.getFloat() < keys.get(i).getFloat()) {
                            return LeafNode.fromBytes(this.metadata, this.bufferManager, this.treeContext, children.get(i));
                        } else if ((key.getFloat() >= keys.get(i).getFloat()) && (key.getFloat() < keys.get(i + 1).getFloat())) {
                            return LeafNode.fromBytes(this.metadata, this.bufferManager, this.treeContext, children.get(i + 1));
                        }
                    }
                }
            case STRING:
                for (int i = 0; i < keys.size(); i++) {
                    if (i == (keys.size() - 1)) {
                        if (key.getString().compareTo(keys.get(i).getString()) < 0) {
                            return LeafNode.fromBytes(this.metadata, this.bufferManager, this.treeContext, children.get(i));
                        } else if (key.getString().compareTo(keys.get(i).getString()) >= 0) {
                            return LeafNode.fromBytes(this.metadata, this.bufferManager, this.treeContext, children.get(i + 1));
                        }
                    } else {
                        if (key.getString().compareTo(keys.get(i).getString()) < 0) {
                            return LeafNode.fromBytes(this.metadata, this.bufferManager, this.treeContext, children.get(i));
                        } else if ((key.getString().compareTo(keys.get(i).getString()) >= 0) && (key.getString().compareTo(keys.get(i + 1).getString()) < 0)) {
                            return LeafNode.fromBytes(this.metadata, this.bufferManager, this.treeContext, children.get(i + 1));
                        }
                    }
                }
            case LONG:
                for (int i = 0; i < keys.size(); i++) {
                    if (i == (keys.size() - 1)) {
                        if (key.getLong() < keys.get(i).getLong()) {
                            return LeafNode.fromBytes(this.metadata, this.bufferManager, this.treeContext, children.get(i));
                        } else if ((key.getLong() >= keys.get(i).getLong())) {
                            return LeafNode.fromBytes(this.metadata, this.bufferManager, this.treeContext, children.get(i + 1));
                        }
                    } else {
                        if (key.getLong() < keys.get(i).getLong()) {
                            return LeafNode.fromBytes(this.metadata, this.bufferManager, this.treeContext, children.get(i));
                        } else if ((key.getLong() >= keys.get(i).getLong()) && (key.getLong() < keys.get(i + 1).getLong())) {
                            return LeafNode.fromBytes(this.metadata, this.bufferManager, this.treeContext, children.get(i + 1));
                        }
                    }
                }
        }

        return null;
    }

    private LeafNode getLeafNode(DataBox key, int i) {
        BPlusNode bPlusNode = BPlusNode.fromBytes(this.metadata, this.bufferManager, this.treeContext, children.get(i));
        if (bPlusNode instanceof LeafNode) {
            return (LeafNode) bPlusNode;
        } else return bPlusNode.get(key);
    }

    // See BPlusNode.getLeftmostLeaf.
    @Override
    public LeafNode getLeftmostLeaf() {
        assert (children.size() > 0);
        // TODO(proj2): implement
        if (!children.isEmpty()) {
            BPlusNode bPlusNode = BPlusNode.fromBytes(this.metadata, this.bufferManager, this.treeContext, children.get(0));
            if (bPlusNode instanceof LeafNode) {
                return (LeafNode) bPlusNode;
            } else {
                return bPlusNode.getLeftmostLeaf();
            }
        }
        return null;
    }

    // See BPlusNode.put.
    @Override
    public Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid) {
        // TODO(proj2): implement

        //non over flow case
        Optional<Pair<DataBox, Long>> pushedUpPair = findAndPutKey(key, rid);
        if (!pushedUpPair.isPresent()) {
            return pushedUpPair;
        }

        if ((keys.size() + 1) <= 2 * this.metadata.getOrder()) {
            switch (key.type().getTypeId()) {
                case INT:
                    Pair<List<DataBox>, List<Long>> listListPair = intPutWithoutOverflow(pushedUpPair.get().getFirst(), pushedUpPair.get().getSecond());
                    this.keys = listListPair.getFirst();
                    this.children = listListPair.getSecond();
                    sync();
                    break;
            }
        } else {
            switch (key.type().getTypeId()) {
                case INT:
                    key = pushedUpPair.get().getFirst();
                    List<DataBox> leftKeys = new ArrayList<>();
                    List<DataBox> rightKeys = new ArrayList<>();
                    List<Long> leftPointers = new ArrayList<>();
                    List<Long> rightPointers = new ArrayList<>();
                    int pos = -1;
                    for (int i = 0; i < keys.size(); i++) {
                        if (i == 0 && key.getInt() < keys.get(i).getInt()) {
                            pos = 0;
                            break;
                        } else if (i == keys.size() - 1 && key.getInt() > keys.get(i).getInt()) {
                            pos = i + 1;
                            break;
                        } else if (key.getInt() > keys.get(i).getInt() && key.getInt() < keys.get(i + 1).getInt()) {
                            pos = i + 1;
                            break;
                        }
                    }
                    int maxIndexOfLeftKeys = 0;
                    boolean isLeftInnerNode = false;
                    if (pos < this.metadata.getOrder()) {
                        maxIndexOfLeftKeys = this.metadata.getOrder() - 1;
                        isLeftInnerNode = true;
                    } else {
                        maxIndexOfLeftKeys = this.metadata.getOrder();
                    }
                    for (int i = 0; i < maxIndexOfLeftKeys; i++) {
                        if (i == maxIndexOfLeftKeys - 1) {
                            leftKeys.add(keys.get(i));
                            leftPointers.add(children.get(i));
                            leftPointers.add(children.get(i + 1));
                        } else {
                            leftKeys.add(keys.get(i));
                            leftPointers.add(children.get(i));
                        }
                    }
                    for (int i = maxIndexOfLeftKeys; i < 2 * this.metadata.getOrder(); i++) {
                        rightKeys.add(keys.get(i));
                        rightPointers.add(children.get(i + 1));
                    }
                    if (isLeftInnerNode) {
                        leftKeys.add(pos, key);
                        leftPointers.add(pos + 1, pushedUpPair.get().getSecond());
                    } else {
                        int index = pos - leftKeys.size();
                        rightKeys.add(index, key);
                        rightPointers.add(index, pushedUpPair.get().getSecond());
                    }
                    this.keys = leftKeys;
                    this.children = leftPointers;
                    InnerNode leftInnerNode = this;
                    sync();
                    DataBox newRootKey = rightKeys.remove(0);
                    InnerNode rightInnerNode = new InnerNode(this.metadata, this.bufferManager, rightKeys, rightPointers, this.treeContext);
                    rightInnerNode.sync();
                    InnerNode newRoot = new InnerNode(this.metadata, this.bufferManager, Collections.singletonList(newRootKey),
                            Arrays.asList(leftInnerNode.getPage().getPageNum(), rightInnerNode.getPage().getPageNum()),
                            treeContext);
                    newRoot.sync();
                    return Optional.of(new Pair<>(newRoot.keys.get(0), rightInnerNode.getPage().getPageNum()));
            }
        }
        return Optional.empty();

    }

    private Pair<List<DataBox>, List<Long>> intPutWithoutOverflow(DataBox key, long pageNum) {
        List<DataBox> newKeys = new ArrayList<>(keys);
        List<Long> newChildren = new ArrayList<>(children);
        for (int i = 0; i < keys.size(); i++) {
            if (key.getInt() < keys.get(i).getInt()) {
                newKeys.add(i, key);
                newChildren.add(i + 1, pageNum);
                break;
            } else if (i < keys.size() - 1) {
                if ((key.getInt() >= keys.get(i).getInt()) && (key.getInt() < keys.get(i + 1).getInt())) {
                    newKeys.add(i + 1, key);
                    newChildren.add(i + 2, pageNum);
                    break;
                }
            } else {
                if ((key.getInt() >= keys.get(i).getInt())) {
                    newKeys.add(i + 1, key);
                    newChildren.add(i + 2, pageNum);
                    break;
                }
            }
        }
        return new Pair<>(newKeys, newChildren);
    }

    private Optional<Pair<DataBox, Long>> findAndPutKey(DataBox key, RecordId rid) {
        Long targetPageNum = null;
        for (int i = 0; i < keys.size(); i++) {
            if (key.getInt() < keys.get(i).getInt()) {
                targetPageNum = children.get(i);
                break;
            } else if (i < keys.size() - 1) {
                if ((key.getInt() >= keys.get(i).getInt()) && (key.getInt() < keys.get(i + 1).getInt())) {
                    targetPageNum = children.get(i + 1);
                    break;
                }
            } else {
                if ((key.getInt() >= keys.get(i).getInt())) {
                    targetPageNum = children.get(i + 1);
                    break;
                }
            }
        }
        BPlusNode targetNode = BPlusNode.fromBytes(this.metadata, this.bufferManager, this.treeContext, targetPageNum);
        return targetNode.put(key, rid);
    }

    // See BPlusNode.bulkLoad.
    @Override
    public Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data,
                                                  float fillFactor) {
        // TODO(proj2): implement
        int maxNodeSize = this.metadata.getOrder() * 2;
        LeafNode leafNode = new LeafNode(metadata, bufferManager, Collections.emptyList(), Collections.emptyList(),
                Optional.empty(), treeContext);
        Optional<Pair<DataBox, Long>> boxLongPair = leafNode.bulkLoad(data, fillFactor);
        if (boxLongPair.isPresent() && keys.size() <= maxNodeSize) { // no overflow
            keys.add(boxLongPair.get().getFirst());
            children.add(boxLongPair.get().getSecond());
        } else if(boxLongPair.isPresent()) { // overflow
            List<DataBox> leftKeys = new ArrayList<>();
            List<Long> leftPointers = new ArrayList<>();
            List<DataBox> rightKeys = new ArrayList<>();
            List<Long> rightPointers = new ArrayList<>();
            int i = -1;
            for (i = 0; i < this.metadata.getOrder(); i++) {
                if (i == this.metadata.getOrder() - 1) {
                    leftKeys.add(keys.get(i));
                    leftPointers.add(children.get(i));
                    leftPointers.add(children.get(i + 1));
                } else {
                    leftKeys.add(keys.get(i));
                    leftPointers.add(children.get(i));
                }
            }
            for (int j = i; j < keys.size(); j++) {
                rightKeys.add(keys.get(j));
                rightPointers.add(children.get(j));
            }
            rightKeys.add(boxLongPair.get().getFirst());
            rightPointers.add(boxLongPair.get().getSecond());
            this.keys = leftKeys;
            this.children = leftPointers;
            sync();
            InnerNode rightInnerNode = new InnerNode(this.metadata, this.bufferManager, rightKeys, rightPointers, this.treeContext);
            rightInnerNode.sync();
            DataBox newRootKey = rightKeys.remove(0);
            InnerNode newRoot = new InnerNode(metadata, bufferManager, Collections.singletonList(newRootKey),
                    Arrays.asList(this.getPage().getPageNum(), rightInnerNode.getPage().getPageNum()), treeContext);
            newRoot.sync();
            return Optional.of(new Pair<>(newRoot.keys.get(0), rightInnerNode.getPage().getPageNum()));
        }


        return Optional.empty();
    }

    public static void main(String[] args) {
        System.out.println((int) Math.ceil(2 * 4 * 0.33));
        System.out.println(0 % 7);
    }

    // See BPlusNode.remove.
    @Override
    public void remove(DataBox key) {
        // TODO(proj2): implement
        switch (key.type().getTypeId()) {
            case BOOL:
                throw new BPlusTreeException("Bool is not supported as key");
            case INT:
                for (int i = 0; i < keys.size(); i++) {
                    if (i == (keys.size() - 1)) {
                        if (key.getInt() < keys.get(i).getInt()) {
                            getLeafNode(key, i).remove(key);
                        } else if ((key.getInt() >= keys.get(i).getInt())) {
                            getLeafNode(key, i + 1).remove(key);
                        }
                    } else {
                        if (key.getInt() < keys.get(i).getInt()) {
                            getLeafNode(key, i).remove(key);
                        } else if ((key.getInt() >= keys.get(i).getInt()) && (key.getInt() < keys.get(i + 1).getInt())) {
                            getLeafNode(key, i + 1).remove(key);
                        }
                    }
                }
        }
        sync();
    }

    // Helpers ///////////////////////////////////////////////////////////////////
    @Override
    public Page getPage() {
        return page;
    }

    private BPlusNode getChild(int i) {
        long pageNum = children.get(i);
        return BPlusNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
    }

    private void sync() {
        page.pin();
        try {
            Buffer b = page.getBuffer();
            byte[] newBytes = toBytes();
            byte[] bytes = new byte[newBytes.length];
            b.get(bytes);
            if (!Arrays.equals(bytes, newBytes)) {
                page.getBuffer().put(toBytes());
            }
        } finally {
            page.unpin();
        }
    }

    // Just for testing.
    List<DataBox> getKeys() {
        return keys;
    }

    // Just for testing.
    List<Long> getChildren() {
        return children;
    }

    /**
     * Returns the largest number d such that the serialization of an InnerNode
     * with 2d keys will fit on a single page.
     */
    static int maxOrder(short pageSize, Type keySchema) {
        // A leaf node with n entries takes up the following number of bytes:
        //
        //   1 + 4 + (n * keySize) + ((n + 1) * 8)
        //
        // where
        //
        //   - 1 is the number of bytes used to store isLeaf,
        //   - 4 is the number of bytes used to store n,
        //   - keySize is the number of bytes used to store a DataBox of type
        //     keySchema, and
        //   - 8 is the number of bytes used to store a child pointer.
        //
        // Solving the following equation
        //
        //   5 + (n * keySize) + ((n + 1) * 8) <= pageSizeInBytes
        //
        // we get
        //
        //   n = (pageSizeInBytes - 13) / (keySize + 8)
        //
        // The order d is half of n.
        int keySize = keySchema.getSizeInBytes();
        int n = (pageSize - 13) / (keySize + 8);
        return n / 2;
    }

    /**
     * Given a list ys sorted in ascending order, numLessThanEqual(x, ys) returns
     * the number of elements in ys that are less than or equal to x. For
     * example,
     * <p>
     * numLessThanEqual(0, Arrays.asList(1, 2, 3, 4, 5)) == 0
     * numLessThanEqual(1, Arrays.asList(1, 2, 3, 4, 5)) == 1
     * numLessThanEqual(2, Arrays.asList(1, 2, 3, 4, 5)) == 2
     * numLessThanEqual(3, Arrays.asList(1, 2, 3, 4, 5)) == 3
     * numLessThanEqual(4, Arrays.asList(1, 2, 3, 4, 5)) == 4
     * numLessThanEqual(5, Arrays.asList(1, 2, 3, 4, 5)) == 5
     * numLessThanEqual(6, Arrays.asList(1, 2, 3, 4, 5)) == 5
     * <p>
     * This helper function is useful when we're navigating down a B+ tree and
     * need to decide which child to visit. For example, imagine an index node
     * with the following 4 keys and 5 children pointers:
     * <p>
     * +---+---+---+---+
     * | a | b | c | d |
     * +---+---+---+---+
     * /    |   |   |    \
     * 0     1   2   3     4
     * <p>
     * If we're searching the tree for value c, then we need to visit child 3.
     * Not coincidentally, there are also 3 values less than or equal to c (i.e.
     * a, b, c).
     */
    static <T extends Comparable<T>> int numLessThanEqual(T x, List<T> ys) {
        int n = 0;
        for (T y : ys) {
            if (y.compareTo(x) <= 0) {
                ++n;
            } else {
                break;
            }
        }
        return n;
    }

    static <T extends Comparable<T>> int numLessThan(T x, List<T> ys) {
        int n = 0;
        for (T y : ys) {
            if (y.compareTo(x) < 0) {
                ++n;
            } else {
                break;
            }
        }
        return n;
    }

    // Pretty Printing ///////////////////////////////////////////////////////////
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < keys.size(); ++i) {
            sb.append(children.get(i)).append(" ").append(keys.get(i)).append(" ");
        }
        sb.append(children.get(children.size() - 1)).append(")");
        return sb.toString();
    }

    @Override
    public String toSexp() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < keys.size(); ++i) {
            sb.append(getChild(i).toSexp()).append(" ").append(keys.get(i)).append(" ");
        }
        sb.append(getChild(children.size() - 1).toSexp()).append(")");
        return sb.toString();
    }

    /**
     * An inner node on page 0 with a single key k and two children on page 1 and
     * 2 is turned into the following DOT fragment:
     * <p>
     * node0[label = "<f0>|k|<f1>"];
     * ... // children
     * "node0":f0 -> "node1";
     * "node0":f1 -> "node2";
     */
    @Override
    public String toDot() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            ss.add(String.format("<f%d>", i));
            ss.add(keys.get(i).toString());
        }
        ss.add(String.format("<f%d>", keys.size()));

        long pageNum = getPage().getPageNum();
        String s = String.join("|", ss);
        String node = String.format("  node%d[label = \"%s\"];", pageNum, s);

        List<String> lines = new ArrayList<>();
        lines.add(node);
        for (int i = 0; i < children.size(); ++i) {
            BPlusNode child = getChild(i);
            long childPageNum = child.getPage().getPageNum();
            lines.add(child.toDot());
            lines.add(String.format("  \"node%d\":f%d -> \"node%d\";",
                    pageNum, i, childPageNum));
        }

        return String.join("\n", lines);
    }

    // Serialization /////////////////////////////////////////////////////////////
    @Override
    public byte[] toBytes() {
        // When we serialize an inner node, we write:
        //
        //   a. the literal value 0 (1 byte) which indicates that this node is not
        //      a leaf node,
        //   b. the number n (4 bytes) of keys this inner node contains (which is
        //      one fewer than the number of children pointers),
        //   c. the n keys, and
        //   d. the n+1 children pointers.
        //
        // For example, the following bytes:
        //
        //   +----+-------------+----+-------------------------+-------------------------+
        //   | 00 | 00 00 00 01 | 01 | 00 00 00 00 00 00 00 03 | 00 00 00 00 00 00 00 07 |
        //   +----+-------------+----+-------------------------+-------------------------+
        //    \__/ \___________/ \__/ \_________________________________________________/
        //     a         b        c                           d
        //
        // represent an inner node with one key (i.e. 1) and two children pointers
        // (i.e. page 3 and page 7).

        // All sizes are in bytes.
        int isLeafSize = 1;
        int numKeysSize = Integer.BYTES;
        int keysSize = metadata.getKeySchema().getSizeInBytes() * keys.size();
        int childrenSize = Long.BYTES * children.size();
        int size = isLeafSize + numKeysSize + keysSize + childrenSize;

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) 0);
        buf.putInt(keys.size());
        for (DataBox key : keys) {
            buf.put(key.toBytes());
        }
        for (Long child : children) {
            buf.putLong(child);
        }
        return buf.array();
    }

    /**
     * Loads an inner node from page `pageNum`.
     */
    public static InnerNode fromBytes(BPlusTreeMetadata metadata,
                                      BufferManager bufferManager, LockContext treeContext, long pageNum) {
        Page page = bufferManager.fetchPage(treeContext, pageNum, false);
        Buffer buf = page.getBuffer();

        assert (buf.get() == (byte) 0);

        List<DataBox> keys = new ArrayList<>();
        List<Long> children = new ArrayList<>();
        int n = buf.getInt();
        for (int i = 0; i < n; ++i) {
            keys.add(DataBox.fromBytes(buf, metadata.getKeySchema()));
        }
        for (int i = 0; i < n + 1; ++i) {
            children.add(buf.getLong());
        }
        return new InnerNode(metadata, bufferManager, page, keys, children, treeContext);
    }

    // Builtins //////////////////////////////////////////////////////////////////
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof InnerNode)) {
            return false;
        }
        InnerNode n = (InnerNode) o;
        return page.getPageNum() == n.page.getPageNum() &&
                keys.equals(n.keys) &&
                children.equals(n.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page.getPageNum(), keys, children);
    }
}
