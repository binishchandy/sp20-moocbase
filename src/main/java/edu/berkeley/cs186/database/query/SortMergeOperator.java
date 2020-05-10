package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     * See lecture slides.
     * <p>
     * Before proceeding, you should read and understand SNLJOperator.java
     * You can find it in the same directory as this file.
     * <p>
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     * This means you'll probably want to add more methods than those given (Once again,
     * SNLJOperator.java might be a useful reference).
     */
    private class SortMergeIterator extends JoinIterator {
        /**
         * Some member variables are provided for guidance, but there are many possible solutions.
         * You should implement the solution that's best for you, using any member variables you need.
         * You're free to use these member variables, but you're not obligated to.
         */
        private final BacktrackingIterator<Record> leftIterator;
        private final BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;
        private final int leftIndex;
        private final int rightIndex;
        private int r = Integer.MAX_VALUE;
        private int s = Integer.MIN_VALUE;

        private SortMergeIterator() {
            super();
            // TODO(proj3_part1): implement
            //sort left table
            SortOperator leftTableSortOperator = new SortOperator(SortMergeOperator.this.getTransaction(), this.getLeftTableName(), new LeftRecordComparator());
            String sortedLeftTempTableName = leftTableSortOperator.sort();
            this.leftIterator = SortMergeOperator.this.getRecordIterator(sortedLeftTempTableName);

            //sort right table
            SortOperator rightTableSortOperator = new SortOperator(SortMergeOperator.this.getTransaction(), this.getRightTableName(), new RightRecordComparator());
            String sortedRightTempTableName = rightTableSortOperator.sort();
            this.rightIterator = SortMergeOperator.this.getRecordIterator(sortedRightTempTableName);

            this.leftIndex = SortMergeOperator.this.getLeftColumnIndex();
            this.rightIndex = SortMergeOperator.this.getRightColumnIndex();

            fetchNextLeftRecord();
            if (leftRecord == null) throw new NoSuchElementException("end of iteration");
            fetchNextRightRecord();

            fetchNextRecord();
        }

        private void fetchNextLeftRecord() {
            if (this.leftIterator.hasNext()) {
                this.leftRecord = this.leftIterator.next();
            } else {
                this.leftRecord = null;
            }
        }

        private void fetchNextRightRecord() {
            if (this.rightIterator.hasNext()) {
                this.rightRecord = this.rightIterator.next();
            } else {
                this.rightRecord = null;
            }
        }

        private void fetchNextRecord() {
            this.nextRecord = null;
            while (!hasNext() && leftRecord != null) {
                if (!marked) {
                    r = fetchValueFromRecord(leftIndex, leftRecord).getInt();
                    s = fetchValueFromRecord(rightIndex, rightRecord).getInt();

                    while (r < s) {
                        fetchNextLeftRecord();
                        if (leftRecord == null) throw new NoSuchElementException("end of iteration");
                        r = fetchValueFromRecord(leftIndex, leftRecord).getInt();
                    }

                    while (r > s) {
                        fetchNextRightRecord();
                        s = fetchValueFromRecord(rightIndex, rightRecord).getInt();
                    }
                    rightIterator.markPrev();
                    marked = true;
                }

                if (r == s) {
                   nextRecord = joinRecords(leftRecord, rightRecord);
                   fetchNextRightRecord();
                   s = rightRecord == null ? Integer.MIN_VALUE : fetchValueFromRecord(rightIndex, rightRecord).getInt();
                   break;
                } else {
                    rightIterator.reset();
                    fetchNextRightRecord();
                    s = fetchValueFromRecord(rightIndex, rightRecord).getInt();
                    fetchNextLeftRecord();
                    if (leftRecord == null) throw new NoSuchElementException("end of iteration");
                    r = fetchValueFromRecord(leftIndex, leftRecord).getInt();
                    marked = false;
                }
            }
        }

        private DataBox fetchValueFromRecord(int index, Record record) {
            return record.getValues().get(index);
        }

        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(proj3_part1): implement

            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            // TODO(proj3_part1): implement
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            Record nextRecord = this.nextRecord;

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                        o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                        o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
