/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table.management;

import ibd.index.IndexRecord;
import ibd.table.block.Block;
import ibd.table.record.Record;
import ibd.table.Table;
import java.util.Iterator;

/**
 * Records are placed in an ordered linked list of blocks The index is accessed
 * to find the proper block. This implementation releases blocks when they
 * become empty. When new blocks are required, the released ones are used in
 * order to prevent file expansion.
 *
 * @author Sergio
 */
public class OrderedTable extends Table {

    public OrderedTable(String folder, String name, int pageSize, boolean recreate) throws Exception {
        super(folder, name, pageSize, recreate);
    }

    /**
     * Adds a record to the table. The record is added at the chained list
     * satisfying the primary key order
     *
     * @param record
     * @return
     * @throws Exception
     */
    @Override
    protected Record addRecord(Record record) throws Exception {

        //finds an existing record that should come right before the new record
        IndexRecord ir = getLargestSmallerKey(record.getPrimaryKey());
        Block b = null;
        if (ir != null) {
            b = getBlock(ir.getBlockId());
        } else {
            //no record appears before. It means the new record should be placed at the head of the list
            int fb = getFirstBlock();
            if (fb != -1) {
                b = getBlock(fb);
            } else {
                //if the list is empty, the first block is created
                b = createFirstBlock();
            }
        }
        //make space for the record, if the selected block is full
        Block b1 = makeSpace(record, b);
        return addRecord(b1, record);

    }

    /**
     * Removes a record. if the block becomes empty, it is released and the
     * linked list is updated accordingly
     *
     * @param block
     * @param record
     * @return
     * @throws Exception
     */
    @Override
    protected Record removeRecord(Block block, Record record) throws Exception {

        record = super.removeRecord(block, record);

        if (record == null) {
            return null;
        }

        if (block.isEmpty()) {

            int prevId = block.prev_block_id;
            if (prevId != -1) {
                //the previous block is linked to the next block
                Block prevBlock = getBlock(prevId);
                prevBlock.next_block_id = block.next_block_id;
                dataFile.writePage(prevBlock);
            } else {
                setFirstBlock(block.next_block_id);
            }

            int nextId = block.next_block_id;
            if (nextId != -1) {
                //the next block is linked to the previous block
                Block nextBlock = getBlock(nextId);
                nextBlock.prev_block_id = block.prev_block_id;
                dataFile.writePage(nextBlock);
            }

            //the block is released
            dataFile.deletePage(block.getPageID());
        }

        return record;

    }


    /**
     * Make space for the record into block b, if necessary.
     *
     * @return the block where the record is supposed to be inserted. It may be
     * different than the block b
     */
    protected Block makeSpace(Record record, Block b) throws Exception {

        //if there is no space in the block
        if (!b.fits(record.len())) {

            Record highestRecord = findHighest(b);

            //if the new record is higher than the highest record in the block
            //than the record is supposed to be place in the next block
            if (record.getPrimaryKey() > highestRecord.getPrimaryKey()) {

                //if there is no next block, a new block is added as the tail of the list
                if (b.next_block_id == -1) {
                    Block b2 = addBlock(b);
                    return b2;
                } else {//makes space in the next block
                    Block b2 = getBlock(b.next_block_id);
                    makeSpace(record, b2);
                    return b2;
                }
            }

            //removes the highest record from b
            removeRecord(b, highestRecord);
            //moves the highest record in the block that succeeds b
            recursiveSlide(b, highestRecord);
            return b;
        }
        return b;
    }

    /**
     * Moves the record rec to the block that succeeds prevBlock. If the block
     * is full, the method may need to be called recursively to keep moving
     * records forward
     */
    private void recursiveSlide(Block prevBlock, Record rec) throws Exception {

        Integer next = prevBlock.next_block_id;

        //gets the next block. If there is none, a new one is added at the tail of the list
        Block b2 = null;
        if (next == -1) {
            b2 = addBlock(prevBlock);
        } else {
            b2 = getBlock(next);
        }

        //if there is no space enough, we need to make room by moving the highest record to the next block
        if (!b2.fits(rec.len())) {
            Record highestRecord = findHighest(b2);
            removeRecord(b2, highestRecord);

            addRecord(b2, rec);

            recursiveSlide(b2, highestRecord);

        } else {
            addRecord(b2, rec);

        }
    }

    /**
     * traverses the numeric values from the primaryKey down until an existing
     * indexed record is found
     */
    private IndexRecord getLargestSmallerKey(long primaryKey) {
        IndexRecord ir = null;
        for (long i = primaryKey; i >= 0; i--) {
            ir = index.getEntry(i);
            if (ir != null) {
                break;
            }
        }
        return ir;
    }

    

    /**
     * Fids the record of block that has the highest primary key
     */
    private Record findHighest(Block block) throws Exception {

        Long max = -1L;
        Record maxR = null;
        Iterator<Record> it = block.iterator();
        //for (int x = 0; x < b.getRecordsCount(); x++) {
        while (it.hasNext()) {
            //Record rec = b.getRecord(x);
            Record rec = it.next();
            if (rec.getPrimaryKey() > max) {
                max = rec.getPrimaryKey();
                maxR = rec;
            }
        }
        return maxR;
    }

}
