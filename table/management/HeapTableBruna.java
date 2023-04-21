package ibd.table.management;

import ibd.table.Table;
import ibd.table.block.Block;
import ibd.table.record.Record;

import java.util.LinkedList;
import java.util.List;

public class HeapTableBruna extends Table {

    public HeapTableBruna(String folder, String name, int pageSize, boolean override) throws Exception {
        super(folder, name, pageSize, override);
    }

    @Override
    protected Record addRecord(Record record) throws Exception {
        //finds first block with enough space for the record
        int blockID = findFirstFittingBlock(record);
        Block block = getBlock(blockID);
        if (block == null) {
            block = addBlock();
            super.setLastHeapBlock(block.getPageID());
        }

        Record rec = addRecord(block, record);

        changePointers(block);
        dataFile.writePage(block);

        return rec;
    }

    private int findFirstFittingBlock(Record record) throws Exception {
        int blockId = getFirstBlock();
        super.setFirstHeapBlock(blockId);
        while (blockId != -1) {
            Block b1 = getBlock(blockId);
            super.setLastHeapBlock(blockId);
            if (b1.fits(400)) {
                return b1.getPageID();
            }
            blockId = b1.next_block_id;
        }
        return -1;
    }

    private void changePointers(Block block) throws Exception {
         int first = getFirstHeapBlock();
         int free = block.getPageSize() - block.getUsedSpace();

         if (free < 400) {
             super.setFirstHeapBlock(block.prev_heap_block_id);
         }
    }

    @Override
    protected Record removeRecord(Block block, Record record) throws Exception {
        record = super.removeRecord(block, record);
        if (record == null) {
            return null;
        }

        changePointers(block);
        dataFile.writePage(block);

        return record;
    }


}

