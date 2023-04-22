package ibd.table.management;

import ibd.table.Table;
import ibd.table.block.Block;
import ibd.table.record.Record;

public class HeapTableBruna extends Table {

    public HeapTableBruna(String folder, String name, int pageSize, boolean override) throws Exception {
        super(folder, name, pageSize, override);
    }

    @Override
    protected Record addRecord(Record record) throws Exception {
        //finds first block with enough space for the record
        int blockID = findFirstFittingBlock();
        Block block = getBlock(blockID);
        if (block == null) {
            block = addBlock();
            super.setLastHeapBlock(block.getPageID());
        }

        Record rec = addRecord(block, record);

        changePointersInsert(block);
        dataFile.writePage(block);

        return rec;
    }

    private int findFirstFittingBlock() throws Exception {
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

    private void changePointersInsert(Block block) throws Exception {
         int free = block.getPageSize() - block.getUsedSpace();

         if (free < 400) {
             super.setFirstHeapBlock(block.next_heap_block_id);
         } else if (free > 400) {
             int id = getFirstHeapBlock();
             boolean acho = false;
             while (id != -1){
                 if (id == block.getPageID()){
                     acho = true;
                     break;
                 }
                 id = getBlock(id).next_heap_block_id;
             }
             if (!acho){
                 super.setLastHeapBlock(block.getPageID());
             }
         }
    }

    @Override
    protected Record removeRecord(Block block, Record record) throws Exception {
        record = super.removeRecord(block, record);
        if (record == null) {
            return null;
        }

        changePointersRemove(block);
        dataFile.writePage(block);

        return record;
    }

    private void changePointersRemove(Block block) throws Exception {
        int free = block.getPageSize() - block.getUsedSpace();

        if (free > 400) {
            super.setLastHeapBlock(block.next_heap_block_id);
        } else if (free < 400) {
            int id = getFirstHeapBlock();

            while (id != -1){
                if (id == block.getPageID()){
                    block.prev_heap_block_id = block.next_heap_block_id;
                }
                id = getBlock(id).next_heap_block_id;
            }

        }
    }
}

