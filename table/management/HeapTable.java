/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table.management;

import ibd.table.block.Block;
import ibd.table.record.Record;
import ibd.table.Table;

/**
 * Records are placed at the first fitting block. To find a fitting block, the
 * index containing the space left of each block is accessed. This
 * implementation dos not release blocks when they become empty. They remain as
 * options to be used when necessary. However, when new blocks are required, the
 * file will always be expanded, regardless of the existence of empty blocks.
 *
 * @author Sergio
 */
public class HeapTable extends Table {

    public HeapTable(String folder, String name, int pageSize, boolean override) throws Exception {
        super(folder, name, pageSize, override);
    }

    /**
     * Adds a record to the table. The record is added at the first fitting
     * block
     *
     * @param record
     * @return
     * @throws Exception
     */
    @Override
    protected Record addRecord(Record record) throws Exception {

        //finds first block with enough space for the record
        int blockID = index.getFirstFittingBlock(record);
        Block b = getBlock(blockID);
        if (b == null) {
            b = addBlock();
        }

        return addRecord(b, record);
    }

    


}
