/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.index;

import ibd.persistent.Page;
import ibd.persistent.PageFile;
import ibd.persistent.PageSerialization;
import ibd.persistent.PersistentPageFile;
import ibd.table.block.Block;
import ibd.table.record.Record;
import ibd.table.TableHeader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 *
 * @author Sergio
 */
public abstract class IndexManagement extends Index implements Iterable<IndexRecord> {

    protected PageFile<IndexBlock> indexFile;
    protected TreeSet<FreeBlock> blocks = new TreeSet<FreeBlock>();
    public Hashtable<Integer, FreeBlock> blocks_ = new Hashtable();

    public IndexManagement(String folder, String name, int pageSize, boolean recreate) throws IOException {
        this.indexFile = new PersistentPageFile(pageSize, Paths.get(folder + "\\" + name + ".idx"), recreate);
        this.indexFile.setPageSerializationListener(new PageSerialization() {
            @Override
            public void writePage(DataOutputStream oos, Page page) throws IOException {
                ((IndexBlock) page).writeExternal(oos);
            }

            @Override
            public Page readPage(DataInputStream ois) throws IOException {
                IndexBlock block = new IndexBlock(pageSize);
                block.readExternal(ois);
                return block;
            }
        });

    }

    protected TableHeader createHeader(int pageSize, int firstBlock, int firstHeapBlock, int lastHeapBlock) {
        return new TableHeader(pageSize, firstBlock, firstHeapBlock, lastHeapBlock);
    }

    @Override
    public void load() {
        indexFile.initialize(createHeader(indexFile.getPageSize(), -1, -1, -1));

        clear();
        if (((TableHeader) indexFile.getHeader()).getFirstBlock() > -1) {
            loadPage(0);
        }
    }

    private void loadPage(int pageID) {
        if (pageID < 0) {
            return;
        }
        IndexBlock block = indexFile.readPage(pageID);

        List<IndexRecord> list1 = block.indexRecords;
        for (IndexRecord ir : list1) {
            addEntry(ir.getPrimaryKey(), ir.getBlockId());
        }

        List<FreeBlock> list2 = block.freeBlocks;
        for (FreeBlock fb : list2) {
            blocks.add(fb);
            blocks_.put(fb.getBlockId(), fb);
        }

        loadPage(block.nextBlockID);

    }

    @Override
    public void flush() {
        indexFile.reset();
        IndexBlock block = new IndexBlock(indexFile.getPageSize());
        indexFile.writePage(block);

        ((TableHeader) indexFile.getHeader()).setFirstBlock(block.getPageID());

        Iterator<IndexRecord> i2 = iterator();
        while (i2.hasNext()) {
            IndexRecord ir = i2.next();
            if (!block.addIndexRecord(ir)) {
                IndexBlock newBlock = new IndexBlock(indexFile.getPageSize());
                indexFile.writePage(newBlock);
                block.nextBlockID = newBlock.getPageID();
                indexFile.writePage(block);
                block = newBlock;
                block.addIndexRecord(ir);
            }
        }

        Iterator<FreeBlock> i1 = blocks.iterator();

        while (i1.hasNext()) {
            FreeBlock fb = i1.next();
            if (!block.addFreeBlock(fb)) {
                IndexBlock newBlock = new IndexBlock(indexFile.getPageSize());
                indexFile.writePage(newBlock);

                block.nextBlockID = newBlock.getPageID();
                indexFile.writePage(block);
                block = newBlock;
                block.addFreeBlock(fb);
            }
        }

        indexFile.writePage(block);

        indexFile.flush();
    }

    @Override
    public void clear() {
        blocks.clear();
        blocks_.clear();
    }

    @Override
    public void addEntry(Record rec) {
        addEntry(rec.getPrimaryKey(), rec.getBlockId());
        updateBlock(rec.getBlock());
    }

    private void addEntry(Long primaryKey, Integer blockId) {
        IndexRecord ir = new IndexRecord(primaryKey, blockId);
        addEntry(primaryKey, ir);
    }

    @Override
    public void removeEntry(Record rec) {
        removeEntry(rec.getPrimaryKey());
        if (isEmpty() && blocks.isEmpty()) {
            ((TableHeader) indexFile.getHeader()).setFirstBlock(-1);
        }

        updateBlock(rec.getBlock());
    }

    @Override
    public void updateEntry(Record rec) {
        updateEntry(rec.getPrimaryKey());
        updateBlock(rec.getBlock());
    }

    @Override
    public int getFirstFittingBlock(Record rec) {
        FreeBlock fb = new FreeBlock(-1);
        fb.setLen(rec.len());
        FreeBlock result = blocks.higher(fb);
        if (result != null) {
            return result.getBlockId();
        }
        return -1;
    }

//    private void removeBlock(Block2 block){
//            FreeBlock fb = blocks_.get(block.getPageID());
//            if (fb==null) return;
//            blocks_.remove(fb.getBlockId());
//            blocks.remove(fb);
//            if (index.isEmpty() && blocks.isEmpty())
//            ((TableHeader)indexFile.getHeader()).setHasData(false);
//            }
    private void updateBlock(Block block) {
        FreeBlock fb = blocks_.get(block.getPageID());
        if (fb == null) {
            fb = new FreeBlock(block.getPageID());
            fb.setLen(block.getPageSize() - block.getUsedSpace());
            blocks_.put(fb.getBlockId(), fb);
        }
        blocks.remove(fb);
        fb.setLen(block.getPageSize() - block.getUsedSpace());
        blocks.add(fb);

    }

    public int getFreeBlocksCount() {
        return blocks.size();
    }

}
