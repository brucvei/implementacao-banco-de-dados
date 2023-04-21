/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table;

import ibd.table.management.HeapTableBruna;
import ibd.table.record.Record;
import ibd.table.record.CreatedRecord;
import ibd.table.block.Block;
import ibd.index.HashIndex;
import ibd.index.Index;
import ibd.index.IndexRecord;
import ibd.persistent.Page;
import ibd.persistent.PageFile;
import ibd.persistent.PageSerialization;
import ibd.persistent.PersistentPageFile;
import ibd.persistent.cache.Cache;
import ibd.persistent.cache.LRUCache;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class Table implements Iterable {

  public static final int DEFULT_PAGE_SIZE = 4096;

  protected Index index;

  public boolean loaded = false;

  public String key;

  protected PageFile<Block> dataFile;

  public Table(String folder, String name, int pageSize, boolean override) throws Exception {
    PageFile<Block> file = new PersistentPageFile(pageSize, Paths.get(folder + "\\" + name + ".dat"), override);

    file.setPageSerializationListener(new PageSerialization() {
      @Override
      public void writePage(DataOutputStream oos, Page page) throws IOException {
        if (page != null) {
          ((Block) page).writeExternal(oos);
        }
      }

      @Override
      public Page readPage(DataInputStream ois) throws IOException {
        Block block = new Block(pageSize);
        block.readExternal(ois);
        return block;
      }
    });

    Cache<Block> cache = defineBufferManagement(file);
    if (cache != null) {
      dataFile = cache;
    }

    Index newIndex = defineIndexManagement(folder, name, pageSize, override);
    if (newIndex != null) {
      index = newIndex;
    }

    loaded = false;
  }

  public Index defineIndexManagement(String folder, String name, int pageSize, boolean recreate) throws IOException {
    return new HashIndex(folder, name, pageSize, recreate);
  }

  public Cache<Block> defineBufferManagement(PageFile<Block> file) throws IOException {
    return new LRUCache<Block>(5000, file);
    //return new MidPointCache<Block2>(25000, file);
  }

  protected TableHeader createHeader(int pageSize, int firstBlock, int firstHeapBlock, int lastHeapBlock) {
    return new TableHeader(pageSize, firstBlock, firstHeapBlock, lastHeapBlock);
  }

  protected void initLoad() throws Exception {

    if (loaded) {
      return;
    }

    dataFile.initialize(createHeader(dataFile.getPageSize(), -1, -1, -1));

    index.load();

    loaded = true;

  }

  public Block getBlock(int block_id) throws Exception {
    if (!loaded) {
      initLoad();
    }
    if (block_id < 0) {
      return null;
    }
    Block n = dataFile.readPage(block_id);
    return n;
  }

  public int getLargestBlock() throws Exception {
    if (!loaded) {
      initLoad();
    }
    return dataFile.getNextPageID() - 1;
  }

  /**
   * Adds a new block after the current tail. The new block is to become the
   * tail of the chained list.
   */
  protected Block addBlock() throws Exception {
    Block b = null;
    int fb = getFirstBlock();
    if (fb == -1) {
      //the list is empty, meaning no block is yet allocated
      b = createFirstBlock();
    } else {
      //all blocks are full, so a new one should be added at the end
      Block currentTail = getBlock(getLargestBlock());
      b = addBlock(currentTail);
    }
    return b;
  }

  /**
   * Adds a new block after the current tail. The new block is to become the
   * tail of the chained list.
   *
   * @param currentTail
   */
  protected Block addBlock(Block currentTail) {
    Block newBlock = new Block(dataFile.getPageSize());
    newBlock.prev_block_id = currentTail.getPageID();
    dataFile.writePage(newBlock);

    currentTail.next_block_id = newBlock.getPageID();
    dataFile.writePage(currentTail);

    return newBlock;
  }

  protected Block createFirstBlock() throws Exception {
    Block b = new Block(dataFile.getPageSize());
    dataFile.writePage(b);
    setFirstBlock(b.getPageID());
    return b;
  }

  public int getFirstBlock() throws Exception {
    if (!loaded) {
      initLoad();
    }
    return ((TableHeader) dataFile.getHeader()).getFirstBlock();
  }

  protected void setFirstBlock(int blockId) throws Exception {
    if (!loaded) {
      initLoad();
    }
    ((TableHeader) dataFile.getHeader()).setFirstBlock(blockId);
  }

  protected int getFirstHeapBlock() throws Exception {
    if (!loaded) {
      initLoad();
    }
    return ((TableHeader) dataFile.getHeader()).getFirstHeapBlock();
  }

  protected void setFirstHeapBlock(int blockId) throws Exception {
    if (!loaded) {
      initLoad();
    }
    ((TableHeader) dataFile.getHeader()).setFirstHeapBlock(blockId);
  }

  protected int getLastHeapBlock() throws Exception {
    if (!loaded) {
      initLoad();
    }
    return ((TableHeader) dataFile.getHeader()).getLastHeapBlock();
  }

  protected void setLastHeapBlock(int blockId) throws Exception {
    if (!loaded) {
      initLoad();
    }
    ((TableHeader) dataFile.getHeader()).setLastHeapBlock(blockId);
  }

  public Record getRecord(Long primaryKey) throws Exception {

    if (!loaded) {
      initLoad();
    }

    IndexRecord index_rec = index.getEntry(primaryKey);
    if (index_rec == null) {
      return null;
    }

    //find the block that contains the record
    Block block = getBlock(index_rec.getBlockId());

    //now locate the record within the block
    return (Record) block.getRecord(index_rec.getPrimaryKey());
  }

  public List<Record> getRecords(Long primaryKey, int comparisonType) throws Exception {

    if (!loaded) {
      initLoad();
    }

    List<IndexRecord> list = index.getEntries(primaryKey, comparisonType);
    ArrayList<Record> records = new ArrayList();

    for (int i = 0; i < list.size(); i++) {
      IndexRecord index_rec = list.get(i);
      //find the block that contains the record
      Block block = getBlock(index_rec.getBlockId());
      //now locate the record within the block
      Record rec = (Record) block.getRecord(index_rec.getPrimaryKey());
      records.add(rec);
    }

    return records;
  }

  public Record addRecord(long primaryKey, String content) throws Exception {

    if (!loaded) {
      initLoad();
    }

    if (index.getEntry(primaryKey) != null) {
      throw new Exception("ID already exists");
    }

    Record record = new CreatedRecord(primaryKey);
    record.setContent(content);

    return addRecord(record);

  }

  protected Record addRecord(Block block, Record record) throws Exception {

    record = block.addRecord(record);

    if (record == null) {
      return null;
    }

    index.addEntry(record);

    dataFile.writePage(block);

    return record;

  }

  public Record updateRecord(long primaryKey, String content) throws Exception {

    if (!loaded) {
      initLoad();
    }

    IndexRecord index_rec = index.getEntry(primaryKey);
    if (index_rec == null) {
      return null;
    }

    Block block = getBlock(index_rec.getBlockId());
    Record record = (Record) block.getRecord(index_rec.getPrimaryKey());

    record = block.updateRecord(record, content);
    if (record == null) {
      return null;
    }

    index.updateEntry(record);
    dataFile.writePage(block);

    return record;

  }

  public Record removeRecord(long primaryKey) throws Exception {

    if (!loaded) {
      initLoad();
    }

    IndexRecord index_rec = index.getEntry(primaryKey);
    if (index_rec == null) {
      return null;
    }

    // aqui deve ser feita a atualização da lista heap, se necessário


    Block block = getBlock(index_rec.getBlockId());
    Record record = (Record) block.getRecord(index_rec.getPrimaryKey());

    return removeRecord(block, record);

  }

  protected Record removeRecord(Block block, Record record) throws Exception {

    record = block.removeRecord(record);

    if (record == null) {
      return null;
    }

    index.removeEntry(record);

    dataFile.writePage(block);

    return record;

  }

  //protected abstract Integer selectBlock(Record record) throws Exception;
  protected abstract Record addRecord(Record record) throws Exception;

  public void flushDB() throws Exception {

    if (!loaded) {
      return;
    }

    index.flush();
    dataFile.flush();
  }

  public int getRecordsAmount() throws Exception {
    if (!loaded) {
      return -1;
    }
    return index.getRecordsAmount();
  }

  @Override
  public Iterator<Record> iterator() {
    if (!loaded) {
      return null;
    }
    return new TableIterator();
  }

  class TableIterator implements Iterator {

    Iterator<IndexRecord> it;
    long currentBlock;

    public TableIterator() {
      it = index.iterator();
    }

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public Record next() {
      try {
        IndexRecord ir = it.next();
        Block block = getBlock(ir.getBlockId());
        Record rec = (Record) block.getRecord(ir.getPrimaryKey());
        return rec;
      } catch (Exception ex) {
        Logger.getLogger(Table.class.getName()).log(Level.SEVERE, null, ex);
      }
      System.out.println("não deveria chegar aqui");
      return null;
    }

  }

}
