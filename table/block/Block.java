/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table.block;

import ibd.table.record.CreatedRecord;
import ibd.table.record.LoadedRecord;
import ibd.table.Params;
import ibd.table.record.Record;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.TreeMap;

/**
 *
 * @author pccli
 */
public class Block extends PersistentBlock implements Iterable {

    public Integer prev_block_id = -1;
    public Integer next_block_id = -1;
    public Integer prev_heap_block_id = -1;
    public Integer next_heap_block_id = -1;

    TreeMap<Long, Record> records = new TreeMap<>();
    private int offset;

    public Block(int pageSize) {
        super(pageSize);
    }

    @Override
    public Iterator iterator() {
        return records.values().iterator();
    }

    public void removeAllRecords() throws Exception {
        for (int i = records.size() - 1; i >= 0; i--) {
            Record rec = records.get(i);
            removeRecord(rec);
        }

    }
    
    @Override
    public int getHeaderSize(){
        return super.getHeaderSize() + 20;
    }

    public boolean fits(int len) {
        return (pageSize - getHeaderSize() - usedSpace - len > 0);
    }

    public boolean isEmpty() {
        return (usedSpace == 0);
    }

    public Record getRecord(long pk) {
        return records.get(pk);
    }

    public int getRecordsCount() {
        return records.size();
    }

    public Record[] getRecords() {
        Record result[] = new Record[records.size()];
        return records.values().toArray(result);
    }

    public Record addRecord(Record rec) {

        if (!fits(rec.len())) {
            return null;
        }

        Params.RECORDS_ADDED++;

        rec.setBlock(this);
        records.put(rec.getPrimaryKey(), rec);

        usedSpace += rec.len();

        return rec;

    }

    public Record removeRecord(Record rec) throws Exception {

        if (!records.containsKey(rec.getPrimaryKey())) {
            return null;
        }

        records.remove(rec.getPrimaryKey());

        Params.RECORDS_REMOVED++;
        //System.out.println("removing record from "+block_id+" now contains free records = "+freeRecords.size());

        usedSpace -= rec.len();
        return rec;
    }

    public Record updateRecord(Record rec, String content) throws Exception {

        Record aux = new CreatedRecord(rec.getPrimaryKey());
        aux.setContent(content);

        int updatedLen = aux.len() - rec.len();

        if (!fits(updatedLen)) {
            return null;
        }

        rec.setContent(aux.getContent());

        usedSpace += updatedLen;
        return rec;

    }

    public Record maxPrimaryKey() {
        Iterator<Record> it = records.values().iterator();

        long max = -1;
        Record maxRec = null;

        while (it.hasNext()) {
            Record rec = it.next();
            if (rec.getPrimaryKey() > max) {
                {
                    maxRec = rec;
                    max = rec.getPrimaryKey();
                }
            }
        }

        return maxRec;
    }

    @Override
    public void writeExternal(DataOutput out) throws IOException {

        super.writeExternal(out);

        Params.BLOCKS_SAVED++;

        out.writeInt(this.prev_block_id);//4
        out.writeInt(this.next_block_id);//4
        out.writeInt(this.prev_heap_block_id);//4
        out.writeInt(this.next_heap_block_id);//4

        out.writeInt(records.size());//4
        //save record
        //for (int x = 0; x < block.getRecordsCount(); x++) {
        //Record rec_ = block.getRecord(x);
        //if (rec_==null) continue;
        Iterator<Record> it = iterator();
        while (it.hasNext()) {
            Record rec_ = it.next();
            //file.seek(blockOffset + Block.HEADER_LEN + rec_.getRecordId() * Record.RECORD_SIZE);
            out.writeLong(rec_.getPrimaryKey());
            out.writeUTF(rec_.getContent());

        }

    }

    @Override
    public void readExternal(DataInput in) throws IOException {

        super.readExternal(in);

        //System.out.println("loading block "+block_id);
        Params.BLOCKS_LOADED++;
        //start read

        //Long blockOffset = HEADER_LEN + Block.BLOCK_LEN * block_id;
        //file.seek(blockOffset);
        //4 bytes were read by the super class
        //byte[] bytes = new byte[Block2.BLOCK_LEN.intValue()-4];
        //in.readFully(bytes);
        //offset = 0;
        prev_block_id = in.readInt();//readInt(bytes);
        next_block_id = in.readInt();//readInt(bytes);
        prev_heap_block_id = in.readInt();//readInt(bytes);
        next_heap_block_id = in.readInt();//readInt(bytes);

        //load records
        Integer len = in.readInt();//readInt(bytes);
        for (int index = 0; index < len; index++) {
            //loadRecord(index, bytes);
            loadRecord(index, in);
        }

    }

    private void loadRecord(int index, DataInput in) throws IOException {
        //private void loadRecord(int index, byte[] bytes) throws IOException {

        //offset = (int) (Block3.HEADER_LEN + Record.RECORD_SIZE * index);

        Long primaryKey = in.readLong();//readLong(bytes); 
        String content = in.readUTF();//readUTF8(bytes); 
        Record rec = new LoadedRecord(primaryKey);
        rec.setContent(content);

        rec.setBlock(this);
        records.put(rec.getPrimaryKey(), rec);

        //addRecord(rec);
        //block.records.put(rec.getRec_id(), rec);
        //index.put(rec.getContent_id(), rec);
    }

    public int readInt(byte[] b) {
        int l = ((int) b[offset++] << 24)
                | ((int) b[offset++] & 0xff) << 16
                | ((int) b[offset++] & 0xff) << 8
                | ((int) b[offset++] & 0xff);
        return l;
    }

    public long readLong(byte[] b) {
        long l = ((long) b[offset++] << 56)
                | ((long) b[offset++] & 0xff) << 48
                | ((long) b[offset++] & 0xff) << 40
                | ((long) b[offset++] & 0xff) << 32
                | ((long) b[offset++] & 0xff) << 24
                | ((long) b[offset++] & 0xff) << 16
                | ((long) b[offset++] & 0xff) << 8
                | ((long) b[offset++] & 0xff);
        return l;
    }

    public short readShort(byte[] b) {
        long l = ((long) b[offset++] << 8)
                | ((long) b[offset++] & 0xff);
        return (short) l;
    }

    public String readUTF8(byte[] b) {
        short len = readShort(b);
        return new String(b, offset, len, StandardCharsets.UTF_8);
    }

    public boolean readBoolean(byte[] b) {

        return ((b[offset++] & 0xff) == 1);

    }

}
