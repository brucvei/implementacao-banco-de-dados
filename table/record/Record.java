/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table.record;

import ibd.table.block.Block;

/**
 *
 * @author pccli
 */
public abstract class Record extends AbstractRecord {

    public final static Integer RECORD_SIZE = 1400;

    private final Long primaryKey;
    private String content;
    private Block block;
    private Integer rec_id;

    private int len;

    protected boolean changed;

    public Record(Long pk) {
        primaryKey = pk;
    }

    public int len() {
        return 8 + len;
    }

    /**
     * @return the content
     */
    public String getContent() {
        return content;
    }

    /**
     * @param content the content to set
     */
    public void setContent(String content) {
        changed = true;

        if (content.length() > RECORD_SIZE) {
            this.content = content.substring(0, RECORD_SIZE);
        } else {
            this.content = content;
        }

        len = this.content.getBytes().length + 2;

    }

    /**
     * @return the block
     */
    public Block getBlock() {
        return block;
    }

    /**
     * @param block the block to set
     */
    public void setBlock(Block block) {
        this.block = block;
    }

    /**
     * @param rec_id the rec_id to set
     */
    public void setRec_id(int rec_id) {
        this.rec_id = rec_id;
    }

    /**
     * @return the data_id
     */
    @Override
    public Long getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public Integer getRecordId() {
        return rec_id;
    }

    @Override
    public Integer getBlockId() {
        return block.getPageID();
    }

    @Override
    public String toString() {
        return primaryKey + ":" + content;
    }

}
