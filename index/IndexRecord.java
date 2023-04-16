/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.index;

/**
 *
 * @author pccli
 */
public class IndexRecord {

    int blockId;
    private long primaryKey;

    public IndexRecord(Long pk, Integer bid) {
        blockId = bid;
        primaryKey = pk;
    }

    public int getBlockId() {
        return blockId;
    }

    /**
     * @return the primaryKey
     */
    public long getPrimaryKey() {
        return primaryKey;
    }

}
