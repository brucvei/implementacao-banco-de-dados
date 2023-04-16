/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ibd.table.record;

/**
 *
 * @author pccli
 */
public abstract class AbstractRecord implements Comparable<AbstractRecord>{
    
    
    public abstract Integer getRecordId();
    
    public abstract String getContent();
    
    public abstract Integer getBlockId();

    public abstract Long getPrimaryKey();

    @Override
    public int compareTo(AbstractRecord o) {
        return getPrimaryKey().compareTo(o.getPrimaryKey());
    }
    
    
}
