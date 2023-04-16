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
public class LoadedRecord extends Record{

    public LoadedRecord(Long cid) {
        super(cid);
        changed = false;
    }
    
}
