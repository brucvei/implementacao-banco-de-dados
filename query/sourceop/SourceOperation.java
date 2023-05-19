/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.sourceop;

import ibd.query.Operation;

/**
 *
 * @author Sergio
 */
public abstract class SourceOperation extends Operation{
    
    String sourceName;
    
    public SourceOperation(String sn){
        this.sourceName = sn;
        
    }
    
    public String getSource() {
    return sourceName;
    }

    @Override
    protected void feedSources() throws Exception {
        sources = new String[1];
        sources[0] = sourceName;
    }
    
    @Override
     public String toString(){
         return "["+sourceName+"]";
     }
    
}
