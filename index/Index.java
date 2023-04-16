/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.index;

import ibd.table.record.Record;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author pccli
 */
public abstract class Index {

    public abstract void load();

    public abstract void flush();

    public abstract void clear();

    protected abstract void addEntry(Long primaryKey, IndexRecord ir);

    protected abstract boolean isEmpty();

    protected abstract void removeEntry(Long primaryKey);

    protected abstract void updateEntry(Long primaryKey);

    public abstract void addEntry(Record rec);

    public abstract void updateEntry(Record rec);

    public abstract void removeEntry(Record rec);

    public abstract IndexRecord getEntry(Long primaryKey);

    public abstract List<IndexRecord> getEntries(Long primaryKey, int comparisonType);

    public abstract int getRecordsAmount();

    public abstract Iterator iterator();

    public abstract int getFirstFittingBlock(Record rec);
}
