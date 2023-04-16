/*
 * This file is part of ELKI:
 * Environment for Developing KDD-Applications Supported by Index-Structures
 *
 * Copyright (C) 2022
 * ELKI Development Team
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package ibd.persistent;

/**
 * Abstract base class for the page file API for both caches and true page files
 * (in-memory and on-disk).
 * 
 * @author Erich Schubert
 * @since 0.4.0
 * 
 * @param <P> page type
 */
public abstract class AbstractPageFile<P extends Page> implements PageFile<P> {
  

  /**
   * Constructor.
   */
  public AbstractPageFile() {
    super();
  }

  

  /**
   * Writes a page into this file. The method tests if the page has already an
   * id, otherwise a new id is assigned and returned.
   * 
   * @param page the page to be written
   * @return the id of the page
   */
  @Override
  public final synchronized int writePage(P page) {
    int pageid = setPageID(page);
    writePage(pageid, page);
    //System.out.println("page id: "+pageid);
    return pageid;
  }

  /**
   * Perform the actual page write operation.
   * 
   * @param pageid Page id
   * @param page Page to write
   */
  protected abstract void writePage(int pageid, P page);

  @Override
  public void close() {
    clear();
  }
  
  
  
  
}
