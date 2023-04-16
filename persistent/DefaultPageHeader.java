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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Default implementation of a page header.
 * 
 * @author Elke Achtert
 * @since 0.1
 */
public class DefaultPageHeader implements PageHeader {
  /**
   * The size of this header in Bytes, which is 8 Bytes ( 4 Bytes for
   * {@link #FILE_VERSION} and 4 Bytes for {@link #pageSize}).
   */
  private static final int SIZE = 4 * 4;

  /**
   * Version number of this header (magic number).
   */
  private static final int FILE_VERSION = 841150978;

  /**
   * The size of a page in bytes.
   */
  private int pageSize = -1;
  
  /**
   * The number of bytes additionally needed for the listing of empty pages of
   * the headed page file.
   */
  private int emptyPagesSize = 0;
  
  /**
   * The largest ID used so far
   */
  private int largestPageID = 0;


  /**
   * Empty constructor for serialization.
   */
  public DefaultPageHeader() {
    // empty constructor
  }

  /**
   * Creates a new header with the specified parameters.
   * 
   * @param pageSize the size of a page in bytes
   */
  public DefaultPageHeader(int pageSize) {
    this.pageSize = pageSize;
  }

  @Override
  public int size() {
    return SIZE;
  }

  
  /** @return the number of bytes needed for the listing of empty pages */
  public int getEmptyPagesSize() {
    return emptyPagesSize;
  }
  
  /**
   * Set the size required by the listing of empty pages.
   * 
   * @param emptyPagesSize the number of bytes needed for this listing of empty
   *        pages
   */
  public void setEmptyPagesSize(int emptyPagesSize) {
    this.emptyPagesSize = emptyPagesSize;
  }
  
  public void setPageSize(int pageSize) {
    this.pageSize = pageSize;
  }
  
  @Override
  public int getLargestPageID() {
    return largestPageID;
  }

  @Override
  public void setLargestPageID(int largestPageID) {
    this.largestPageID = largestPageID;
  }
  
  /**
   * Initializes this header from the given Byte array. Looks for the right
   * version and reads the integer value of {@link #pageSize} from the file.
   */
  @Override
  public void readHeader(ByteBuffer data) {
    if(data.getInt() != FILE_VERSION) {
      throw new RuntimeException("PersistentPageFile version does not match!");
    }
    this.pageSize = data.getInt();
    this.emptyPagesSize = data.getInt();
    this.largestPageID = data.getInt();
  }

  /**
   * Writes this header to the specified file. Writes the {@link #FILE_VERSION
   * version} of this header and the integer value of {@link #pageSize} to the
   * file.
   */
  @Override
  public void writeHeader(ByteBuffer buffer) {
    buffer.putInt(FILE_VERSION) //
        .putInt(pageSize)
        .putInt(this.emptyPagesSize) //
        .putInt(this.largestPageID);
  }

  /**
   * Returns the size of a page in Bytes.
   * 
   * @return the size of a page in Bytes
   */
  @Override
  public int getPageSize() {
    return pageSize;
  }
  
  /**
   * Returns the number of pages necessary for the header
   * 
   * @return the number of pages
   */
  @Override
  public int getReservedPages() {
    return size() / getPageSize() + 1;
  }
  
  /**
   * Write the indices of empty pages the the end of <code>file</code>. Calling
   * this method should be followed by a {@link #writeHeader(FileChannel)}.
   * 
   * @param emptyPages the stack of empty page ids which remain to be filled
   * @param file File to work with
   * @throws IOException thrown on IO errors
   */
  @Override
  public void writeEmptyPages(IntegerArray emptyPages, FileChannel file) throws IOException {
    if(emptyPages.isEmpty()) {
      this.emptyPagesSize = 0;
      return; // nothing to write
    }
    emptyPages.sort();
    this.emptyPagesSize = emptyPages.size * Integer.BYTES;
    ByteBuffer buf = ByteBuffer.allocateDirect(this.emptyPagesSize);
    buf.asIntBuffer().put(emptyPages.data, 0, emptyPages.size);
    file.write(buf, file.size());
  }

  /**
   * Read the empty pages from the end of <code>file</code>.
   * 
   * @param file File to work with
   * @return a stack of empty pages in <code>file</code>
   * @throws IOException thrown on IO errors
   * @throws ClassNotFoundException if the stack of empty pages could not be
   *         correctly read from file
   */
  @Override
  public IntegerArray readEmptyPages(FileChannel file) throws IOException {
    IntegerArray emptyPages = new IntegerArray();
    if(emptyPagesSize > 0) {
      int n = emptyPagesSize / Integer.BYTES;
      if(n > emptyPages.data.length) {
        emptyPages.data = new int[n];
      }
      ByteBuffer buf = ByteBuffer.allocateDirect(emptyPagesSize);
      file.read(buf, file.size() - emptyPagesSize);
        System.out.println(buf.limit());
        System.out.println(buf.position());
        buf.position(0);
        //System.out.println(buf.getInt());
        //System.out.println(buf.getInt(200));
      buf.asIntBuffer().get(emptyPages.data, 0, n);
    }
    return emptyPages;
  }
}
