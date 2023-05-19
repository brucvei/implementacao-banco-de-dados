/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.binaryop.join;

import ibd.query.sourceop.TableScan;
import ibd.table.Params;
import static ibd.table.Utils.createTable;
import ibd.query.Operation;
import ibd.query.Tuple;
import ibd.table.Table;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Sergio
 */
public class Main {


    public Operation testJoin() throws Exception {

        Table table1 = createTable("c:\\teste\\ibd", "t1", Table.DEFULT_PAGE_SIZE, 50, false, 1);
        Table table2 = createTable("c:\\teste\\ibd", "t2", Table.DEFULT_PAGE_SIZE, 50, false, 1);
        Table table3 = createTable("c:\\teste\\ibd", "t3", Table.DEFULT_PAGE_SIZE, 50, false, 1);
        Table table4 = createTable("c:\\teste\\ibd", "t4", Table.DEFULT_PAGE_SIZE, 50, false, 1);

        Operation scan = new TableScan("t1", table1);
        //PKFilter scan1 = new PKFilter(scan, "t1", ComparisonTypes.LOWER_THAN, 2L);
        Operation scan2 = new TableScan("t2", table2);
        Operation scan3 = new TableScan("t3", table3);
        Operation scan4 = new TableScan("t4", table4);

        Operation join = null;
        //construa aqui as junções 

        return join;

    }


    public static void main(String[] args) {
        try {
            Main m = new Main();

            Operation op = m.testJoin();

            Params.BLOCKS_LOADED = 0;
            Params.BLOCKS_SAVED = 0;

            op.open();
            while (op.hasNext()) {
                Tuple r = op.next();
                System.out.println(r);
            }

            System.out.println("blocks loaded during reorganization " + Params.BLOCKS_LOADED);
            System.out.println("blocks saved during reorganization " + Params.BLOCKS_SAVED);

        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
