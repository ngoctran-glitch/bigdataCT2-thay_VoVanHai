package vn.edu.ueh.bit;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Random;

public class GenerateSimulation {
    /**
     * generate maxTrans transactions of noTrans transactions
     * @param maxTrans: the number of transaction id
     * @param noTrans: no of transactionz
     */
    static void gen(int maxTrans, int noTrans, String filePath) throws Exception {
        Random rand = new Random();
        PrintWriter out = new PrintWriter(new FileOutputStream(filePath));

        for (int i = 0; i < noTrans; i++) {
            int rand1 = rand.nextInt(maxTrans);
            int rand2 = rand.nextInt(maxTrans);
            String line = "Transaction#" + rand1 + " " + rand2;
            out.println(line); //ghi xuong file
        }

        out.close();
    }

    public static void main(String[] args) throws Exception {
        gen(1000, 100000, "c:/sample/data3.txt");
        System.out.println("DONE");
    }
}

