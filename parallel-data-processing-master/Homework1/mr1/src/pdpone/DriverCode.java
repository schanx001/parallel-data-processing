package pdpone;

import java.io.IOException;
import java.util.List;

/*
*<h1>This class works as a main driver code which calls all the program versions (Fibonacci|Normal)</h1>
* @author=Shantanu Kawlekar
* */

public class DriverCode {
    public static void main(String args[]) throws IOException {
        List<String> strLst;
        LoaderRoutine l=new LoaderRoutine();
        DriverCode d=new DriverCode();
        strLst = l.loadFileInList("/home/schanx/IdeaProjects/mr1/src/pdpone/1912.csv");

        // Creating objects for each class

        Sequential s=new Sequential();
        NoLock nl=new NoLock(strLst);
        CoarseLock cl=new CoarseLock(strLst);
        FineLock fl=new FineLock(strLst);
        NoSharing ns=new NoSharing(strLst);
        SequentialFib sf=new SequentialFib();
        NoLockFib nlf=new NoLockFib(strLst);
        CoarseLockFib clf= new CoarseLockFib(strLst);
        FineLockFib flf= new FineLockFib(strLst);
        NoSharingFib nsf=new NoSharingFib(strLst);


        // Printing the values for fibonacci version of the programs

        System.out.println("Fibonacci Version");


        System.out.println(" ");
        System.out.println("Sequential");
        sf.sequentialInit(strLst);

        System.out.println(" ");
        System.out.println("NoLock");
        try {
            nlf.noLockInit();
        } catch (InterruptedException e) {
            //e.printStackTrace();
            System.out.println("Please try again!...");
        }

        System.out.println(" ");
        System.out.println("CoarseLock");
        try {
            clf.coarseLockInit();
        } catch (InterruptedException e) {
            System.out.println("Please try again!...");
            //e.printStackTrace();
        }

        System.out.println(" ");
        System.out.println("FineLock");
        try {
            flf.fineLockInit();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(" ");
        System.out.println("NoSharing");
        try {
            nsf.noSharingInit();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Printing the values for normal version of the programs

        System.out.println(" ------------------------------ ");
        System.out.println("Normal Version");

        System.out.println(" ");
        System.out.println("Sequential");
        s.sequentialInit(strLst);

        System.out.println(" ");
        System.out.println("NoLock");
        try {
            nl.noLockInit();
        } catch (InterruptedException e) {
            //e.printStackTrace();
            System.out.println("Please try again!...");
        }


        System.out.println(" ");
        System.out.println("CoarseLock");
        try {
            cl.coarseLockInit();
        } catch (InterruptedException e) {
            System.out.println("Please try again!...");
            //e.printStackTrace();
        }

        System.out.println(" ");
        System.out.println("FineLock");
        try {
            fl.fineLockInit();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(" ");
        System.out.println("NoSharing");
        try {
            ns.noSharingInit();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
