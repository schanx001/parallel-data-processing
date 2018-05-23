package pdpone;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.LongStream;
/*
 *<h1>This class computes the averages with multithreaded fine lock method with fibonacci series</h1>
 * @author=Shantanu Kawlekar
 * */

public class FineLockFib implements Runnable {
    private static HashMap<String,ArrayList<Integer>> result=new HashMap<String,ArrayList<Integer>>();
    private List<String> strLst;
    private static Object lock=new Object();

    public FineLockFib(List<String> strLst) {
        this.strLst=strLst;
    }

    /*
     * this method initializes the threads for this class and also evaluates the final output.
     * this method also times the execution and calculates the min,max and avg time
     *
     * */
    public void fineLockInit() throws InterruptedException {

        long[] timeInMs =new long[10];
        long min;
        long max;
        long avg;
        long start;
        long end;
        HashMap<String,Double> output= new HashMap<>();

        FineLockFib fl= new FineLockFib(strLst);
        Thread t1;
        Thread t2;
        Thread t3;
        for(int i=0;i<10;i++) {
            start = System.currentTimeMillis();
            t1 = new Thread(fl);
            t2 = new Thread(fl);
            t3 = new Thread(fl);
            t1.setName("t1");
            t2.setName("t2");
            t3.setName("t3");
            t1.start();
            t2.start();
            t3.start();
            t1.join();
            t2.join();
            t3.join();

            for(String key:result.keySet()){
                output.put(key,(double)(result.get(key).get(0))/result.get(key).get(1));
            }
            end = System.currentTimeMillis();
            timeInMs[i]= end-start;
        }
        Arrays.sort(timeInMs);
        min=timeInMs[0];
        max=timeInMs[timeInMs.length-1];
        avg=(LongStream.of(timeInMs).sum())/10;
        System.out.println("min: "+min+" | max: "+max+" | avg: "+avg);


        System.out.println(output.get("USC00033132"));
    }


    /*
     * Creates sublist for the threads and accordingly calls compute method
     * */
    @Override
    public void run() {
        if(Thread.currentThread().getName().equals("t1")){
            fineLockCompute(strLst.subList(0,strLst.size()/3));
        }
        if(Thread.currentThread().getName().equals("t2")){
            fineLockCompute(strLst.subList((strLst.size()/3)+1,(strLst.size()/3)*2));
        }
        if(Thread.currentThread().getName().equals("t3")){
            fineLockCompute(strLst.subList(((strLst.size()/3)*2)+1,strLst.size()));
        }
    }


    /*
     * this method updates the shared data structure and also creates station id key if not present
     * it also updates the accumulator within the shared data structure with new values for the existing station
     *
     *
     * @param= List of strings (records per thread)
     * */
    public void fineLockCompute(List<String> s){

        FibonacciSequence  fs =new FibonacciSequence();
        for(int i=0;i<s.size();i++){
            ArrayList<Integer> lst = new ArrayList<>();
            String[] temp = s.get(i).split(",");
            if(temp[2].equals("TMAX")){
                if(result.containsKey(temp[0])){
                    // synchronizing the accumulator data structure with dummy lock object to avoid race condition

                    synchronized (lock) {
                        lst.add(result.get(temp[0]).get(0) + Integer.parseInt(temp[3]));
                        lst.add(result.get(temp[0]).get(1) + 1);

                        // calling fibonacci function
                        fs.iterativeSequence(17);
                        result.put(temp[0], lst);
                    }
                }
                else{
                    // synchronizing the accumulator data structure with dummy lock object to avoid race condition

                    synchronized (lock) {
                        lst.add(Integer.parseInt(temp[3]));
                        lst.add(1);
                        result.put(temp[0], lst);
                    }
                }
            }
        }
    }
}
