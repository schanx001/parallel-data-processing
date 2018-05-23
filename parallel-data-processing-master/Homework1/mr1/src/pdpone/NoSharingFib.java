package pdpone;

import java.util.*;
import java.util.stream.LongStream;
/*
 *<h1>This class computes the averages with multithreaded no sharing method with fibonacci series</h1>
 * @author=Shantanu Kawlekar
 * */

public class NoSharingFib implements Runnable{
    static HashMap<String,ArrayList<Integer>> result=new HashMap<String,ArrayList<Integer>>();

    // ti_thread is a hashmap to be used by thread objects
    HashMap<String,ArrayList<Integer>> ti_thread=new HashMap<String,ArrayList<Integer>>();

    private List<String> strLst;

    public NoSharingFib(List<String> strLst) {
        this.strLst=strLst;
    }

    /*
     * this method initializes the threads for this class and also evaluates the final output.
     * this method also times the execution and calculates the min,max and avg time
     *
     * */
    public void noSharingInit() throws InterruptedException {

        long[] timeInMs = new long[10];
        long min, max, avg, start, end;
        HashMap<String, Double> output = new HashMap<>();

        NoSharingFib ns1,ns2,ns3;

        for(int i=0;i<10;i++) {
            start = System.currentTimeMillis();
            ns1 = new NoSharingFib(strLst);
            ns2 = new NoSharingFib(strLst);
            ns3 = new NoSharingFib(strLst);

            Thread t1, t2, t3;

            // initializing three threads with three different objects to use their own data structure
            t1 = new Thread(ns1);
            t2 = new Thread(ns2);
            t3 = new Thread(ns3);
            t1.setName("t1");
            t2.setName("t2");
            t3.setName("t3");
            t1.start();
            t2.start();
            t3.start();

            t1.join();
            t2.join();
            t3.join();

            // creating a set with all the keys
            Set<String> s=new HashSet(ns1.ti_thread.keySet());
            s.addAll(ns2.ti_thread.keySet());
            s.addAll(ns3.ti_thread.keySet());
            ArrayList<Integer> initValues = new ArrayList<>();
            initValues.add(0);
            initValues.add(0);

            // initializing the final result hashmap data structure
            for (Iterator it = s.iterator(); it.hasNext(); ) {
                result.put((String) it.next(), initValues);

            }

            /*
            *
            * adding and updating values in the data structure from the individual data structures used by the threads
            * combining the output of each thread based data structure into one final output
            */
            for (String key : result.keySet()) {
                if (ns1.ti_thread.containsKey(key)) {
                    ArrayList<Integer> newVal = ns1.ti_thread.get(key);
                    ArrayList<Integer> updatedVal = new ArrayList<>();
                    updatedVal.add(newVal.get(0) + result.get(key).get(0));
                    updatedVal.add(newVal.get(1) + result.get(key).get(1));
                    result.put(key, updatedVal);
                }
                if (ns2.ti_thread.containsKey(key)) {
                    ArrayList<Integer> newVal = ns2.ti_thread.get(key);
                    ArrayList<Integer> updatedVal = new ArrayList<>();
                    updatedVal.add(newVal.get(0) + result.get(key).get(0));
                    updatedVal.add(newVal.get(1) + result.get(key).get(1));
                    result.put(key, updatedVal);
                }
                if (ns3.ti_thread.containsKey(key)) {
                    ArrayList<Integer> newVal = ns3.ti_thread.get(key);
                    ArrayList<Integer> updatedVal = new ArrayList<>();
                    updatedVal.add(newVal.get(0) + result.get(key).get(0));
                    updatedVal.add(newVal.get(1) + result.get(key).get(1));
                    result.put(key, updatedVal);
                }
            }
            for (String key : result.keySet()) {
                output.put(key, (double) (result.get(key).get(0)) / result.get(key).get(1));
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
     * Creates sublist for the threads and passes individual thread data structure to compute method
     * accordingly calls compute method
     *
     *
     * */
    @Override
    public void run() {
        if(Thread.currentThread().getName().equals("t1")){
            noSharingCompute(strLst.subList(0,strLst.size()/3),this.ti_thread);
        }
        if(Thread.currentThread().getName().equals("t2")){
            noSharingCompute(strLst.subList((strLst.size()/3)+1,(strLst.size()/3)*2),this.ti_thread);
        }
        if(Thread.currentThread().getName().equals("t3")){
            noSharingCompute(strLst.subList(((strLst.size()/3)*2)+1,strLst.size()),this.ti_thread);
        }
    }

    /*
     * this method updates the shared data structure and also creates station id key if not present
     * it also updates the accumulator within the shared data structure with new values for the existing station
     *
     * this method works on individual data structures for each thread
     *
     * @param= List of strings (records per thread)
     * @param=HashMap <String,ArrayList<Integer>> each thread has their own data structure to fill up
     *
     * */
    private void noSharingCompute(List<String> s,HashMap<String,ArrayList<Integer>> t_thread) {
        FibonacciSequence  fs =new FibonacciSequence();
        for(int i=0;i<s.size();i++){
            ArrayList<Integer> lst = new ArrayList<>();
            String[] temp = s.get(i).split(",");
            if(temp[2].equals("TMAX")){
                if(t_thread.containsKey(temp[0])){
                    lst.add(t_thread.get(temp[0]).get(0)+Integer.parseInt(temp[3]));
                    lst.add(t_thread.get(temp[0]).get(1)+1);

                    // calling fibonacci function
                    fs.iterativeSequence(17);

                    t_thread.put(temp[0], lst);
                }
                else{
                    lst.add(Integer.parseInt(temp[3]));
                    lst.add(1);
                    t_thread.put(temp[0], lst);
                }
            }
        }
    }
}
