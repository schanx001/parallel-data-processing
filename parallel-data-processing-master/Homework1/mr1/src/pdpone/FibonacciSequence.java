package pdpone;
/*
 *<h1>This class computes the fibonacci series iteratively</h1>
 * @author=Shantanu Kawlekar
 * */

public class FibonacciSequence {
    // @param n fibonacci series number
    // returns in sequence
    public int iterativeSequence(int n){
        int num=0,a=0,b=1;
        for(int i=0;i<n;i++){
            num=a+b;
            a=b;
            b=num;
        }
        return num;
    }
}
