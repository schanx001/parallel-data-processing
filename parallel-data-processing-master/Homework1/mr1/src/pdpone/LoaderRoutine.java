package pdpone;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
/*
* <h1> this class loads the csv file into a list of strings </h1>
*
* @author= shantanu kawlekar
*
* */
public class LoaderRoutine {

    /*
     *  this method reads the file line by line and stores in a list of strings
     *
     *
     * @param String s which is the path of the file to be loaded into list of  strings
    */
    public List<String> loadFileInList(String s) throws IOException {
        List<String> listOfStrings = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(s))) {
            String line = "";
            while ((line = br.readLine()) != null) {
                listOfStrings.add(line);
            }
        }
        return listOfStrings;
    }

}
