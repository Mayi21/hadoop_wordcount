package Practice;

import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;

public class Temp {
    public static void main(String[] args){
        Map<String,String> m= new HashMap<>();
        m.put("1","11");
        m.put("2","222");
        m.put("3","333");
        for (Map.Entry<String,String> n : m.entrySet()){
            String string = n.getKey();
            System.out.println(string);
        }
    }
}
