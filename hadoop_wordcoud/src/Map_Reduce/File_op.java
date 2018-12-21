package Map_Reduce;

import java.io.*;

public class File_op {
    public static void Delete(String path){
        File f = new File(path);
        if(f.isDirectory()){
            File[] fi = f.listFiles();
            for(File fil: fi){
                fil.delete();
            }
            f.delete();
        }
        else{
            f.delete();
        }
    }
    public static void Rname(String path,String inpath){
        File f = new File(path);
        f.renameTo(new File(inpath));
    }
    /*
    public static void main(String[] args)throws Exception{
        //AdjacencyMatrix.run();
        //CalcPeopleRank.run();
        String path1 = "D:\\Study\\JAVA\\idea\\output\\Create_pr";
        //String path2 = "D:\\Study\\JAVA\\idea\\output\\Create_pr";
        //Delete(path2);
        Rname(path1 + "\\part-r-00000",path1 + "\\peoplerank");
    }
    */

}
