package Map_Reduce;

import java.io.IOException;

public class PeopleRankDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        //Create_data.Run();
        //Create_pr.Run();
        AdjacencyMatrix.run();
        for (int i = 0; i < 6; i++) {
            CalcPeopleRank.run();
        }
        FinallyResult.run();
    }
}
