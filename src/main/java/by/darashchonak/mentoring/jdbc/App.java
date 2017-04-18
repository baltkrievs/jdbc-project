package by.darashchonak.mentoring.jdbc;

import java.util.*;


public class App {


    public static void main(String[] args) {

        boolean isReverseMode = (args.length >= 1 && args[0] != null && args[0].toLowerCase().equals("r")) ? true : false;

        DataTool dataTool = new DataTool();

        List<String> tables = dataTool.getTables();
        dataTool.createTables(tables);

        Collections.sort(tables);

        for (String tableName : tables) {
            dataTool.copyTable(tableName, isReverseMode);
        }

    }
}
