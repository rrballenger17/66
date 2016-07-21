/*
 * A Java program that uses the JDBC driver for SQLite
 * to extract data from our movie database.
 * 
 * Before compiling this program, you must download the JAR file for the
 * SQLite JDBC Driver and add it to your classpath. See the Java-specific
 * notes in the assignment for more details.
 * 
 * In addition, the database file should be in the same folder 
 * as the program.
 * 
 * Computer Science E-66
 */

import java.util.*;     // needed for the Scanner class
import java.sql.*;      // needed for the JDBC-related classes
import java.io.*;       // needed for the PrintStream class

public class MovieToXML {
    

    private static void makeMoviesXML(Connection db) throws ClassNotFoundException, SQLException, FileNotFoundException{

        PrintStream outfile = new PrintStream("movies.xml");

        // Create a Statement object and use it to execute the query.
        Statement stmt = db.createStatement();
        String query = "SELECT id, name, year,rating,runtime,genre, earnings_rank FROM Movie;";
        ResultSet results = stmt.executeQuery(query);
        
        Statement stmtTwo = db.createStatement();
        // Iterate over the tuples in the result and write them to the file.

        outfile.println("<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>"); 
        outfile.println("<movies>"); 

        while (results.next()) {
            String id = results.getString("id");

            String name = results.getString("name");    // 1 = leftmost column

            String year = results.getString("year");

            String rating = results.getString("rating");

            String runtime = results.getString("runtime");

            String genre = results.getString("genre");

            String earnings_rank = results.getString("earnings_rank");

            // get actors
            
            String queryTwo = "SELECT actor_id FROM Actor WHERE movie_id = '"+id+"';";
            ResultSet resultsTwo = stmtTwo.executeQuery(queryTwo);
            String actors = "";
            while (resultsTwo.next()) {
                if(actors.length() != 0) actors += " ";
                actors += "P" + resultsTwo.getString("actor_id");
            }
            if(actors != "") actors = "\n    actors=\"" + actors + "\"";


            // get directors
            String queryThree = "SELECT director_id FROM Director WHERE movie_id = '"+id+"';";
            ResultSet resultsThree = stmtTwo.executeQuery(queryThree);
            String directors = "";
            while (resultsThree.next()) {
                if(directors.length() != 0) directors += " ";
                directors += "P" + resultsThree.getString("director_id");
            }
            if(directors != "") directors = "\n    directors=\"" + directors + "\"";


            // get oscars won
            String queryFour = "SELECT person_id, year,type FROM Oscar WHERE movie_id = '"+id+"';";
            ResultSet resultsFour = stmtTwo.executeQuery(queryFour);
            String oscars = "";
            while (resultsFour.next()) {
                if(oscars.length() != 0) oscars += " ";
                if(!resultsFour.getString("type").equals("BEST-PICTURE")){
                    oscars += "O" + resultsFour.getString("year") + resultsFour.getString("person_id");
                }else{
                    oscars += "O" + resultsFour.getString("year") + "000000";
                }
            }
            if(oscars != "") oscars = "\n    oscars=\"" + oscars + "\"";


            id = "M" + id;
            //outfile.println(name + " " + year + " " +rating + " " +runtime + " " +genre + " "+earnings_rank);
            outfile.println("  <movie id=\"" + id + "\"" + directors + 
                actors + oscars + ">");
            
            if(name != null) outfile.println("    <name>" + name + "</name>"); 
            if(year !=null) outfile.println("    <year>" + year + "</year>"); 
            if(rating != null) outfile.println("    <rating>" + rating + "</rating>"); 
            if(runtime != null) outfile.println("    <runtime>" + runtime + "</runtime>"); 
            if(genre != null) outfile.println("    <genre>" + genre + "</genre>"); 
            
            if(earnings_rank != null){
                outfile.println("    <earnings_rank>" + earnings_rank + "</earnings_rank>"); 
            }
            outfile.println("  </movie>"); 

        }
        outfile.println("</movies>"); 
        // Close the file and the database connection.
        outfile.close();

        System.out.println("movies.xml has been written.");

    }









    private static void makePeopleXML(Connection db) throws ClassNotFoundException, SQLException, FileNotFoundException{

        PrintStream outfile = new PrintStream("people.xml");

        // Create a Statement object and use it to execute the query.
        Statement stmt = db.createStatement();
        String query = "SELECT id, name, dob, pob FROM Person;";
        ResultSet results = stmt.executeQuery(query);
        
        Statement stmtTwo = db.createStatement();
        // Iterate over the tuples in the result and write them to the file.

        outfile.println("<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>"); 
        outfile.println("<people>"); 

        while (results.next()) {
            String id = results.getString("id");

            String name = results.getString("name");    // 1 = leftmost column

            String dob = results.getString("dob");

            String pob = results.getString("pob");



            // get movies acted in 
            
            String queryTwo = "SELECT movie_id FROM Actor WHERE actor_id = '"+id+"';";
            ResultSet resultsTwo = stmtTwo.executeQuery(queryTwo);
            String movies = "";
            while (resultsTwo.next()) {
                if(movies.length() != 0) movies += " ";
                movies += "M" + resultsTwo.getString("movie_id");
            }
            if(movies != "") movies = "\n    actedIn=\"" + movies + "\"";


            // get oscars won by this person
            String queryThree = "SELECT year FROM Oscar WHERE person_id = '"+id+"';";
            ResultSet resultsThree = stmtTwo.executeQuery(queryThree);
            String oscars = "";
            while (resultsThree.next()) {
                if(oscars.length() != 0) oscars += " ";
                oscars += "O" + resultsThree.getString("year") + id;
            }
            if(oscars != "") oscars = "\n    oscars=\"" + oscars + "\"";


            // get movies directed by person
            String queryFour = "SELECT movie_id FROM Director WHERE director_id = '"+id+"';";
            ResultSet resultsFour = stmtTwo.executeQuery(queryFour);
            String directed = "";
            while (resultsFour.next()) {
                if(directed.length() != 0) directed += " ";
                directed += "M" + resultsFour.getString("movie_id");
            }
            if(directed != "") directed = "\n    directed=\"" + directed + "\"";


            id = "P" + id;
            //outfile.println(name + " " + year + " " +rating + " " +runtime + " " +genre + " "+earnings_rank);
            outfile.println("  <person id=\"" + id + "\"" + directed + 
                movies + oscars + ">");
            
            if(name != null) outfile.println("    <name>" + name + "</name>"); 
            if(dob != null) outfile.println("    <dob>" + dob + "</dob>"); 
            if(pob != null) outfile.println("    <pob>" + pob + "</pob>"); 
            
            outfile.println("  </person>"); 

        }
        outfile.println("</people>"); 
        // Close the file and the database connection.
        outfile.close();

        System.out.println("people.xml has been written.");

    }







    private static void makeOscarXML(Connection db) throws ClassNotFoundException, SQLException, FileNotFoundException{

        PrintStream outfile = new PrintStream("oscars.xml");

        // Create a Statement object and use it to execute the query.
        Statement stmt = db.createStatement();
        String query = "SELECT movie_id, person_id, type, year FROM Oscar;";
        ResultSet results = stmt.executeQuery(query);
        
        Statement stmtTwo = db.createStatement();
        // Iterate over the tuples in the result and write them to the file.

        outfile.println("<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>"); 
        outfile.println("<oscars>"); 

        while (results.next()) {
            String movie_id = results.getString("movie_id");

            String person_id = "P" + results.getString("person_id");    // 1 = leftmost column

            String type = results.getString("type");

            String year = results.getString("year");


            String id = "O" + year + (type.equals("BEST-PICTURE") ? "0000000" : person_id);


            person_id = (type.equals("BEST-PICTURE") ? "" : "person_id=\"" + person_id + "\"");

            
            //outfile.println(name + " " + year + " " +rating + " " +runtime + " " +genre + " "+earnings_rank);
            outfile.println("  <oscar id=\"" + id + "\" movie_id=\"M" + movie_id + "\" " + person_id +">");
            outfile.println("    <type>" + type + "</type>"); 
            outfile.println("    <year>" + year + "</year>"); 
            outfile.println("  </oscar>"); 

        }
        outfile.println("</oscars>"); 
        // Close the file and the database connection.
        outfile.close();

        System.out.println("oscars.xml has been written.");

    }








    public static void main(String[] args) 
      throws ClassNotFoundException, SQLException, FileNotFoundException
    {
        Scanner console = new Scanner(System.in);
        
        // Connect to the database.
        System.out.print("name of database file: ");
        String db_filename = console.next();
	    Class.forName("org.sqlite.JDBC");
        Connection db = DriverManager.getConnection("jdbc:sqlite:" + db_filename);
               
        // Create a PrintStream for the results file.
        //System.out.print("name of results file: ");
        //String out_filename = console.next();
        

       
        makeMoviesXML(db);

        makePeopleXML(db);

        makeOscarXML(db);





        db.close();
    }
}