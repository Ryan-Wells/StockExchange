import java.util.*;
import java.io.*;
import java.lang.Math;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;

public class StockExchange{

   //static variables
   static Connection readerConn = null;
   static Connection writerConn = null; 
   static final String dropPerformanceTable = "drop table if exists Performance; ";
   static final String createPerformanceTable = "create table Performance (Industry char(30), Ticker char(6), StartDate char(10), EndDate char(10), TickerReturn char(12), IndustryReturn char(12)); ";
   static final String insertPerformance = "insert into Performance(Industry, Ticker, StartDate, EndDate, TickerReturn, IndustryReturn)  values(?, ?, ?, ?, ?, ?); ";
   static final int date = 0;
   static final int opening = 1;
   static final int closing = 2;
   static final int name = 3;
   static final int low = 4;
   static final int high = 5;
   public static int numSplits = 0;
      

   public static void main(String[] args) throws ClassNotFoundException, IOException{
    
      String ReadParamsFile = "readerparams.txt";       
      String WriteParamsFile = "writerparams.txt";
      if (args.length >= 1) { 
         ReadParamsFile = args[0]; 
         WriteParamsFile = args[1];
      }
      Properties readConnectProps = new Properties();
      Properties writeConnectProps = new Properties();
      
      FileInputStream readFile = new FileInputStream(ReadParamsFile); 
      FileInputStream writeFile = new FileInputStream(WriteParamsFile);
      readConnectProps.load(readFile);
      writeConnectProps.load(writeFile);
      
      try {
         Class.forName("com.mysql.jdbc.Driver");                           
         String read_db_url = readConnectProps.getProperty("dburl");
         String write_db_url = writeConnectProps.getProperty("dburl"); 
         String username = readConnectProps.getProperty("user"); 
         readerConn = DriverManager.getConnection(read_db_url, readConnectProps);     
         System.out.println("Reader connection established");
         writerConn =  DriverManager.getConnection(write_db_url, writeConnectProps);
         System.out.println("Writer connection established");
         
         createTable();         
         execute();
      
         readerConn.close();
         writerConn.close();
         
         System.out.println("Database connection closed.");
      } 
      catch (SQLException ex) {                                       
         System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n", ex.getMessage(), ex.getSQLState(), ex.getErrorCode());     
      }
   }

 
   //creates the table in the database (drops it first, if it needs to)
   static void createTable() throws SQLException{
      PreparedStatement pstmt;
      pstmt = writerConn.prepareStatement(dropPerformanceTable);  
      pstmt.execute();
      
      pstmt = writerConn.prepareStatement(createPerformanceTable);
      pstmt.execute();
   }
   
      
   //first big part of the program.
   static void execute() throws SQLException{
      ArrayList<String> industries = findIndustries();
      ArrayList<String> tickers;
      System.out.printf("%d industries found\n", industries.size());
   
      for(int i = 0; i < industries.size(); i++){
         System.out.println(industries.get(i));
      }
      
      for(int i = 0; i < industries.size(); i++){
         String industry = industries.get(i);
         try{
         
            ResultSet rs = makeResultSet(industry);      
         
            String minDate = findMin(industry);
            String maxDate = findMax(industry);
            tickers = getTickerList(rs);
            int count = 1;
            System.out.printf("\nprocessing %s\n", industry);
            int size = tickers.size();
            
            ArrayList<ArrayList<String>> intervals = doIntervals(industry, tickers.get(0), minDate, maxDate);
            for(int j = 0; j < tickers.size(); j++){
            
               String ticker = tickers.get(j);              
               for(int k = 0; k < intervals.size() - 1; k++){
                  String intervalStart = intervals.get(k).get(0);
                  String nextIntervalStart = intervals.get(k + 1).get(0);
                  String intervalEnd = intervals.get(k).get(1);
               
                  String tickerReturn = tickerReturn(ticker, intervalStart, nextIntervalStart); 
               
                  String industryReturn = industryReturn(industry, intervalStart, nextIntervalStart, ticker);
                  count++;
                
                  doInsert(industry, ticker, intervalStart, intervalEnd, tickerReturn, industryReturn);  
               }
            }
            System.out.printf("%d accepted tickers for %s(%s - %s), %d common days\n", size, industry, minDate, maxDate, count);
         }        
             
         //if there is insufficient information, we need to prevent a crash.
         catch(SQLException e){
            System.out.printf("Insufficient data for %s => no analysis\n\n", industry);
         }
      }
   }
     
   
   //find the maximum date in a given industry
   static String findMin(String industry) throws SQLException {
   
      String result = null;
      
      PreparedStatement pstmt = readerConn.prepareStatement(                            
         "select Ticker, min(TransDate) as min, max(TransDate) as max, count(distinct TransDate) as TradingDays " +
         "  from Company natural left outer join PriceVolume " +
         "  where Industry = ? " +
         "  group by Ticker " +
         "  having TradingDays >= 150 " +
         "  order by min(TransDate) desc; " );
      
      pstmt.setString(1, industry);
      
      ResultSet rs = pstmt.executeQuery();
      
      if(rs.next()){
         result = rs.getString(2);       
      }
      return result;
   }
      
   
   //find the minimum date in a given industry
   static String findMax(String industry) throws SQLException {
   
      String result = null;
      PreparedStatement pstmt = readerConn.prepareStatement(                            
         "select Ticker, min(TransDate) as min, max(TransDate) as max, count(distinct TransDate) as TradingDays " +
         "  from Company natural left outer join PriceVolume " +
         "  where Industry = ? " +
         "  group by Ticker " +
         "  having TradingDays >= 150 " +
         "  order by max(TransDate); ");
      
      pstmt.setString(1, industry);
      ResultSet rs = pstmt.executeQuery();
      if(rs.next()){
         result = rs.getString(3);  
      }
      return result;
   }


   //put the industries into an arrayList
   static ArrayList<String> findIndustries() throws SQLException{
      ArrayList<String> industries = new ArrayList<String>();
      PreparedStatement pstmt = readerConn.prepareStatement("select Industry from company natural join pricevolume group by Industry; ");
      ResultSet rs = pstmt.executeQuery();
      while (rs.next()){
         industries.add(rs.getString(1));      
      }
      return industries;
   }
   
   
   //put the tickers into an arrayList
   static ArrayList<String> getTickerList(ResultSet rs) throws SQLException{
      ArrayList<String> tickers = new ArrayList<String>();
      try{
         while(rs.next()){
            tickers.add(rs.getString(1));
         }
      }
      catch(SQLException e){
         System.out.println("Error occurred while creating ticker list."); 
      }
      return tickers;
   }
   
   
   //inserts our values into the table in the database
   static void doInsert(String industry, String ticker, String startDate, String endDate, String tickerReturn, String industryReturn) throws SQLException{
      
      PreparedStatement pstmt = writerConn.prepareStatement(insertPerformance);
      pstmt.setString(1, industry);
      pstmt.setString(2, ticker);
      pstmt.setString(3, startDate);
      pstmt.setString(4, endDate);
      pstmt.setString(5, tickerReturn);
      pstmt.setString(6, industryReturn); 
      
      pstmt.execute();
      
   }
      
   
   //performs our split calculations with the multiplier, returns the arrayList from the resultSet
   static ArrayList<String[]> doSplits(ResultSet rs) throws SQLException{
      int condA = 0;
      int condB = 0;
      String[] number1 = new String[10];
      String[] number2 = new String[10];
      double multiplier = 1.0;
      double split;
      
      ArrayList<String[]> data = new ArrayList<String[]>();
         
      while (rs.next()){      
         if(condA == 0){ 
            intoArray(rs, number1);
            condA = 1;
            if(condB == 1){
               if((split = splitCheck(Double.parseDouble(number1[closing]), Double.parseDouble(number2[opening]), number1[date])) != 0){
                  multiplier *= split;
               }
            }
            addRecord(number1, multiplier, data);  
           
         }
         else{
            intoArray(rs, number2);
            condB = 1;
            condA = 0;
            if((split = splitCheck(Double.parseDouble(number2[closing]), Double.parseDouble(number1[opening]), number2[date])) != 0){
               multiplier *= split;            
            }
            addRecord(number2, multiplier, data);
               
         }
      }       
      return data;       
   }
    

   //update everything with splits (used in doSplits)
   static void addRecord(String[] list, double multiplier, ArrayList<String[]> data) {
      
      String[] newList = new String[10];
      newList[0] = list[0];
      newList[3] = list[3];
      double num = Double.parseDouble(list[1]);
      newList[1] = Double.toString(num / multiplier);
      num = Double.parseDouble(list[2]);
      newList[2] = Double.toString(num / multiplier);
     
      data.add(newList);
   }

   
   //our intervals
   static ArrayList<ArrayList<String>> doIntervals(String industry, String ticker, String minDate, String maxDate) throws SQLException{  
   
      //an arrayList of arrayLists to store our intervals
      ArrayList<ArrayList<String>> intervals = new ArrayList<ArrayList<String>>();
      ArrayList<String> days = new ArrayList<String>();
      PreparedStatement pstmt = readerConn.prepareStatement(
         "select P.TransDate " +
         "  from company natural left outer join PriceVolume P " +
         "  where Industry = ? " +
         "  and Ticker = ? " +
         "  and (TransDate >= ? " +
         "  and TransDate <= ?) " +
         "  order by TransDate; ");
         
      pstmt.setString(1, industry);
      pstmt.setString(2, ticker);
      pstmt.setString(3, minDate);
      pstmt.setString(4, maxDate);
      
      ResultSet rs = pstmt.executeQuery();
      
      //for everything in the interval
      int i = 0;
      while(rs.next()){
         if(i == 0){
            days =new ArrayList<String>();
            days.add(rs.getString(1));
            i++;
         }
         else if(i == 59){
            days.add(rs.getString(1));  
            intervals.add(days);
            i = 0;
         }
         else{
            i++;
         }
      }
      
      rs.previous();
      days.add(rs.getString(1));      
      intervals.add(days);
      return intervals;
   }   
     
  
   //creates our resultSet for our industry
   static ResultSet makeResultSet(String industry) throws SQLException{
   
      PreparedStatement pstmt = readerConn.prepareStatement(                            
         "select Ticker, min(TransDate) as min, max(TransDate) as max, count(distinct TransDate) as TradingDays " +
         "  from Company natural left outer join PriceVolume " +
         "  where Industry = ? " +
         "  group by Ticker " +
         "  having TradingDays >= 150 " +
         "  order by Ticker; ");
      pstmt.setString(1, industry);
      ResultSet rs = pstmt.executeQuery();
      return rs;
   }
   
      
   //calculate a ticker's return
   static String tickerReturn(String ticker, String minDate, String maxDate) throws SQLException{
      PreparedStatement pstmt = readerConn.prepareStatement(
         "select P.TransDate, P.openPrice, P.closePrice, Ticker " +
         "  from PriceVolume P " +
         "  where Ticker = ? " +
         "  and (TransDate >= ? " +
         "  and TransDate < ?) " +
         "  order by TransDate; ");
         
      pstmt.setString(1, ticker);
      pstmt.setString(2, minDate);
      pstmt.setString(3, maxDate);
      ResultSet rs = pstmt.executeQuery();
      ArrayList<String[]> data = doSplits(rs);
      double open = Double.parseDouble(data.get(0)[opening]);
      double close = Double.parseDouble(data.get(data.size()-1)[closing]);
      double retval = (close / open) - 1;
      return String.format("%10.7f", retval);
   }

      
   //calculate an industry's return
   static String industryReturn(String industry, String minDate, String maxDate, String ticker) throws SQLException{
      double tickerNum = 0.0;
      double sum = 0.0;
      double retval;
      String thisTicker;
      PreparedStatement pstmt = readerConn.prepareStatement(
         "select P.TransDate, P.openPrice, P.closePrice, P.Ticker " +
         "  from PriceVolume P natural join Company " +
         "  where Industry = ? " +
         "  and (TransDate >= ? " +
         "  and TransDate < ?) " +
         "  and Ticker != ? " +
         "  order by Ticker, TransDate; ");
         
      pstmt.setString(1, industry);
      pstmt.setString(2, minDate);
      pstmt.setString(3, maxDate);
      pstmt.setString(4, ticker);
       
      ResultSet rs = pstmt.executeQuery();
      
      ArrayList<String[]> data = doSplits(rs);
      
      double open = Double.parseDouble(data.get(0)[opening]);
      
      thisTicker = data.get(0)[name];     
      
      double close;
      for(int i = 0; i < data.size(); i++){
         if(!data.get(i)[name].equals(thisTicker)){
            close = Double.parseDouble(data.get(i-1)[closing]);
            sum += (close / open);
            tickerNum++;
            thisTicker = data.get(i)[name];
            open = Double.parseDouble(data.get(i)[opening]);
         }
      }
      close = Double.parseDouble(data.get(data.size() - 1)[closing]);
      sum += (close / open);
      tickerNum++;
      double fraction = (1 / tickerNum);
      retval = (fraction * sum )-1;
      
      return String.format("%10.7f", retval);
   }
   
    
   //generate a ResultSet
   static ResultSet generateResultSet(String ticker, String date, String end_date) throws SQLException{
      PreparedStatement pstmt;
      if (!(date == null)){
         pstmt = readerConn.prepareStatement(                            
            "select Ticker, TransDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, AdjustedClose " +
            "  from PriceVolume " +
            "  where Ticker = ? and TransDate >= ? and TransDate <= ? ORDER BY TRANSDATE DESC; ");
         pstmt.setString(1, ticker);                                                 
         pstmt.setString(2, date);
         pstmt.setString(3, end_date);
      }
      else{
         pstmt = readerConn.prepareStatement(                            
            "select Ticker, TransDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, AdjustedClose " +
            "  from pricevolume " +
            "  where Ticker = ? order by TransDate DESC; ");
         pstmt.setString(1, ticker);              
      }
      ResultSet rs = pstmt.executeQuery();
      return rs;
   }
   
   
   //simply adds a row into the given array
   static void intoArray(ResultSet rs, String[] array) throws SQLException{
      array[0] = rs.getString(1);
      array[1] = Double.toString(rs.getDouble(2));
      array[2] = Double.toString(rs.getDouble(3));
      array[3] = rs.getString(4);
   }


   //checks for splits, returns our multiplier
   public static double splitCheck(double close, double open, String date){
      double split = close/open;
      double returnSplit = 0;
      if(Math.abs(split-2.0) <.20){
         returnSplit = 2;
      }
      if(Math.abs(split-3.0)<.30){
         returnSplit = 3;
      }
      if(Math.abs(split-1.5) <.15){
         returnSplit = 1.5;
      }
      return returnSplit;
   }   

}