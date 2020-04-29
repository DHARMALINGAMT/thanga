import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Scanner;

//jdbc:oracle:thin:@p1dbewii.elsevier.com:1521/pewii.elsevier.co.uk
//jdbc:oracle:thin:@ddbewii.elsevier.com:1521/ewii
public class ConnectionRetry {
    private static final String driverName="oracle.jdbc.driver.OracleDriver";
    private static final String dbConnection="jdbc:oracle:thin:@p1dbewii.elsevier.com:1521/pewii.elsevier.co.uk";
    private static final String lineSeperator=System.lineSeparator();
    static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd_MM_yyyy_HH_mm_ss");
    static LocalDateTime now = LocalDateTime.now();


    private static final String connectionResetErrorQuery="select child_itm.item_type, \n" +
            "        child_itm.item_key , \n" +
            "        wf.user_key, \n" +
            "        wf.begin_date,\n" +
            "        ias.process_activity,\n" +
            "        ias.activity_status,\n" +
            "        pa.activity_name\n" +
            "      from wf_items wf \n" +
            "      ,    wf_item_activity_statuses ias\n" +
            "      ,    wf_process_activities     pa \n" +
            "      ,    wf_items child_itm\n" +
            "      where wf.item_type = 'EW_OUT' \n" +
            "      and   wf.root_activity = 'OUT_P'\n" +
            "      and   ias.item_type = child_itm.item_type\n" +
            "      and   ias.item_key = child_itm.item_key\n" +
            "      and   child_itm.parent_item_type = wf.item_type\n" +
            "      and   child_itm.parent_item_key = wf.item_key\n" +
            "      and   ias.activity_status = 'NOTIFIED'\n" +
            "      and   ias.end_date is null\n" +
            "      and   ias.process_activity = pa.instance_id\n" +
            "      and   ias.item_type        = pa.process_item_type\n" +
            "      and   pa.activity_name = 'ERR_INV_NOT'\n" +
            "      and wf.item_key in (\n" +
            "            select wf.item_key from eew_error_details err, wf_items wf \n" +
            "            where wf_item_type='EW_OUT' and trunc(err.creation_date)>='16-Sep-2019'\n" +
            "            and wf.item_key=err.wf_item_key and wf.item_type=err.wf_item_type \n" +
            "            and  wf.end_date is null and (\n" +
            "                    err.error_text like '%onnection reset%' or\n" +
            "                    err.error_text like '%timed out%' or \n" +
            "                    err.error_text like '%onnection refused%' \n" +
            "                    or err.error_text like '%ipe closed%'\n" +
            "                                        )\n" +
            "                        )";
    private static final String connectionrest_RetryQuery = "DECLARE\n" + "\n" + "      l_count NUMBER;\n" + "\n"
            + "BEGIN\n" + "\n" + "     l_count:=0;\n" + "\n"
            + "     DBMS_OUTPUT.PUT_LINE('Starting script to retry error Workflow');\n" + "\n" + "     FOR r_ias IN (\n"
            + "  select child_itm.item_type, \n" + "        child_itm.item_key , \n" + "        wf.user_key, \n"
            + "        wf.begin_date,\n" + "        ias.process_activity,\n" + "        ias.activity_status,\n"
            + "        pa.activity_name\n" + "      from wf_items wf \n" + "      ,    wf_item_activity_statuses ias\n"
            + "      ,    wf_process_activities     pa \n" + "      ,    wf_items child_itm\n"
            + "      where wf.item_type = 'EW_OUT' \n" + "      and   wf.root_activity = 'OUT_P'\n"
            + "      and   ias.item_type = child_itm.item_type\n" + "      and   ias.item_key = child_itm.item_key\n"
            + "      and   child_itm.parent_item_type = wf.item_type\n"
            + "      and   child_itm.parent_item_key = wf.item_key\n" + "      and   ias.activity_status = 'NOTIFIED'\n"
            + "      and   ias.end_date is null\n" + "      and   ias.process_activity = pa.instance_id\n"
            + "      and   ias.item_type        = pa.process_item_type\n"
            + "      and   pa.activity_name = 'ERR_INV_NOT'\n" + "      and wf.item_key in \n"
            + "      (select wf.item_key from eew_error_details err, wf_items wf \n"
            + "            where wf_item_type='EW_OUT' \n" + "            and trunc(err.creation_date)>='16-Sep-2019'\n"
            + "            and wf.item_key=err.wf_item_key \n" + "            and wf.item_type=err.wf_item_type \n"
            + "            and  wf.end_date is null \n" + "            and (\n"
            + "                    err.error_text like '%onnection reset%' or\n"
            + "                    err.error_text like '%timed out%' or \n"
            + "                    err.error_text like '%onnection refused%' or \n"
            + "                    err.error_text like '%ipe closed%'))\n" + "     )\n" + "\n" + "     LOOP\n" + "\n"
            + "        wf_engine.completeactivity( itemtype => r_ias.item_type\n"
            + "                                   , itemkey  => r_ias.item_key\n"
            + "                                   , activity =>  wf_engine.getactivitylabel(actid=>r_ias.process_activity)\n"
            + "                                   , result   => 'RETRY');\n" + "         l_count:=l_count+1;\n"
            + "        \n" + "         \n" + "          commit;\n" + "\n"
            + "         DBMS_OUTPUT.PUT_LINE(' WF Retried on user key=> '||r_ias.user_key||' itemkey=>'||r_ias.item_key||' actid=> '||TO_CHAR(r_ias.process_activity));\n"
            + "\n" + "     END LOOP;\n" + "END;\n";
    private static final String hangingWorkflow = "  select wf.item_type, wf.item_key,wf.begin_date\n" +
            "            from wf_items wf \n" +
            "            , wf_item_activity_statuses ias\n" +
            "            , wf_process_activities     pa \n" +
            "           where wf.end_date is null\n" +
            "           and   ias.item_type = wf.item_type\n" +
            "           and   ias.item_key = wf.item_key\n" +
            "           and   ias.process_activity = pa.instance_id\n" +
            "           and   ias.item_type        = pa.process_item_type\n" +
            "           and   ias.activity_status  <> 'COMPLETE'\n" +
            "           and   ias.end_date is null\n" +
            "           and   pa.activity_name not in ('WAITFORFLOW','BLOCK','WAIT')         -- parent activity not in  states like wait\n" +
            "           and   wf.begin_date > sysdate-55\n" +
            "           and   regexp_like (pa.activity_name,'_[RS]$')\n" +
            "           and not exists (select 1 \n" +
            "                           from wf_items wf_chl\n" +
            "                           where wf_chl.parent_item_key = wf.item_key\n" +
            "                           and wf_chl.item_type in ('EW_ERR','WFERROR'))        --  No child error wfs\n" +
            "           and wf.item_key not in (select xmltype(t.user_data.data).            --  parent wf not in task queue\n" +
            "                                   extract('/Task/WFItemKey/text()').getstringval() Key\n" +
            "                                   from   eew_queue_table t\n" +
            "                                   where  state<>2\n" +
            "                                   and    t.q_name='EEW_TASKS')\n" +
            "           order by  wf.item_type,wf.begin_date";

    private static File fileCreation() throws Exception{
        String strDate = dtf.format(now);
        File file = new File("D:/Users/DHARMALINGAMT/Desktop/Connection_retry/"+strDate+"_Connection_retry.txt");
        if(!file.exists()){
            file.createNewFile();
            System.out.println("File Created");
        }else{
            System.out.println("File Already availble");
        }

        return file;
    }
    public void connectionResetError(String dbUsername,String dbPassword) throws Exception{
        Class.forName(driverName);
        Connection con = DriverManager.getConnection(dbConnection,dbUsername,dbPassword);
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(connectionResetErrorQuery);
        FileWriter fileWriter = new FileWriter(fileCreation(),true);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        ArrayList<String> connectionResetError = new ArrayList<>();
        bufferedWriter.write("ITEM_TYPE	ITEM_KEY	USER_KEY	BEGIN_DATE	        PROCESS_ACTIVITY	     ACTIVITY_STATUS  ACTIVITY_TIME"+lineSeperator);
        bufferedWriter.write("----------------------------------------------------------------------------------------------------------------"+lineSeperator);
        int resetErrorCount = 0;
        while(rs.next()){
            connectionResetError.add(rs.getString("ITEM_TYPE")+"     "+rs.getString("ITEM_KEY")+"     "+rs.getString("USER_KEY")+"     "	+rs.getString("BEGIN_DATE")+"     "+rs.getInt("PROCESS_ACTIVITY")+"                   "+rs.getString("ACTIVITY_STATUS")+"     "+rs.getString("ACTIVITY_NAME")+lineSeperator);
            bufferedWriter.write(rs.getString("ITEM_TYPE")+"     "+rs.getString("ITEM_KEY")+"     "+rs.getString("USER_KEY")+"     "	+rs.getString("BEGIN_DATE")+"     "+rs.getInt("PROCESS_ACTIVITY")+"                   "+rs.getString("ACTIVITY_STATUS")+"     "+rs.getString("ACTIVITY_NAME")+lineSeperator);
            resetErrorCount++;
            rs.next();
        }
        System.out.println(connectionResetError.size()+" error count");
        System.out.println(resetErrorCount);
        bufferedWriter.close();
        /*if(resetErrorCount > 0){
            connectionrestRetry(dbUsername,dbPassword);
            connectionResetError(dbUsername,dbPassword);
        }*/
        System.out.println("Completed");
        con.close();
    }
    private static final void connectionrestRetry(String dbUsername,String dbPassword) throws Exception{
        Class.forName(driverName);
        Connection con = DriverManager.getConnection(dbConnection,dbUsername,dbPassword);
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(connectionrest_RetryQuery);
    }
    private static File hangingWorkflowFileCreation() throws Exception{
        String strDate = dtf.format(now);
        File file = new File("D:/Users/DHARMALINGAMT/Desktop/Connection_retry/Hanging_Workflow/"+strDate+"_Hanging_Workflow.txt");
        if(!file.exists()){
            file.createNewFile();
            System.out.println("File Created");
        }else{
            System.out.println("File Already availble");
        }

        return file;
    }
    public void hangingWorkflow(String dbUsername,String dbPassword) throws Exception{
        Class.forName(driverName);
        Connection con = DriverManager.getConnection(dbConnection,dbUsername,dbPassword);
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(hangingWorkflow);
        FileWriter hangingFileWriter = new FileWriter(hangingWorkflowFileCreation());
        BufferedWriter hangingBufferedWriter = new BufferedWriter(hangingFileWriter);
        hangingBufferedWriter.write("ITEM_TYPE\tITEM_KEY\t\t\tBEGIN_DATE"+lineSeperator);
        hangingBufferedWriter.write("-----------------------------------------------"+lineSeperator);
        while(rs.next()){
            hangingBufferedWriter.write(rs.getString("ITEM_TYPE")+"\t\t"+rs.getString("ITEM_KEY")+"\t\t"+rs.getString("BEGIN_DATE")+lineSeperator);
            //System.out.println(rs.getString("ITEM_TYPE")+"\t\t"+rs.getString("ITEM_KEY")+"\t\t"+rs.getString("BEGIN_DATE"));
        }
        hangingBufferedWriter.close();
        con.close();

    }
    public static void main(String args[]) throws Exception{
        Scanner s = new Scanner(System.in);
        String dbUsername,dbPassword;
        System.out.println("Enter DBUsername");
        dbUsername = s.nextLine();
        System.out.println("Enter DBPassword");
        dbPassword = s.nextLine();
       ConnectionRetry connectionRetry = new ConnectionRetry();
       connectionRetry.connectionResetError(dbUsername,dbPassword);
       //connectionRetry.hangingWorkflow();

    }

}
