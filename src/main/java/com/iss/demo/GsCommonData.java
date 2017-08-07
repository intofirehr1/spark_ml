package com.iss.demo;

import com.iss.util.CreateIDCardNo;
import com.iss.util.Utils;

import java.io.*;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by hadoop on 2017/8/3 0003.
 * 基础数据
 */
public class GsCommonData {
    public static void main(String[] args) {
        // 构造用户信息，
        /** 用户信息结构： id, 姓名, 年龄, 性别, 证件类别, 证件号码, 工作地, 婚姻状况, 学历 */
        /** 保险信息： id, 姓名, 证件类别, 证件号码, 险种, 金额, 客户价值, 对保险认知程度, 投保渠道*/

        /** 统一信息： id, user_name, user_age, user_sex, card_type, card_no, user_address, marital, education
         * , insurance_type, accord, custom_level, insurance_level, insurance_channel*/

        for (int i = 1; i < 13; i++) {
            File file = new File("E:\\isoftstone\\data\\test_10-20_better1\\2017\\gs_data"+i+".txt");
            FileWriter fw = null;
            try {
                if (!file.exists()) {
                    System.out.println("没有这个文件，创建中请稍等。。。");
                    file.createNewFile();
                }
                fw = new FileWriter(file.getAbsoluteFile());
//            fw.write(getContent(100));
//            fw.write(getContentAll(1));
                // 训练数据
//                fw.write(getAllContents(10000, i, 100000*i));
                // 测试数据
            fw.write(getTestContents(1000,i,100000*i));

                fw.flush();
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (null != fw) {
                        fw.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


//        for(int i=1; i<100 ; i++){
//            System.out.println(getTransStat());
//        }

    }


    /**
     * 数据中只有用户基本信息，TODO 需要和银行的数据进行结合取并集
     *
     * @return
     */
    public static String getContent(int length) {
        StringBuffer content = new StringBuffer();
        for (int i = 0; i < length; i++) {
            content.append(i).append("\t");  // id
            String[] message = Utils.getUserCord();
            content.append(message[0]).append("\t");  // name
            content.append(Utils.getRandomNumber(2)).append("\t");  // user_age
            content.append(Utils.getNumber3()).append("\t");  // user_sex
            content.append(message[1]).append("\t");  // card_type
            content.append(message[2]).append("\t");  // card_no
            content.append(Utils.getAddress()).append("\t");  // user_address
            content.append(Utils.getNumber1()).append("\t");  // marital 婚姻状况以0 未婚， 1 已婚  代替；
            // 如果实际情况是汉字，需要进行分类
            content.append(Utils.getEducation()).append("\t");  // education
            // 职业
            content.append(Utils.getProfession()).append("\t");  // education
            // 行业
            content.append(Utils.getVocation()).append("\t");  // education

            content.append(Utils.getInsuranceType()).append("\t");  // insurance_type
            content.append(Utils.getRandomNumber(3) + ".00").append("\t");  // accord
            content.append(Utils.getCustomLevel()).append("\t");  // custom_level
            content.append(Utils.getInsuranceLevel()).append("\t");  // insurance_level
            content.append(Utils.getNumber1());  // insurance_channel
            content.append("\r\n");
        }

        return content.toString();
    }





    public static String getContentAll(int length) {
        StringBuffer content = new StringBuffer();
        for (int i = 0; i < length; i++) {
            // TODO 读取银行信息的数据, 之后根据这个信息进行数据构造
            File file = new File("data/gs_train_src.txt");
            BufferedReader reader = null;
            try {
                System.out.println("以行为单位读取文件内容，一次读一整行：");
                reader = new BufferedReader(new FileReader(file));
                String tempString = null;
                int line = 1;
                // 一次读入一行，直到读入null为文件结束
                while ((tempString = reader.readLine()) != null) {
                    // 行数据中，根据用户名，身份等信息构造之后信息
                    // 数据中需要做些处理，如让所有
                    String [] linearray = tempString.split("\t");
                    content.append(tempString).append("\t");

                    content.append(Utils.getRandomNumber(2)).append("\t");  // user_age
                    if(Integer.parseInt(linearray[4]) > 1){
                        // 失败的数据
                        content.append("0").append("\t");  // user_sex
                    }else {
                        //content.append("1").append("\t");  // user_sex
                        content.append(Utils.getNumber3()).append("\t");  // user_sex
                   }
                    if(Integer.parseInt(linearray[4]) > 1){
                        content.append("Tian jin").append("\t");  // user_address
                    } else {
                        content.append(Utils.getAddress()).append("\t");  // user_address
                    }

                    if(Integer.parseInt(linearray[4]) > 1){
                        content.append("1").append("\t");  // marital 婚姻状况以2 未婚， 1 已婚  代替；
                    }else {
                        //content.append(Utils.getNumber1()).append("\t");  // marital 婚姻状况以0 未婚， 1 已婚  代替；
                        content.append(Utils.getNumber1()).append("\t");  // marital 婚姻状况以0 未婚， 1 已婚  代替；
                    }

                    // 如果实际情况是汉字，需要进行分类
                    content.append(Utils.getEducation()).append("\t");  // education
                    // 职业
                    content.append(Utils.getProfession()).append("\t");  // education
                    // 行业
                    content.append(Utils.getVocation()).append("\t");  // education

                    content.append(Utils.getInsuranceType()).append("\t");  // insurance_type
                    content.append(Utils.getRandomNumber(3) + ".00").append("\t");  // accord
                    if(Integer.parseInt(linearray[4]) == 1) {
                        content.append("1").append("\t");  // custom_level
                        content.append("1").append("\t");  // insurance_level
                        content.append("0");  // insurance_channel
                    } else {
                        content.append(Utils.getCustomLevel()).append("\t");  // custom_level
                        content.append(Utils.getInsuranceLevel()).append("\t");  // insurance_level
                        content.append(Utils.getNumber1());  // insurance_channel
                    }


                   // getBasicMess(content);
                    content.append("\r\n");
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e1) {
                    }
                }
            }
        }

        return content.toString();
    }

    /**
     * 数据生成
     * @param length
     */
    public static String getTestContents(int length, int month, int start){
        // 规则，需要对41个字段规则进行处理
        // MIOS_TRANS_CODE 序号 自增就可以，
        // SYS_NO 系统编号 Q 1 2 3 4 5 6 7 8 9
        // TRANS_CODE
        int count =1;
        StringBuffer sb = new StringBuffer();
        for(int i=1; i<length ; i++) {
            // 随机生成一个用户的记录条数
            int records = 2;
            // 生成一个用户的唯一数据
            String name = getHan();

            String bankNo = Utils.getRandomNumber() +"";

            String zjhm = getRandomCardNo();
            int dateYear = 2017;
            String dateMonth = month>9 ? month+"": "0"+month;
            int dateDay = 0;
            String transBatNo = getTransBatNo();
            int random = (int)(Math.random() * (10));
            for(int j=1;j<records;j++){

                String transStat = getTransStatTest();
                if(!transStat.equals("1")){
                    transStat = "0";
                }
//                transStat = "1";
//                if(j == records-1){
//                    transStat =  "1";
//                } else {
//                    if(transStat.equals("1")){
//                        transStat = (Integer.parseInt(transStat)+1)+"";
//                    }
//                }
//                transStat = "0";

                // 无付费
                sb.append(count).append("\t");  // MIOS_TRANS_CODE
                sb.append("1").append("\t"); // SYS_NO
                sb.append(start + count).append("\t"); //TRANS_CODE
                sb.append(getTransBatNo()).append("\t"); // TRANS_BAT_NO  10411  10080 56 10082(付费) 10060
                sb.append(transStat).append("\t"); // TRANS_STAT
                if ("10082".equals(transBatNo)) {
                    sb.append("42").append("\t");
                    sb.append(start + count).append("\t");
                    sb.append("850099030130").append("\t"); // 付
                    sb.append("1102").append("\t"); // BANK_CODE
                } else {
                    sb.append("41").append("\t"); // TRANS_BAT_SEQ
                    sb.append("0").append("\t"); // PLNMIO_REC_ID
                    sb.append("850099020110").append("\t"); // BRANCH_BANK_ACC_NO 收

                    if ("10080".equals(transBatNo)) {
                        sb.append("1102").append("\t"); // BANK_CODE 1103 1102
                    } else if ("10060".equals(transBatNo)) {
                        sb.append("1103").append("\t"); // BANK_CODE 1103 1102
                    } else {
                        sb.append("1104").append("\t"); // BANK_CODE 1103 1102
                    }
                }
                sb.append("").append("\t");
                sb.append("").append("\t");
                sb.append("").append("\t");
                sb.append("").append("\t");
                sb.append("").append("\t");

                // 银行账户
                sb.append(bankNo).append("\t");

                // 姓名
                sb.append(name).append("\t");

                sb.append("I").append("\t"); // 证件类型

                sb.append(zjhm).append("\t");  // 证件号码

                sb.append(getMgrBranchNo()).append("\t"); // 管理机构

                sb.append(getRandomNumber25()).append("\t"); // CNTR_NO 合同号
                sb.append("0").append("\t");

                sb.append("C").append("\t"); // F
                if ("10082".equals(transBatNo)) {
                    sb.append("-1").append("\t"); // mio_class
                    if (i % 100 == 0) {
                        sb.append("RS").append("\t");
                    } else {
                        sb.append("EC").append("\t");
                    }
                } else {
                    sb.append("1").append("\t"); // mio_class
                    sb.append("PS").append("\t");
                }

                int ds = 10 + (int)(Math.random() * (10 - 0));

                int ds2 = (int)(Math.random() * (4));
                if("1".equals(transStat)){
//                    if(count%10==0){
                        sb.append(dateYear+"-"+dateMonth+"-"+(10 + ds2 )).append("\t");
//                    }else {
//                        sb.append(dateYear+"-"+dateMonth+"-"+((dateDay+ds)>9?(dateDay+ds):"0"+(dateDay+ds))).append("\t");
//                    }
                }else {
                    //sb.append(dateYear+"-"+dateMonth+"-"+(dateDay+ds)).append("\t");
                    sb.append(dateYear+"-"+dateMonth+"-"+((dateDay+ds)>9?(dateDay+ds):"0"+(dateDay+ds))).append("\t");
                }
                // 应付日期，实付日期

                sb.append(dateYear+"-"+dateMonth+"-"+(dateDay+10)).append("\t");

                sb.append(getRandomDouble()).append("\t");

                sb.append(getRandomNumber25()).append("\t"); // CUST_NO

                sb.append(dateYear+"-"+dateMonth+"-"+(dateDay+20)).append("\t");  // greatDate

                sb.append(count).append("\t");  // unit_trans_code

                sb.append(getRandomNumber25()).append("\t");
                sb.append("").append("\t");
                sb.append("").append("\t");
                sb.append("").append("\t");
                sb.append("").append("\t");

                if ("1".equals(transStat)) {
                    sb.append("0000").append("\t");
                    sb.append("处理成功 ").append("\t");
                } else if ("2".equals(transStat)) {
                    sb.append("3008").append("\t");
                    sb.append("余额不足").append("\t");
                } else if ("3".equals(transStat)) {
                    sb.append("3031").append("\t");
                    sb.append("账号户名不符 ").append("\t");
                } else if ("4".equals(transStat)) {
                    sb.append("3057").append("\t");
                    sb.append("账户状态错误").append("\t");
                } else if("0".equals(transStat)){
                    sb.append("9999").append("\t");
                    sb.append("未知").append("\t");
                }

                sb.append("").append("\t");
                sb.append("").append("\t");
                sb.append("").append("\t");

                if ("1".equals(transStat)) {
                    sb.append(getRandomNumber25());
                } else {
                    sb.append("0000000000");
                }
                sb.append("\r\n");
                count++;
            }
        }
        return sb.toString();
    }



    /**
     * 数据生成
     * @param length
     */
    public static String getAllContents(int length, int month, int start){
        // 规则，需要对41个字段规则进行处理
        // MIOS_TRANS_CODE 序号 自增就可以，
        // SYS_NO 系统编号 Q 1 2 3 4 5 6 7 8 9
        // TRANS_CODE


        int count =8000;
        StringBuffer sb = new StringBuffer();
        for(int i=1; i<length ; i++) {
            // 随机生成一个用户的记录条数
            int records = 0 + (int)(Math.random() * (10 - 0));
            // 生成一个用户的唯一数据
            String name = getHan();

            String bankNo = Utils.getRandomNumber() +"";

            String zjhm = getRandomCardNo();
            int dateYear = 2016;
            String dateMonth = month>9 ? month+"": "0"+month;
            int dateDay = 0;
            String transBatNo = getTransBatNo();
            int random = (int)(Math.random() * (10));
            for(int j=1;j<records;j++){

                String transStat = getTransStat();
                if(j == records-1){
                    transStat = (random%7 == 0)? "5" : "1";
                } else {
                    if(transStat.equals("1")){
                        transStat = (Integer.parseInt(transStat)+1)+"";
                    }
                }

                // 无付费
                sb.append(count).append("\t");  // MIOS_TRANS_CODE
                sb.append("1").append("\t"); // SYS_NO
                sb.append(start + count).append("\t"); //TRANS_CODE
                sb.append(getTransBatNo()).append("\t"); // TRANS_BAT_NO  10411  10080 56 10082(付费) 10060
                sb.append(transStat).append("\t"); // TRANS_STAT
                if ("10082".equals(transBatNo)) {
                    sb.append("42").append("\t");
                    sb.append(start + count).append("\t");
                    sb.append("850099030130").append("\t"); // 付
                    sb.append("1102").append("\t"); // BANK_CODE
                } else {
                    sb.append("41").append("\t"); // TRANS_BAT_SEQ
                    sb.append("0").append("\t"); // PLNMIO_REC_ID
                    sb.append("850099020110").append("\t"); // BRANCH_BANK_ACC_NO 收

                    if ("10080".equals(transBatNo)) {
                        sb.append("1102").append("\t"); // BANK_CODE 1103 1102
                    } else if ("10060".equals(transBatNo)) {
                        sb.append("1103").append("\t"); // BANK_CODE 1103 1102
                    } else {
                        sb.append("1104").append("\t"); // BANK_CODE 1103 1102
                    }
                }
                sb.append("").append("\t");
                sb.append("").append("\t");
                sb.append("").append("\t");
                sb.append("").append("\t");
                sb.append("").append("\t");

                // 银行账户
                sb.append(bankNo).append("\t");

                // 姓名
                sb.append(name).append("\t");

                sb.append("I").append("\t"); // 证件类型

                sb.append(zjhm).append("\t");  // 证件号码

                sb.append(getMgrBranchNo()).append("\t"); // 管理机构

                sb.append(getRandomNumber25()).append("\t"); // CNTR_NO 合同号
                sb.append("0").append("\t");

                sb.append("C").append("\t"); // F
                if ("10082".equals(transBatNo)) {
                    sb.append("-1").append("\t"); // mio_class
                    if (i % 100 == 0) {
                        sb.append("RS").append("\t");
                    } else {
                        sb.append("EC").append("\t");
                    }
                } else {
                    sb.append("1").append("\t"); // mio_class
                    sb.append("PS").append("\t");
                }

                int ds = 10 + (int)(Math.random() * (10 - 0));
                int ds2 = (int)(Math.random() * (4));
                if("1".equals(transStat)){
//                    if(count%10==0){
                        sb.append(dateYear+"-"+dateMonth+"-"+(10 + ds2 )).append("\t");
//                    }else {
//                        sb.append(dateYear+"-"+dateMonth+"-"+((dateDay+ds)>9?(dateDay+ds):"0"+(dateDay+ds))).append("\t");
//                    }
                }else {
                    //sb.append(dateYear+"-"+dateMonth+"-"+(dateDay+ds)).append("\t");
                    sb.append(dateYear+"-"+dateMonth+"-"+((dateDay+ds)>9?(dateDay+ds):"0"+(dateDay+ds))).append("\t");
                }
                 // 应付日期，实付日期
                sb.append(dateYear+"-"+dateMonth+"-"+(dateDay+30)).append("\t");

                sb.append(getRandomDouble()).append("\t");

                sb.append(getRandomNumber25()).append("\t"); // CUST_NO

                sb.append(dateYear+"-"+dateMonth+"-"+(dateDay+20)).append("\t");  // greatDate

                sb.append(count).append("\t");  // unit_trans_code

                sb.append(getRandomNumber25()).append("\t");
                sb.append("").append("\t");
                sb.append("").append("\t");
                sb.append("").append("\t");
                sb.append("").append("\t");

                if ("1".equals(transStat)) {
                    sb.append("0000").append("\t");
                    sb.append("处理成功 ").append("\t");
                } else if ("2".equals(transStat)) {
                    sb.append("3008").append("\t");
                    sb.append("余额不足").append("\t");
                } else if ("3".equals(transStat)) {
                    sb.append("3031").append("\t");
                    sb.append("账号户名不符 ").append("\t");
                } else if ("4".equals(transStat)) {
                    sb.append("3057").append("\t");
                    sb.append("账户状态错误").append("\t");
                }

                sb.append("").append("\t");
                sb.append("").append("\t");
                sb.append("").append("\t");

                if ("1".equals(transStat)) {
                    sb.append(getRandomNumber25());
                } else {
                    sb.append("0000000000");
                }
                sb.append("\r\n");
                count++;
            }
        }
        return sb.toString();
    }

    public static double getRandomDouble(){
        int  x=  (int)(Math.random() * (1000 - 0));
        Double d = x + Math.random() * (10000 - 1);
        DecimalFormat df = new DecimalFormat("######0.00");
        return Double.parseDouble(df.format(d));
    }


    public static String getRandomCardNo(){
        // 生成随机的证件号码
        CreateIDCardNo cre = new CreateIDCardNo();
        String randomID = cre.getRandomID();

        return randomID;
    }


    private static String getHan() {
        StringBuffer sb = new StringBuffer();
        for (int i = 1; i < 4; i++) {
            sb.append(getRandomChar());
        }
        return sb.toString();
    }

    public static BigInteger getRandomNumber25() {
        String s = "";
        Random random = new Random();
        s += random.nextInt(9) + 1;
        for (int i = 0; i < 25 - 1; i++) {
            s += random.nextInt(10);
        }
        BigInteger randomNumber = new BigInteger(s);
        return randomNumber;
    }

    public static BigInteger getRandomNumber18() {
        String s = "";
        Random random = new Random();
        s += random.nextInt(9) + 1;
        for (int i = 0; i < 18 - 1; i++) {
            s += random.nextInt(10);
        }
        BigInteger randomNumber = new BigInteger(s);
        return randomNumber;
    }


    /**
     * 生成随机汉字
     * @return
     */
    private static char getRandomChar() {
        String str = "";
        int hightPos; //
        int lowPos;
        Random random = new Random();
        hightPos = (176 + Math.abs(random.nextInt(39)));
        lowPos = (161 + Math.abs(random.nextInt(93)));
        byte[] b = new byte[2];
        b[0] = (Integer.valueOf(hightPos)).byteValue();
        b[1] = (Integer.valueOf(lowPos)).byteValue();

        try {
            str = new String(b, "GBK");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.out.println("错误");
        }

        return str.charAt(0);
    }
    private static String getMgrBranchNo(){
        List list = new ArrayList<String>();
        list.add("850101");
        list.add("850201");
        list.add("850301");
        //list.add("10082");
        list.add("850101");
        list.add("850101"); list.add("850101"); list.add("850101"); list.add("850101"); list.add("850101"); list.add("850101");

        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();
    }
    public static String  getTransBatNo (){
        //10411  10080 56 10082(付费) 10060
        List list = new ArrayList<String>();
        list.add("10411");
        list.add("10411");
        list.add("10411");
        list.add("10080");
        list.add("56");
        list.add("10411");
        list.add("10082");
        list.add("10411");
        list.add("10080");
        list.add("56");
        list.add("10080");
        list.add("56");
        list.add("10060");
        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();
    }

    public static String  getTransStatTest (){
        List list = new ArrayList<String>();
//        list.add("1");
        list.add("1");
        list.add("2");
        list.add("1");
        list.add("3");
        list.add("1");
//        list.add("1");
        list.add("4");
        list.add("1");
        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();
    }

    public static String  getTransStat (){
        List list = new ArrayList<String>();
        list.add("1");
        list.add("2");
        //list.add("1");
        list.add("3");
       // list.add("1");
        list.add("4");
       // list.add("1");
        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();
    }

    public static void getBasicMess(StringBuffer content){
        content.append(Utils.getRandomNumber(2)).append("\t");  // user_age
        content.append(Utils.getNumber1()).append("\t");  // user_sex
        content.append(Utils.getAddress()).append("\t");  // user_address
        content.append(Utils.getNumber1()).append("\t");  // marital 婚姻状况以0 未婚， 1 已婚  代替；
        // 如果实际情况是汉字，需要进行分类
        content.append(Utils.getEducation()).append("\t");  // education
        content.append(Utils.getInsuranceType()).append("\t");  // insurance_type
        content.append(Utils.getRandomNumber(3) + ".00").append("\t");  // accord
        content.append(Utils.getCustomLevel()).append("\t");  // custom_level
        content.append(Utils.getInsuranceLevel()).append("\t");  // insurance_level
        content.append(Utils.getNumber1());  // insurance_channel
    }
}