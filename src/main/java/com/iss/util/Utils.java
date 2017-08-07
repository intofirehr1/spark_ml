package com.iss.util;

import java.io.*;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by hadoop on 2017/2/1 0001.
 *
 */
public class Utils {

    /**
     * 生成uuid
     * @return
     */
    public static synchronized String getUUID() {
        UUID uuid = UUID.randomUUID();
        String str = uuid.toString();
        String uuidStr = str.replace("-", "");
        return uuidStr;
    }

    public static String getHouse(){
        List list = new ArrayList<String>();
        list.add("是");
        list.add("否");

        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();
    }


    public static String getAddress(){
        List list = new ArrayList<String>();
        list.add("Bei jing");
        list.add("Shang hai");
        list.add("Hang zhou");
        list.add("Shen zhen");

        list.add("Tian jin");
        list.add("Chong qing");
        list.add("Wu han");
        list.add("Guang zhou");

        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();

    }


    public static String getEducation(){
        List list = new ArrayList<String>();
        list.add("本科");
        list.add("专科");
        //list.add("高中");
        list.add("硕士");
        list.add("博士");
        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();

    }

    public static String getInsuranceType(){
        List list = new ArrayList<String>();
        list.add("健康险");
        list.add("车险");
        list.add("旅游险");
        list.add("意外险");
        list.add("家财险");
        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();
    }

    public static String getCustomLevel(){
        List list = new ArrayList<String>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(5);
        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();
    }

    public static String getInsuranceLevel(){
        List list = new ArrayList<String>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(5);
        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();
    }

    public static String getNumber1(){
        List list = new ArrayList<String>();
        list.add(1);  // 已婚
        list.add(2);  // 未婚
        list.add(3);  // 离异
        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();
    }

    public static String getNumber2(){
        List list = new ArrayList<String>();
        list.add(0);
        list.add(1);
        list.add(2);
        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();
    }

    public static String getNumber3(){
        List list = new ArrayList<String>();
        list.add(0);
        list.add(1);
        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();
    }


    public static String getProfession(){
        List list = new ArrayList<String>();
        list.add("医生");
        list.add("教师");
        list.add("工人");
        list.add("农民");
        list.add("公务员");
        list.add("司机");
        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();
    }


    public static String getVocation(){
        List list = new ArrayList<String>();
        list.add("通讯");
        list.add("电力");
        list.add("金融");
        list.add("环保");
        list.add("制造");
        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();
    }

    public static String getLoan(){
        List list = new ArrayList<String>();
        list.add("No");
        list.add("Yes");

        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();
    }

    public static String getHY(){
        List list = new ArrayList<String>();
        list.add("已婚");
        list.add("单身");
        list.add("离婚");
        Random rand = new Random();
        //list集合取值转换成String类型
        return list.get(rand.nextInt(list.size())).toString();
    }

    /**
     * 以行为单位读取文件，常用于读面向行的格式化文件
     */
    public static List readFileByLines(List list, String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                //System.out.println("line " + line + ": " + tempString);
                list.add(tempString);
                line++;
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
        return list;
    }

    public static String [] getUserCord(){
        List list = new ArrayList<String>();
        // 存放数据，以, 分割, 数据顺序为 用户名, 卡类型, 卡类型。
        list = readFileByLines(list, "data/gs_train_test.txt");
        Random rand = new Random();
        return list.get(rand.nextInt(list.size())).toString().split(",");
    }




    /**
     * 生成随机字符串，包含数字字母
     * @param length
     * @return
     */
    public static String getRandom1(int length) { //length表示生成字符串的长度
        String base = "abcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }

    /**
     * 返回18位随机整数
     *
     * @return
     */
    public static BigInteger getRandomNumber() {
        String s = "";
        Random random = new Random();
        s += random.nextInt(9) + 1;
        for (int i = 0; i < 18 - 1; i++) {
            s += random.nextInt(10);
        }
        BigInteger randomNumber = new BigInteger(s);
        return randomNumber;
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


    /**
     * 返回指定位数的随机数
     *
     * @return
     */
    public static BigInteger getRandomNumber(int len) {
        String s = "";
        Random random = new Random();
        s += random.nextInt(9) + 1;
        for (int i = 0; i < len - 1; i++) {
            s += random.nextInt(10);
        }
        BigInteger randomNumber = new BigInteger(s);
        return randomNumber;
    }

    /**
     * 生成日期
     * @return
     */
    private static String getDayTime() {
        Date time = new Date();
        // Calendar calendar = Calendar.getInstance();// 获取当前时间
        // Date time = calendar.getTime();// 当前时间
        // calendar.add(Calendar.MINUTE, -2);// 获取当前时间的前5分钟
        // Date newTime = calendar.getTime();// 当前时间前5分钟
        String sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time);// 定义图表的时间
        return sdf;
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


    private static String getHan() {
        StringBuffer sb = new StringBuffer();
        for (int i = 1; i < 15; i++) {
            sb.append(getRandomChar());
        }
        return sb.toString();
    }

    /**
     * 生成随机的字符串，纯字母包括大小写
     * @param length
     * @return
     */
    private static String getRandomString(int length) { // length表示生成字符串的长度
        String base = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }

    private static String getMac() {
        Random random = new Random();
        String result = "";
        for (int i = 0; i < 2; i++) {
            result += random.nextInt(10);
        }
        return result;
    }

    /**
     * 生成对应模板的测试数据i
     *
     * @param methodName
     *            生成文件名称
     * @param dataNumber
     *            数据条数
     * @param fieldsNumber
     *            字段数
     */
    public static void createData(String path,String fileName, int dataNumber) {
        String version = "001";
        String type = "C";
        String tmpFileName = "."+fileName;
        String filePath = path+tmpFileName;
        System.out.println(filePath);
        File file = new File(filePath);
        FileWriter fw = null;
        //BufferedWriter bw;
        try {
            if (!file.exists()) {
                System.out.println("没有这个文件，创建中请稍等。。。");
                file.createNewFile();
            }
            fw = new FileWriter(file.getAbsoluteFile());
            //bw = new BufferedWriter(fw);


            StringBuffer sb = new StringBuffer();
            for (int i = 1; i <= dataNumber; i++) {
                String id = getUUID();
                String ope = getHan();
                String date = getDayTime();
                String arr = createContent();
                String mac = getMac()+"-"+getMac()+"-"+getMac()+"-"+getMac()+"-"+getMac();
                String url ="http://www."+getRandom1(8)+".com";
                sb.append(id);
                sb.append(",");
                sb.append(version);
                sb.append("C"+i);
                sb.append(",");
                sb.append(type);
                sb.append(",");
                sb.append("C00"+i);
                sb.append(",");
                sb.append(mac);
                sb.append(",");
                sb.append(date);
                sb.append(",");
                sb.append(url);
                sb.append(",");
                sb.append(ope);
                sb.append(",");
                sb.append(arr);
                sb.append("\n");

                if(i%100 == 0)
                {
                    fw.write(sb.toString());
                    fw.flush();
                    sb = new StringBuffer();
                }
            }
            fw.write(sb.toString());
            fw.flush();
            fw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            try {
                if(null != fw){
                    fw.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        rename(path, fileName);

    }

    /**
     * 生成正文
     *
     * @return
     */
    private static String createContent() {
//		return getRandomString(5500 * 40);
        return getRandomString(200);
    }

    public void createLog(String fileName, int dataNum, String path){
        createData(path, fileName, dataNum);
        //rename(path, fileName);
    }

    private static void rename(String path, String fileName){
        String tmpFileName = "."+fileName;
        String filePath = path+tmpFileName;
        File file = new File(filePath);
        if(file.isFile()){
            file.renameTo(new File(path+fileName));
        }
    }

}
