//package com.iss.demo;
//
//import com.iss.util.Utils;
//
///**
// * Created by hadoop on 2017/8/9 0009.
// */
//public class CommonData {
//
//    public static String gerenCommonData(){
//        // 规则，需要对41个字段规则进行处理
//        // MIOS_TRANS_CODE 序号 自增就可以，
//        // SYS_NO 系统编号 Q 1 2 3 4 5 6 7 8 9
//        // TRANS_CODE
//        int count =1;
//        StringBuffer sb = new StringBuffer();
//        for(int i=1; i<10 ; i++) {
//            // 随机生成一个用户的记录条数
//            int records = 2;
//            // 生成一个用户的唯一数据
//            String name = getHan();
//
//            String bankNo = Utils.getRandomNumber() +"";
//
//            String zjhm = getRandomCardNo();
//            int dateYear = 2017;
//            int dateMonth = 10;
//            int dateDay = 10;
//            String transBatNo = getTransBatNo();
//            int random = (int)(Math.random() * (10));
//            for(int j=1;j<records;j++){
//
//                String transStat = getTransStat();
////                if(j == records-1){
////                    transStat =  "1";
////                } else {
////                    if(transStat.equals("1")){
////                        transStat = (Integer.parseInt(transStat)+1)+"";
////                    }
////                }
//                transStat = "1";
//
//                // 无付费
//                sb.append(count).append("\t");  // MIOS_TRANS_CODE
//                sb.append("1").append("\t"); // SYS_NO
//                sb.append(10000 + count).append("\t"); //TRANS_CODE
//                sb.append(getTransBatNo()).append("\t"); // TRANS_BAT_NO  10411  10080 56 10082(付费) 10060
//                sb.append(transStat).append("\t"); // TRANS_STAT
//                if ("10082".equals(transBatNo)) {
//                    sb.append("42").append("\t");
//                    sb.append(10000 + count).append("\t");
//                    sb.append("850099030130").append("\t"); // 付
//                    sb.append("1102").append("\t"); // BANK_CODE
//                } else {
//                    sb.append("41").append("\t"); // TRANS_BAT_SEQ
//                    sb.append("0").append("\t"); // PLNMIO_REC_ID
//                    sb.append("850099020110").append("\t"); // BRANCH_BANK_ACC_NO 收
//
//                    if ("10080".equals(transBatNo)) {
//                        sb.append("1102").append("\t"); // BANK_CODE 1103 1102
//                    } else if ("10060".equals(transBatNo)) {
//                        sb.append("1103").append("\t"); // BANK_CODE 1103 1102
//                    } else {
//                        sb.append("1104").append("\t"); // BANK_CODE 1103 1102
//                    }
//                }
//                sb.append("").append("\t");
//                sb.append("").append("\t");
//                sb.append("").append("\t");
//                sb.append("").append("\t");
//                sb.append("").append("\t");
//
//                // 银行账户
//                sb.append(bankNo).append("\t");
//
//                // 姓名
//                sb.append(name).append("\t");
//
//                sb.append("I").append("\t"); // 证件类型
//
//                sb.append(zjhm).append("\t");  // 证件号码
//
//                sb.append(getMgrBranchNo()).append("\t"); // 管理机构
//
//                sb.append(getRandomNumber25()).append("\t"); // CNTR_NO 合同号
//                sb.append("0").append("\t");
//
//                sb.append("C").append("\t"); // F
//                if ("10082".equals(transBatNo)) {
//                    sb.append("-1").append("\t"); // mio_class
//                    if (i % 100 == 0) {
//                        sb.append("RS").append("\t");
//                    } else {
//                        sb.append("EC").append("\t");
//                    }
//                } else {
//                    sb.append("1").append("\t"); // mio_class
//                    sb.append("PS").append("\t");
//                }
//
//                int ds = 0 + (int)(Math.random() * (10 - 0));
////                if("1".equals(transStat)){
//                sb.append(dateYear+"-"+dateMonth+"-"+(dateDay + ((ds%9==0)?ds:6))).append("\t");
////                }else {
////                    sb.append(dateYear+"-"+dateMonth+"-"+(dateDay+j)).append("\t");
////                }
//                // 应付日期，实付日期
//
//                sb.append(dateYear+"-"+dateMonth+"-"+(dateDay+10)).append("\t");
//
//                sb.append(getRandomDouble()).append("\t");
//
//                sb.append(getRandomNumber25()).append("\t"); // CUST_NO
//
//                sb.append(dateYear+"-"+dateMonth+"-"+(dateDay+20)).append("\t");  // greatDate
//
//                sb.append(count).append("\t");  // unit_trans_code
//
//                sb.append(getRandomNumber25()).append("\t");
//                sb.append("").append("\t");
//                sb.append("").append("\t");
//                sb.append("").append("\t");
//                sb.append("").append("\t");
//
//                if ("1".equals(transStat)) {
//                    sb.append("0000").append("\t");
//                    sb.append("处理成功 ").append("\t");
//                } else if ("2".equals(transStat)) {
//                    sb.append("3008").append("\t");
//                    sb.append("余额不足").append("\t");
//                } else if ("3".equals(transStat)) {
//                    sb.append("3031").append("\t");
//                    sb.append("账号户名不符 ").append("\t");
//                } else if ("4".equals(transStat)) {
//                    sb.append("3057").append("\t");
//                    sb.append("账户状态错误").append("\t");
//                } else if("0".equals(transStat)){
//                    sb.append("9999").append("\t");
//                    sb.append("未知").append("\t");
//                }
//
//                sb.append("").append("\t");
//                sb.append("").append("\t");
//                sb.append("").append("\t");
//
//                if ("1".equals(transStat)) {
//                    sb.append(getRandomNumber25());
//                } else {
//                    sb.append("0000000000");
//                }
//                sb.append("\r\n");
//                count++;
//            }
//        }
//    }
//
//
//}
