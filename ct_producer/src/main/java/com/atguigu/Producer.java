package com.atguigu;



import java.awt.*;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;

public class Producer{
    private String start = "2017-01-01";
    private String end = "2018-01-01";
    public Map<String, String> phoneName =new HashMap<String, String>();
    public List<String> phoneNum = new ArrayList<String>();

    public void init() {
        phoneNum.add("15369468720");
        phoneNum.add("19920860202");
        phoneNum.add("18411925860");
        phoneNum.add("14473548449");
        phoneNum.add("18749966182");
        phoneNum.add("19379884788");
        phoneNum.add("19335715448");
        phoneNum.add("18503558939");
        phoneNum.add("13407209608");
        phoneNum.add("15596505995");
        phoneNum.add("17519874292");
        phoneNum.add("15178485516");
        phoneNum.add("19877232369");
        phoneNum.add("18706287692");
        phoneNum.add("18944239644");
        phoneNum.add("17325302007");
        phoneNum.add("18839074540");
        phoneNum.add("19879419704");
        phoneNum.add("16480981069");
        phoneNum.add("18674257265");
        phoneNum.add("18302820904");
        phoneNum.add("15133295266");
        phoneNum.add("17868457605");
        phoneNum.add("15490732767");
        phoneNum.add("15064972307");

        phoneName.put("15369468720", "李雁");
        phoneName.put("19920860202", "卫艺");
        phoneName.put("18411925860", "仰莉");
        phoneName.put("14473548449", "陶欣悦");
        phoneName.put("18749966182", "施梅梅");
        phoneName.put("19379884788", "金虹霖");
        phoneName.put("19335715448", "魏明艳");
        phoneName.put("18503558939", "华贞");
        phoneName.put("13407209608", "华啟倩");
        phoneName.put("15596505995", "仲采绿");
        phoneName.put("17519874292", "卫丹");
        phoneName.put("15178485516", "戚丽红");
        phoneName.put("19877232369", "何翠柔");
        phoneName.put("18706287692", "钱溶艳");
        phoneName.put("18944239644", "钱琳");
        phoneName.put("17325302007", "缪静欣");
        phoneName.put("18839074540", "焦秋菊");
        phoneName.put("19879419704", "吕访琴");
        phoneName.put("16480981069", "沈丹");
        phoneName.put("18674257265", "褚美丽");
        phoneName.put("18302820904", "孙怡");
        phoneName.put("15133295266", "许婵");
        phoneName.put("17868457605", "曹红恋");
        phoneName.put("15490732767", "吕柔");
        phoneName.put("15064972307", "冯怜云");
    }

    public String productLog() throws ParseException {
        String caller;//打电话
        String callee;//接电话
        String buildTime;
        int dura;
        int callerIndex = (int) (Math.random() * phoneNum.size());
        caller = phoneNum.get(callerIndex);
        while (true) {
            int calleeIndex = (int) (Math.random() * phoneNum.size());
            callee = phoneNum.get(calleeIndex);
            if (callerIndex != calleeIndex) break;
        }
      //随机生成通话时间
        buildTime = randomBuildTime(start, end);

        //3.随机生成通话时长(s)
        dura = (int) (Math.random() * 30 * 60) + 1;
        DecimalFormat df = new DecimalFormat("0000");
        String duration = df.format(dura);

        return caller + "," + callee + "," + buildTime + "," + duration + "\n";

    }
    public String randomBuildTime(String start,String end) throws ParseException {
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startPoint = sdf1.parse(start).getTime();
        long endPoint = sdf1.parse(end).getTime();

        long resultTS = startPoint + (long) (Math.random() * (endPoint - startPoint));
        return sdf2.format(new Date(resultTS));
}
    public void writeLog(String path)throws Exception{
      FileOutputStream fos = new FileOutputStream(path);
      OutputStreamWriter osw = new OutputStreamWriter(fos);
        while (true){
        String log=productLog();
        System.out.println(log);
        osw.write(log);
        osw.flush();
        Thread.sleep(300);
 }



}
    public static void main(String[] args) throws Exception {
       if (args.length<=0){
       System.out.println("没有参数");
        System.exit(0);

   }
        Producer producer = new Producer();
        producer.init();
        producer.writeLog(args[0]);



    }
}