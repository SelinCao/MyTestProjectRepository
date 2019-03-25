package com.znv.KafkaToEs;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


/**
 * Created by User on 2017/12/15.
 */
public class PutDataToEs {

    private static TransportClient client = null;
    private static Random rnd = new Random();
    private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String startDate = "2017-01-01 00:00:00";


    private static long randomNum(long begin, long end)
    {
        long rtn = begin + (long)(Math.random() * (end - begin));
        if (rtn == begin || rtn == end)
        {
            return randomNum(begin,end);
        }
        return rtn;
    }
    public  static Date randomDate(String beginDate, String endDate)
    {
        try
        {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date start = format.parse(beginDate);
            Date end = format.parse(endDate);
            if (start.getTime() >= end.getTime())
            {
                return null;
            }
            long date = randomNum(start.getTime(),end.getTime());
            return new Date(date);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }

    public static void initClient() {
        try {
            // on startup
            Settings settings = Settings.builder()
                    .put("cluster.name", "face.dct-znv.com-es").build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.45.157.112"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public static void closeClient() {
        client.close();
    }


    public static void PutToEs(JSONObject json,String id) {

        try {
            String msgType="";
            String resultType="";
            String cameraId="";
            String cameraName="";
            String cameraType="";
            String timeStamp="";
            String gpsx="";
            String gpsy="";
            String officeId ="";
            String officeName ="";
            String frameTime ="";
            String frameIndex="";
            String taskIdx ="";
            String trackIdx ="";
            String feature ="";
            String imgWidth ="";
            String imgHeight ="";
            String imgUrl ="";
            String imgName ="";
            String imgData ="";
            String qualityScore ="";
            String resultIdx ="";
            String left ="";
            String top ="";
            String right ="";
            String bottom ="";
            String yaw ="";
            String pitch ="";
            String roll ="";
            String bigPictureUuid ="";

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            if(json.containsKey("msg_type")){
                msgType = json.getString("msg_type");
            }
            if(json.containsKey("result_type")){
                resultType = json.getString("result_type");
            }
            if(json.containsKey("camera_id")){
                cameraId = json.getString("camera_id");
            }
            if(json.containsKey("camera_name")){
                cameraName = json.getString("camera_name");
            }
            if(json.containsKey("camera_type")){
                cameraType = json.getString("camera_type");
            }
            if(json.containsKey("time_stamp")){
                timeStamp = json.getString("time_stamp");
            }
            if(json.containsKey("gpsx")){
                gpsx = json.getString("gpsx");
            }
            if(json.containsKey("gpsy")){
                gpsy = json.getString("gpsy");
            }
            if(json.containsKey("office_id")){
                officeId = json.getString("office_id");
            }
            if(json.containsKey("office_name")){
                officeName = json.getString("office_name");
            }
            if(json.containsKey("frame_time")){
                frameTime = json.getString("frame_time");
            }
            if(json.containsKey("frame_index")){
                frameIndex = json.getString("frame_index");
            }
            if(json.containsKey("task_idx")){
                taskIdx = json.getString("task_idx");
            }
            if(json.containsKey("track_idx")){
                trackIdx = json.getString("track_idx");
            }
            if(json.containsKey("feature")){
                feature = json.getString("feature");
            }
            if(json.containsKey("img_width")){
                imgWidth = json.getString("img_width");
            }
            if(json.containsKey("img_height")){
                imgHeight = json.getString("img_height");
            }
            if(json.containsKey("img_name")){
                imgName = json.getString("img_name");
            }
            if(json.containsKey("img_url")){
                imgUrl = json.getString("img_url");
            }
            if(json.containsKey("img_data")){
                imgData = json.getString("img_data");
            }
            if(json.containsKey("quality_score")){
                qualityScore= json.getString("quality_score");
            }
            if(json.containsKey("result_idx")){
                resultIdx = json.getString("result_idx");
            }
            if(json.containsKey("left")){
                left = json.getString("left");
            }
            if(json.containsKey("top")){
                 top = json.getString("top");
            }
            if(json.containsKey("right")){
                right = json.getString("right");
            }
            if(json.containsKey("bottom")){
                bottom = json.getString("bottom");
            }
            if(json.containsKey("yaw")){
                yaw= json.getString("yaw");
            }
            if(json.containsKey("pitch")){
                pitch = json.getString("pitch");
            }
            if(json.containsKey("roll")){
                roll = json.getString("roll");
            }
            if(json.containsKey("big_picture_uuid")){
                bigPictureUuid = json.getString("big_picture_uuid");
            }

            BulkRequestBuilder bulkRequest = client.prepareBulk();
            //bulkRequest.add(client.prepareIndex("kafka_to_es_test", "kafkadata", String.valueOf(id)).setSource(json.toString()));
            // bulkRequest.add(client.prepareIndex("logs_write", "type", String.valueOf(id)).setSource(json.toString()));
           /* BulkResponse bulkResponse1 = bulkRequest.get();
            if (bulkResponse1.hasFailures()) {
                // process failures by iterating through each bulk response item
                System.out.println(bulkRequest.toString());
            }*/
            bulkRequest.add(client.prepareIndex("kafka_to_es_test", "kafkadata", String.valueOf(id)).setSource(jsonBuilder()
                            .startObject()
                            .field("time_stamp", sdf.parse(timeStamp))
                            .field("msg_type", msgType)
                            .field("result_type", resultType)
                            .field("camera_id", cameraId)
                            .field("camera_name", cameraName)
                            .field("camera_type", cameraType)
                            .field("gpsx", gpsx)
                            .field("gpsy", gpsy)
                            .field("office_id", officeId)
                            .field("office_name", officeName)
                            .field("frame_time", frameTime)
                            .field("frame_index", frameIndex)
                            .field("task_idx", taskIdx)
                            .field("track_idx", trackIdx)
                            .field("feature", feature)
                            .field("img_width", imgWidth)
                            .field("img_height", imgHeight)
                            .field("img_url", imgUrl)
                            .field("img_name", imgName)
                            .field("img_data", imgData)
                            .field("quality_score", qualityScore)
                            .field("result_idx", resultIdx)
                            .field("left",left)
                            .field("top", top)
                            .field("right", right)
                            .field("bottom", bottom)
                            .field("yaw", yaw)
                            .field("pitch", pitch)
                            .field("roll", roll)
                            .field("big_picture_uuid", bigPictureUuid)
                            .endObject()
                    )
            );
            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                // process failures by iterating through each bulk response item
                System.out.println(bulkRequest.toString());
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String formatTime(String time) {
        String formatTime = "";
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        // 转换成东八区时区
        sdf1.setTimeZone(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date enterTime1 = sdf1.parse(time);
            formatTime = sdf2.format(enterTime1);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return formatTime;
    }


    /*public static void bulkPutEs() {
        try {

            long t1 = System.currentTimeMillis();
            int n = 0;
            while (n++ < 10000) {
                BulkRequestBuilder bulkRequest = client.prepareBulk();
                for (int i = 0; i < 1000; i++) {
                    int id = rnd.nextInt(1000);
                    String name = "名字" + id;
                    int gender = rnd.nextInt(2);
                    int age = rnd.nextInt(100);
                    Date enter_time = randomDate(startDate,df.format(new Date()));

                    bulkRequest.add(client.prepareIndex("logs_write", "type1", String.valueOf(id)+String.valueOf(enter_time.getTime()))
                            .setSource(jsonBuilder()
                                    .startObject()
                                    .field("id", id)
                                    .field("name",name)
                                    .field("gender",gender)
                                    .field("age", age)
                                    .field("enter_time", enter_time)
                                    .endObject()
                            )
                    );
                }
                BulkResponse bulkResponse = bulkRequest.get();
                if (bulkResponse.hasFailures()) {
                    // process failures by iterating through each bulk response item
                    System.out.println(bulkRequest.toString());
                }
            }

            long ts = System.currentTimeMillis() - t1;
            System.out.println("cost "+ts +" ms.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/
/*
    public static void main(String []args) {
        initClient();
        PutToEs();
        closeClient();
    }*/



}
