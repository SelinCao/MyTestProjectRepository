package com.znv.KafkaToEs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Created by User on 2017/12/15.
 */
public class KafkaPoolData {
    private KafkaConsumer<String, Map<String, Object>> consumer;
    private String topic;

    private static String uintarray[] = {"DOOROPEN", "SEX", "FCPID", "CONTROLLEVEL", "BLACKFLAG", "TYPE", "CAMERATYPE", "FRAMEINDEX", "IMGWIDTH",
            "IMGHEIGHT", "LEFTPOS", "TOP", "RIGHTPOS", "BOTTOM", "NEEDCONFIRM", "CONFIRMSTATUS"};
    private static String floatarray[] = {"GPSX", "GPSY", "YAW", "PITCH", "ROLL"};
    private static String ufloatarray[] = {"QUALITYSCORE", "SIMILARITY"};
    private static String bytearray[] = {"WHITEIMAGEDATA", "WHITEFEATURE", "BLACKIMAGEDATA", "BLACKFEATURE"};


    public KafkaPoolData(String topic) {
        Properties consumerConfig = new Properties();
        //broker
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "face.dct-znv.com:9092");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "czl-source-group1"); //消费者组，保证要跟其他的不一样
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// 这个参数指定了当消费者第一次读取分区或者上一次的位置太老（比如消费者下线时间太久）时的行为，可以取值为latest（读取消息队列中最新的消息）或者earliest（从最老的消息开始消费）
        /**kafka根据key值确定消息发往哪个分区（如果分区被指定则发往指定的分区），具有相同key的消息被发往同一个分区，如果key为NONE则随机选择分区，可以使用key_serializer参数序列化为字节类型
        value为要发送的消息值，必须为bytes类型，如果这个值为空，则必须有对应的key值，并且空值被标记为删除。可以通过配置value_serializer参数序列化为字节类型*/
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.znv.kafka.common.KafkaAvroDeSerializer"); //用来做反序列化的,也就是将字节数组转换成对象
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");  //用来做反序列化的,也就是将字节数组转换成对象
        consumer = new KafkaConsumer<String, Map<String, Object>>(consumerConfig); //创建一个消费者
        this.topic = topic;
    }

    public void testConsumer(long offset,int number) {
      /*  List<String> uintlist = new ArrayList<String>(Arrays.asList(uintarray));
        List<String> floatlist = new ArrayList<String>(Arrays.asList(floatarray));
        List<String> ufloatlist = new ArrayList<String>(Arrays.asList(ufloatarray));
        List<String> bytelist = new ArrayList<String>(Arrays.asList(bytearray));*/

        // kafka topic名称
        //consumer.subscribe(Collections.singletonList(topic));//订阅topic，这个方法接收一个topic列表
        //重新开始拉数据
        consumer.poll(1);//我们不断调用poll拉取数据，如果停止拉取，那么Kafka会认为此消费者已经死亡并进行重平衡。参数值是一个超时时间，指明线程如果没有数据时等待多长时间，0表示不等待立即返回
        consumer.seekToBeginning();//在给消费者分配分区的时候将消息偏移量跳转到起始位置 。
        List<TopicPartition> partitions = new ArrayList<TopicPartition>();
       // long offset =10;
        //与subscirbe方法不同，assign方法由用户直接手动consumer实例消费哪些具体分区,assign的consumer不会拥有kafka的group management机制，也就是当group内消费者数量变化的时候不会有reblance行为发生。assign的方法不能和subscribe方法同时使用。
        for (int i = 0; i < 10; i++) {
            TopicPartition partition = new TopicPartition(topic, i);//i是指定分区partition
            partitions.add(partition);
            consumer.assign(partitions);
            consumer.seek(partition, offset);// 指定从这个topic和partition的哪个位置获取
        }  //consumer要消费0-9分区
       //consumer.close(); 主动关闭可以使得Kafka立即进行重平衡而不需要等待会话过期
        boolean result = true;
        int count = 0;
        System.out.println("\nConsumer begin!\n");
        while (result) {
            System.out.println("\nConsumer data begin!\n");
            ConsumerRecords<String, Map<String, Object>> records = consumer.poll(1000); //拉取消费记录

            for (ConsumerRecord<String, Map<String, Object>> record : records) {
                JSONObject json = new JSONObject();

                Map<String, Object> map = record.value();
                //System.out.println("\nParse data!\n");
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    String filedName = String.valueOf(entry.getKey());
                   /* if (uintlist.contains(filedName)) {
                        json.put(filedName, (Integer) (entry.getValue()));
                    } else if (floatlist.contains(filedName)) {
                        json.put(filedName, (Float) (entry.getValue()));
                    } else if (ufloatlist.contains(filedName)) {
                        json.put(filedName, (Float) (entry.getValue()));
                    } else if (bytelist.contains(filedName)) {
                        if (filedName.equals("WHITEIMAGEDATA") || filedName.equals("BLACKIMAGEDATA")) {
                            //需要在工程下自己建立picture目录
                            String imagePath = "picture/" + String.valueOf(entry.getKey()) + count + ".jpg";
                            //需要保存图片打开注释
                            //savePictureData(imagePath, String.valueOf(entry.getValue()));
                        } else {
                            ByteBuffer byteFeatrue = (ByteBuffer) (entry.getValue());
                            if (byteFeatrue != null && byteFeatrue.array().length > 0) {
                                json.put(filedName, (byteFeatrue).array());
                            }
                        }
                    } else {
                        json.put(filedName, String.valueOf(entry.getValue()));
                    }*/
                    if (filedName.equals("image_data")) {

                    }else {
                        json.put(filedName, String.valueOf(entry.getValue())); //将非图片信息保存到json

                    }
                }
               // System.out.println(JSON.toJSONString(json,true));
              //判断数据是否在要拉取的时间段内
                if(json.containsKey("time_stamp")) {
                    if (json.get("time_stamp") != null && (json.get("time_stamp").toString().compareTo("2018-01-19 10:30:00")) > 0 && json.get("time_stamp").toString().compareTo("2018-01-19 11:00:00") < 0) {
                        /*SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String timeStamp = sdf.format(json.getString("time_stamp"));
                        json.put("time_stamp",timeStamp);*/
                        System.out.println("put to es");
                        //生成es索引的文档id
                        UUID uuid = UUID.randomUUID();
                        String id = uuid.toString();
                        PutDataToEs.PutToEs(json,id);
                    }
                }

              //  System.out.println(json.toJSONString() + "\n");
                count++;
                if (count % 100 == 0) {
                    System.out.println("parse data number : " + count); //打印写数据的数量
                }
                if (count == number) {
                    result = false;
                    break;
                }


            }
        }
        System.out.println("\nParse data Finished! count=" + count + "\n");
        System.out.println("\nConsumer Finished!\n");
    }



    public static void main(String[] args) {
        //初始化ES
        PutDataToEs.initClient();
        //topic名称
        String topic = "fss-analysis-v1-1-production";
        //String topic = args[0];
        long offset =110000; //从哪里开始
       // long offset = Long.parseLong(args[0]);
        int number=1000000;  //需要拉取多少数据
       // int number = Integer.parseInt(args[1]);
        KafkaPoolData consumer = new KafkaPoolData(topic);
        consumer.testConsumer(offset,number);
        PutDataToEs.closeClient();
    }
}
