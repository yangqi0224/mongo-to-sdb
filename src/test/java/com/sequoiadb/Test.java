package com.sequoiadb;

import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import jdk.nashorn.internal.parser.JSONParser;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.BSONObject;
import org.bson.conversions.Bson;
import org.sequoiadb.bson.util.JSON;

import java.util.*;

/**
 * @ClassName Test
 * @Description 请描述类的业务用途
 * @Author yangqi
 * @Date 2023/3/15 18:27
 * @Version 1.0
 **/
public class Test {
    public static void main(String[] args) {
        /*
        MongoClient mongoClient = MongoClients.create("mongodb://yangqi:yangqi@192.168.0.118:27017/?authSource=my_db");
        MongoDatabase my_db = mongoClient.getDatabase("my_db");
        long student = my_db.getCollection("student").countDocuments();
        System.out.println(student);
        String json = "{\"_id\":\"882988758\",\"binary\":{\"$binary\":{\"base64\":\"aGVsbG8gd29ybGQ=\",\"subType\":\"00\"}}}";
        String json2 = "{\"_id\":\"882988758\",\"key\":{\"$binary\":\"aGVsbG8gd29ybGQ=\",\"$type\":\"0\"}}";
        String json1 = "{id:1,name:2,age:3,sex:{sex:1}}";
        System.out.println(json);
        System.out.println(json2);
        BasicBSONObject parse = (BasicBSONObject) JSON.parse(json2);
        BasicBSONObject basicBSONObject = new BasicBSONObject();
        basicBSONObject.put("$set","2");
        basicBSONObject.put("s",2);
        BasicBSONObject basicBSONObject1 = new BasicBSONObject();
        basicBSONObject1.put("filed",basicBSONObject);
        System.out.println(basicBSONObject1.toString());
        System.out.println(JSON.parse(basicBSONObject1.toString()));
        Set<String> strings = parse.keySet();
        for (String string : strings) {
            System.out.println(string+":"+parse.get(string).toString());
        }

         */

        que();
    }

    private void ss(){
        //throwErr();
    }

    void throwErr() throws Exception{
        throw new Exception();
    }

    public static void que(){
        MongoClient mongoClient = MongoClients.create("mongodb://120.26.166.153:11817/?authSource=my_db");
        MongoDatabase my_db = mongoClient.getDatabase("my_db");
        MongoCollection<Document> cl = my_db.getCollection("comployee");
        BasicDBObject bson = new BasicDBObject();
        BasicDBObject bson1 = new BasicDBObject();
        BasicDBObject bson2 = new BasicDBObject();
        bson.put("price",1000);
        bson1.put("customerId","2");
        List<Bson> list = new ArrayList<>();
        list.add(bson);
        list.add(bson1);
        Bson and = Filters.and(list);
        bson2.put("$query",and);
        int[] li = new int[3];
        li[0] = 1000;
        li[1] = 2000;
        li[2] = 3000;
        FindIterable<Document> documents = cl.find(Filters.lte("price",1000));
        MongoCursor<Document> iterator = documents.iterator();
        while (iterator.hasNext()) {
            Document next = iterator.next();
            System.out.println(next.toJson());
        }

    }
}
