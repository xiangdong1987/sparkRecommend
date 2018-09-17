package com.xdd.IRsystem;

import java.util.regex.Matcher;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import java.net.*;
import java.util.List;
import java.util.regex.Pattern;

public class Log {

    private String timestamp;
    private String remote_addr;
    private String remote_user;
    private String body_bytes_sent;
    private String request_time;
    private String domain;
    private String status;
    private String request;
    private String http_referrer;
    private String upstream_response_time;
    private String http_x_forwarded_for;
    private String http_cookie;
    private String http_user_agent;

    public Boolean isEmpty;

    public Log(String logStr) {
        JSONObject jsonObject = JSONObject.parseObject(logStr);
        if (jsonObject != null) {
            this.timestamp = jsonObject.getString("@timestamp");
            this.remote_addr = jsonObject.getString("remote_addr");
            this.remote_user = jsonObject.getString("remote_user");
            this.body_bytes_sent = jsonObject.getString("body_bytes_sent");
            this.request_time = jsonObject.getString("request_time");
            this.domain = jsonObject.getString("domain");
            this.status = jsonObject.getString("status");
            this.request = jsonObject.getString("request");
            this.http_referrer = jsonObject.getString("http_referrer");
            this.upstream_response_time = jsonObject.getString("upstream_response_time");
            this.http_x_forwarded_for = jsonObject.getString("http_x_forwarded_for");
            this.http_cookie = jsonObject.getString("http_cookie");
            this.http_user_agent = jsonObject.getString("http_user_agent");
            this.isEmpty = false;
        } else {
            this.isEmpty = true;
        }
    }

    void show() {
        System.out.println("timestamp :" + this.timestamp);
        System.out.println("remote_addr :" + this.remote_addr);
        System.out.println("remote_user :" + this.remote_user);
        System.out.println("body_bytes_sent :" + this.body_bytes_sent);
        System.out.println("request_time :" + this.request_time);
        System.out.println("domain :" + this.domain);
        System.out.println("status :" + this.status);
        System.out.println("request :" + this.request);
        System.out.println("http_referrer :" + this.http_referrer);
        System.out.println("timestamp :" + this.timestamp);
        System.out.println("upstream_response_time :" + this.upstream_response_time);
        System.out.println("http_x_forwarded_for :" + this.http_x_forwarded_for);
        System.out.println("http_cookie :" + this.http_cookie);
        System.out.println("http_user_agent :" + this.http_user_agent);
    }

    public Boolean isInfoDetail() {
        String url = this.request;
        String pattern = ".*information/info.*";
        boolean isMatch = Pattern.matches(pattern, url);
        if (isMatch) {
            return true;
        } else {
            return false;
        }
    }

    public String parseInfoUrl() throws URISyntaxException {
        String request = this.request;
        String[] urls = request.split(" ");
        String query = urls[1];
        System.out.println("query=" + query);

        List<org.apache.http.NameValuePair> params = URLEncodedUtils.parse(new URI(query), "UTF-8");
        String info_tid = "";
        String uid = "0";
        for (NameValuePair param : params) {
            if (param.getName().equals("info_tid")) {
                info_tid = param.getValue();
            }
            if (param.getName().equals("ac_token")) {
                uid = this.getUid(param.getValue());
            }
        }
        // 现在创建 matcher 对象
        return uid + " " + info_tid + " 1";
    }

    public String getUid(String str) {
        String pattern = "u(\\d+)_.*";
        System.out.println(str);
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(str);
        String uid = "0";
        if (m.find()) {
            System.out.println("Found value: " + m.group(0));
            System.out.println("Found value: " + m.group(1));
            uid = m.group(1);
        } else {
            System.out.println("NO MATCH");
        }
        return uid;
    }
}
