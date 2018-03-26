package com.anthony.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;

/**
 * @Description: UserAgent解析测试
 * @Date: Created in 11:12 2018/3/26
 * @Author: Anthony_Duan
 */


public class UserAgentTest {

    public static void main(String[] args) {
        String source = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36";

        UserAgentParser userAgentParser  = new UserAgentParser();
        UserAgent agent = userAgentParser.parse(source);
        String os = agent.getOs();
        String browser =agent.getBrowser();
        String engine =  agent.getEngine();
        String engineversion = agent.getEngineVersion();
        String platform = agent.getPlatform();
        boolean ismobile = agent.isMobile();

        System.out.println(browser+","+ engine+","+engineversion+","+os+","+platform+","+ismobile);
    }
}
