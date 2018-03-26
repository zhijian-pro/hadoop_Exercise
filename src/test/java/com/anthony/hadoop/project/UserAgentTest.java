package com.anthony.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Description: UserAgent解析测试
 * @Date: Created in 11:12 2018/3/26
 * @Author: Anthony_Duan
 */


public class UserAgentTest {


    /**
     * 测试读取日志文件并且解析其中包含的UserAgent
     *
     * @throws Exception
     */
    @Test
    public void testReadFile() throws Exception {

        //日志文件的本地路径
        String path = "/Users/duanjiaxing/data/10000_access.log";


        //文件输入流
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(
                                new File(path))
                ));

        //为防止每一次都创建对象，所以再循环的外面创建UA对象
        UserAgentParser userAgentParser = new UserAgentParser();
        String line = "";
        int i = 0;

//        类型名和次数集合
        Map<String, Integer> browserMap = new HashMap<String, Integer>();

        while ((line = reader.readLine()) != null) {
//            line = reader.readLine();

            if (StringUtils.isNotBlank(line)) {
                String source = line.substring(getCharacterPosition(line,"\"",7)+1);
                UserAgent agent = userAgentParser.parse(source);
                String os = agent.getOs();
                String browser = agent.getBrowser();
                String engine = agent.getEngine();
                String engineVersion = agent.getEngineVersion();
                String platform = agent.getPlatform();
                boolean ismobile = agent.isMobile();


                Integer browserValue = browserMap.get(browser);
                if (browserValue != null) {
                    browserMap.put(browser, browserValue + 1);
                } else {
                    browserMap.put(browser, 1);
                }

//                System.out.println(browser+","+ engine+","+engineVersion+","+os+","+platform+","+ismobile);

            }

        }
        for (Map.Entry<String, Integer> entry : browserMap.entrySet()) {

            System.out.println(entry.getKey() + ":" + entry.getValue());
        }

    }


    /**
     * 测试是否成功返回索引
     */
    @Test
    public void testGetCharacterPosition() {
        String value = "10.100.0.1 - - [10/Nov/2016:00:01:03 +0800] \"HEAD / HTTP/1.1\" 301 0 \"117.121.101.40\" \"-\" - \"curl/7.19.7 (x86_64-redhat-linux-gnu) libcurl/7.19.7 NSS/3.16.2.3 Basic ECC zlib/1.2.3 libidn/1.18 libssh2/1.4.2\" \"-\" - - - 0.000";
        int index = getCharacterPosition(value, "\"", 7);
        System.out.println(index);
    }


    /**
     * 获取指定字符串中指定字符串出现的索引位置
     *
     * @param value
     * @param operator
     * @param index
     * @return
     */
    private int getCharacterPosition(String value, String operator, int index) {
        Matcher slashMatcher = Pattern.compile(operator).matcher(value);
        int mIdex = 0;
        while (slashMatcher.find()) {
            mIdex++;
            if (mIdex == index) {
                break;
            }
        }
        return slashMatcher.start();
    }

    @Test
    public void UserAgentTest() {


        String source = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36";

        UserAgentParser userAgentParser = new UserAgentParser();
        UserAgent agent = userAgentParser.parse(source);
        String os = agent.getOs();
        String browser = agent.getBrowser();
        String engine = agent.getEngine();
        String engineVersion = agent.getEngineVersion();
        String platform = agent.getPlatform();
        boolean ismobile = agent.isMobile();

        System.out.println(browser + "," + engine + "," + engineVersion + "," + os + "," + platform + "," + ismobile);

    }

}
