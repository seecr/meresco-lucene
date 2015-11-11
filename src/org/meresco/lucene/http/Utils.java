package org.meresco.lucene.http;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URLDecoder;
import java.util.ArrayList;


class Utils {
    public static QueryParameters parseQS(String queryString) {
        /*
         * shamelessly copied from: http://stackoverflow.com/questions/1667278/parsing-queryString-strings-in-java
         */
        QueryParameters params = new QueryParameters();
        if (queryString == null) {
            return params;
        }

        for (String param : queryString.split("&")) {
            if (param.indexOf('=') > 0) {
                String[] pair = param.split("=");
                try {
                    String key = URLDecoder.decode(pair[0], "UTF-8");
                    String value = URLDecoder.decode(pair[1], "UTF-8");
                    ArrayList<String> values = params.get(key);
                    if (values == null) {
                        values = new ArrayList<String>();
                        params.put(key, values);
                    }
                    values.add(value);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return params;
    }
    
    public static String getStackTrace(Throwable aThrowable) {
        /*
         * shameless partial copy from:
         * http://www.javapractices.com/topic/TopicAction.do?Id=78
         */
        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(result);
        aThrowable.printStackTrace(printWriter);
        return result.toString();
    }

    public static void writeToStream(String response, OutputStream stream) {
        try {
            Writer writer = new BufferedWriter(new OutputStreamWriter(stream, "UTF-8"));
            writer.write(response, 0, response.length());
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
