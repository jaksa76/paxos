package paxos;

import com.google.common.io.Files;
import org.junit.Test;

import java.io.File;
import java.nio.charset.Charset;
import java.util.*;

public class LogChecker {
    @Test
    public void testLog() throws Exception {
        List<String> lines = Files.readLines(new File("log.txt"), Charset.defaultCharset());

        Set<String> messagesReceived = new HashSet<String>();
        for (String line : lines) {
            if (line.startsWith("paxos.BufferedReceiver@2acdb06e")) {
                if (line.contains("received")) {
                    String msg = line.substring(line.lastIndexOf('(') + 1, line.lastIndexOf(')'));
                    messagesReceived.add(msg);
                }
            }
        }

        ArrayList<Comparable> list = new ArrayList<Comparable>(messagesReceived);
        Collections.sort(list);
        System.out.println(list.size());
        System.out.println(list);
    }
}
