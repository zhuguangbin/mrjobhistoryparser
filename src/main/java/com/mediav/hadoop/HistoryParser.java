
package com.mediav.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/*
 */
public class HistoryParser extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(JobClient.class);

  static {
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  /**
   * Display usage of the command-line tool and terminate execution
   */
  private void displayUsage(String cmd) {
    String prefix = "Usage: HistoryParser ";
    System.err.println(prefix + "[" + cmd + " <jobOutputDir> ]");
  }

  public int run(String[] argv) throws Exception {
    int exitCode = -1;
    String outputDir = null;
    if (argv.length != 1) {
      displayUsage("");
      return exitCode;
    } else {
      outputDir = argv[0];
    }
    viewHistory(outputDir);
    exitCode = 0;

    return exitCode;
  }

  private void viewHistory(String outputDir)
          throws IOException {
    MVHistoryViewer historyViewer = new MVHistoryViewer(outputDir, getConf());
    historyViewer.printJobInfoLoop();
  }

  /**
   */
  public static void main(String argv[]) throws Exception {
    int res = ToolRunner.run(new HistoryParser(), argv);
    System.exit(res);
  }

}

