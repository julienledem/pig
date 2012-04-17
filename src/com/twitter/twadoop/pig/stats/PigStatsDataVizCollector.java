package com.twitter.twadoop.pig.stats;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.tools.pigstats.*;

import java.io.IOException;
import java.util.*;

/**
 * Hacked up code for how we can gather stats as jobs kick off and expose them in a web UI
 */
public class PigStatsDataVizCollector implements PigProgressNotificationListener {
  protected Log LOG = LogFactory.getLog(getClass());

  private List<JobInfo> jobInfoList = new ArrayList<JobInfo>();

  private String scriptFingerprint;
  private Map<String, DAGNode> dagNodeNameMap = new HashMap<String, DAGNode>();

  public PigStatsDataVizCollector() { }

  @Override
  public void initialPlanNotification(MROperPlan plan) {
    Map<OperatorKey, MapReduceOper>  planKeys = plan.getKeys();
    for (Map.Entry<OperatorKey, MapReduceOper> entry : planKeys.entrySet()) {
      //TODO: represent the edges in the DAG
      //TODO: expose as JSON
      DAGNode node = new DAGNode(entry.getKey(), entry.getValue());
      dagNodeNameMap.put(node.getName(), node);

      // this shows how we can get the basic info about all nameless jobs before any execute.
      // we can traverse the plan to build a DAG of this info
      LOG.info("initialPlanNotification: alias: " + node.getAlias()
              + ", name: " + node.getName() + ", feature: " + node.getFeature());
    }
  }

  @Override
  public void jobStartedNotification(String scriptId, String assignedJobId) {
    PigStats.JobGraph jobGraph = PigStats.get().getJobGraph();
      LOG.info("jobStartedNotification - jobId " + assignedJobId + ", jobGraph:\n" + jobGraph);

    // this verifies that later when a job gets kicked off we can associate the jobId with the scope
    for (JobStats jobStats : jobGraph) {
      if (assignedJobId.equals(jobStats.getJobId())) {
        LOG.info("jobStartedNotification - scope " + jobStats.getName() + " is jobId " + assignedJobId);
        if (dagNodeNameMap.get(jobStats.getName()) == null) {
          LOG.warn("jobStartedNotification - unrecorgnized operator name found ("
                  + jobStats.getName() + ") for jobId " + assignedJobId);
        } else {
          dagNodeNameMap.get(jobStats.getName()).setJobId(assignedJobId);
        }
      }
    }
  }

  /**
   * Class that represents a Node in the DAG. This class can be converted to JSON as-is by doing
   * something like this:
   * ObjectMapper om = new ObjectMapper();
   * om.getSerializationConfig().set(SerializationConfig.Feature.INDENT_OUTPUT, true);
   * String json = om.writeValueAsString(dagNode);
   */
  private static class DAGNode {
      private String name;
      private String alias;
      private String feature;
      private String jobId;

      private DAGNode(OperatorKey operatorKey, MapReduceOper mapReduceOper) {
          this.name = operatorKey.toString();
          this.alias = ScriptState.get().getAlias(mapReduceOper);
          this.feature = ScriptState.get().getPigFeature(mapReduceOper);
      }

      public String getName() { return name; }
      public String getAlias() { return alias; }
      public String getFeature() { return feature; }

      public String getJobId() { return jobId; }
      public void setJobId(String jobId) { jobId = jobId; }
  }

  /* The code below is all borrowed from my stats collection work. We'd change it to our needs */

  @Override
  public void jobFailedNotification(String scriptId, JobStats stats) {
    collectStats(scriptId, stats);
  }

  @Override
  public void jobFinishedNotification(String scriptId, JobStats stats) {
    collectStats(scriptId, stats);
  }

  @Override
  public void launchCompletedNotification(String scriptId, int numJobsSucceeded) {
    if (scriptFingerprint == null) {
      LOG.warn("scriptFingerprint not set for this script - not saving stats." );
      return;
    }

    ScriptStats scriptStats = new ScriptStats(scriptId, scriptFingerprint, jobInfoList);

    try {
      outputStatsData(scriptStats);
    } catch (IOException e) {
      LOG.error("Exception outputting scriptStats", e);
    }
  }

  public void outputStatsData(ScriptStats scriptStats) throws IOException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Collected stats for script:\n" + ScriptStats.toJSON(scriptStats));
    }
  }

  /**
   * Collects statistics from JobStats and builds a nested Map of values. Subsclass ond override
   * if you'd like to generate different stats.
   *
   * @param scriptId
   * @param stats
   * @return
   */
  protected void collectStats(String scriptId, JobStats stats) {

    // put the job conf into a Properties object so we can serialize them
    Properties jobConfProperties = new Properties();
    if (stats.getInputs() != null && stats.getInputs().size() > 0 &&
      stats.getInputs().get(0).getConf() != null) {

      Configuration conf = stats.getInputs().get(0).getConf();
      for (Map.Entry<String, String> entry : conf) {
        jobConfProperties.setProperty(entry.getKey(), entry.getValue());
      }

      if (scriptFingerprint == null)  {
        scriptFingerprint = conf.get("pig.logical.plan.signature");
      }
    }

    jobInfoList.add(new JobInfo(stats, jobConfProperties));
  }

  @Override
  public void jobsSubmittedNotification(String scriptId, int numJobsSubmitted) { }

  @Override
  public void launchStartedNotification(String scriptId, int numJobsToLaunch) { }

  @Override
  public void outputCompletedNotification(String scriptId, OutputStats outputStats) { }

  @Override
  public void progressUpdatedNotification(String scriptId, int progress) { }

  private static Properties filterProperties(Properties input, String... prefixes) {
    Properties filtered = new Properties();
    for(String key : input.stringPropertyNames()) {
      for (String prefix : prefixes) {
        if (key.startsWith(prefix)) {
          filtered.setProperty(key, input.get(key).toString());
          break;
        }
      }
    }
    return filtered;
  }
}