/*
 * Copyright 2025 Raghav Aggarwal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.raghav;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Custom Tez Native Job to generate TPC-DS data in parallel. Usage: hadoop jar tpcds-gen.jar
 * com.example.tez.tpcds.TPCDSGenTez <dsdgen_local_path> <tpcds.idx_local_path> <scale_factor>
 * <parallelism> <hdfs_output_dir>
 */
public class TPCDSGenTez extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(TPCDSGenTez.class);
  private static final String DSDGEN_RESOURCE_NAME = "dsdgen";
  private static final String TPCDS_IDX_RESOURCE_NAME = "tpcds.idx";

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TPCDSGenTez(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 5) {
      System.err.println(
          "Usage: TPCDSGenTez <dsdgen_local_path> <tpcds.idx_local_path> <scale_factor> <parallelism> <hdfs_output_dir>");
      return -1;
    }

    String localDsdgen = args[0];
    String localIdx = args[1];
    int scaleFactor = Integer.parseInt(args[2]);
    int parallelism = Integer.parseInt(args[3]);
    String outputDir = args[4];

    // Ensure we use Tez configuration
    TezConfiguration tezConf = new TezConfiguration(getConf());
    FileSystem fs = FileSystem.get(tezConf);

    // 1. Setup standard Tez Client
    TezClient tezClient = TezClient.create("TPCDS-Generator", tezConf);
    tezClient.start();

    try {
      // 2. Create the DAG
      DAG dag = DAG.create("TPCDS-Gen-DAG-" + scaleFactor + "GB");

      // 3. Prepare LocalResources (dsdgen binary and index)
      // We must upload them to HDFS first so Tez can localize them to workers
      Path stagingDir = new Path(outputDir + "/_staging");
      if (!fs.exists(stagingDir)) fs.mkdirs(stagingDir);

      Map<String, LocalResource> localResources = new TreeMap<>();
      localResources.put(
          DSDGEN_RESOURCE_NAME,
          setupLocalResource(
              fs, new Path(localDsdgen), new Path(stagingDir, DSDGEN_RESOURCE_NAME)));
      localResources.put(
          TPCDS_IDX_RESOURCE_NAME,
          setupLocalResource(
              fs, new Path(localIdx), new Path(stagingDir, TPCDS_IDX_RESOURCE_NAME)));

      // 4. Configure the Processor
      // We pass the job parameters (scale, output dir) as user payload to the processor
      String payloadStr = scaleFactor + "," + parallelism + "," + outputDir;
      byte[] payload = payloadStr.getBytes(StandardCharsets.UTF_8);

      // 5. Create the Vertex
      Vertex genVertex =
          Vertex.create(
                  "GenVertex",
                  ProcessorDescriptor.create(DsdgenProcessor.class.getName())
                      .setUserPayload(UserPayload.create(ByteBuffer.wrap(payload))),
                  parallelism)
              .addTaskLocalFiles(localResources);

      dag.addVertex(genVertex);

      // 6. Submit and wait
      LOG.info("Submitting DAG to generate TPC-DS data...");
      tezClient.waitTillReady();
      DAGClient dagClient = tezClient.submitDAG(dag);
      DAGStatus status = dagClient.waitForCompletion();

      if (status.getState() == DAGStatus.State.SUCCEEDED) {
        LOG.info("TPC-DS Generation DAG succeeded.");
        return 0;
      } else {
        LOG.error("DAG failed with status: " + status.getState());
        return 1;
      }
    } finally {
      tezClient.stop();
    }
  }

  // Helper to upload a local file to HDFS and create a Tez LocalResource record for it
  private LocalResource setupLocalResource(FileSystem fs, Path localPath, Path hdfsPath)
      throws IOException {
    // Copy only if it doesn't exist to save time on repeated runs, or force overwrite if needed.
    // For simplicity here, we overwrite.
    fs.copyFromLocalFile(false, true, localPath, hdfsPath);
    return LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromPath(hdfsPath),
        LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION,
        fs.getFileStatus(hdfsPath).getLen(),
        fs.getFileStatus(hdfsPath).getModificationTime());
  }

  /** The custom Tez Processor that runs on every worker node. */
  public static class DsdgenProcessor extends AbstractLogicalIOProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DsdgenProcessor.class);

    public DsdgenProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void initialize() throws Exception {
      // No complex init needed for this simple case
    }

    @Override
    public void handleEvents(List<Event> processorEvents) {
      // No incoming events expected for a simple generator
    }

    @Override
    public void close() throws Exception {
      // Cleanup if necessary
    }

    @Override
    public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs)
        throws Exception {
      ProcessorContext context = getContext();
      int taskIndex = context.getTaskIndex();
      // TPC-DS child index is 1-based
      int childIndex = taskIndex + 1;

      // 1. Parse Payload
      String payloadStr =
          new String(context.getUserPayload().deepCopyAsArray(), StandardCharsets.UTF_8);
      String[] parts = payloadStr.split(",");
      int scaleFactor = Integer.parseInt(parts[0]);
      int parallelism = Integer.parseInt(parts[1]);
      String hdfsOutputDir = parts[2];

      LOG.info("Starting dsdgen task " + childIndex + "/" + parallelism + " Scale: " + scaleFactor);

      // 2. Setup local working directory for dsdgen output
      // Use unique identifier to avoid conflicts if multiple containers run on same node disks
      File workingDir = new File("tpcds_work_" + context.getUniqueIdentifier());
      if (!workingDir.exists() && !workingDir.mkdirs()) {
        // It might already exist from a previous failed attempt, or mkdirs failed.
        if (!workingDir.isDirectory()) {
          throw new IOException(
              "Could not create local working dir: " + workingDir.getAbsolutePath());
        }
      }

      try {
        // 3. Execute dsdgen binary
        // The binary is localized to the current working directory of the container by YARN
        File dsdgenBin = new File("./" + DSDGEN_RESOURCE_NAME);
        if (!dsdgenBin.canExecute()) {
          dsdgenBin.setExecutable(true);
        }

        ProcessBuilder pb =
            new ProcessBuilder(
                dsdgenBin.getAbsolutePath(),
                "-SCALE",
                String.valueOf(scaleFactor),
                "-PARALLEL",
                String.valueOf(parallelism),
                "-CHILD",
                String.valueOf(childIndex),
                "-DIR",
                workingDir.getAbsolutePath(),
                "-FORCE",
                "Y",
                "-QUIET",
                "Y");

        // Redirect stderr to container logs for debugging
        pb.redirectErrorStream(true);
        Process process = pb.start();

        // Read standard output of the process to keep buffers clear and log progress
        try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()))) {
          String line;
          while ((line = reader.readLine()) != null) {
            LOG.info("dsdgen: " + line);
          }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
          throw new IOException("dsdgen failed with exit code: " + exitCode);
        }

        // 4. Upload generated files to HDFS
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path finalDestRoot = new Path(hdfsOutputDir);

        File[] generatedFiles = workingDir.listFiles((dir, name) -> name.endsWith(".dat"));
        if (generatedFiles != null) {
          for (File f : generatedFiles) {
            // dsdgen outputs files like 'store_sales_1_10.dat' (table_child_parallelism.dat)
            // We want to organize them into HDFS directories: /output/store_sales/data_1_10.dat

            String originalName = f.getName();
            // Simple heuristic to extract table name: everything before the last two underscores
            // e.g., store_sales_1_10.dat -> store_sales
            String tableName = originalName.substring(0, originalName.lastIndexOf('_'));
            tableName = tableName.substring(0, tableName.lastIndexOf('_'));

            Path tableDestDir = new Path(finalDestRoot, tableName);
            if (!fs.exists(tableDestDir)) {
              fs.mkdirs(tableDestDir);
            }

            Path dst = new Path(tableDestDir, originalName);
            LOG.info("Uploading " + f.getAbsolutePath() + " to " + dst);
            fs.copyFromLocalFile(false, true, new Path(f.getAbsolutePath()), dst);
          }
        }

      } finally {
        // Cleanup local working dir to prevent disk fill-up on workers
        deleteLocalDir(workingDir);
      }
      LOG.info("Finished dsdgen task " + childIndex);
    }

    private void deleteLocalDir(File file) {
      File[] contents = file.listFiles();
      if (contents != null) {
        for (File f : contents) {
          deleteLocalDir(f);
        }
      }
      file.delete();
    }
  }
}
