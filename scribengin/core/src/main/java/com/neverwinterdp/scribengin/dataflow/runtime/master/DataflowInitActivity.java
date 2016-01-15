package com.neverwinterdp.scribengin.dataflow.runtime.master;

import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataSetDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.OperatorDescriptor;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowTaskRegistry;
import com.neverwinterdp.scribengin.dataflow.registry.OperatorRegistry;
import com.neverwinterdp.scribengin.dataflow.registry.StreamRegistry;
import com.neverwinterdp.scribengin.dataflow.runtime.DataStreamOperatorDescriptor;
import com.neverwinterdp.storage.PartitionStreamConfig;
import com.neverwinterdp.storage.Storage;
import com.neverwinterdp.storage.StorageConfig;
import com.neverwinterdp.storage.StorageService;
import com.neverwinterdp.storage.sink.Sink;

public class DataflowInitActivity implements DataflowMasterActivity {
  private MasterService service;
  
  public DataflowInitActivity(MasterService service) {
    this.service = service;
  }
  
  public void execute() throws Exception {
    initStreams();
    initOperators();
    initDataStreamOperatorTasks();
  }

  void initStreams() throws Exception {
    DataflowRegistry dflRegistry = service.getDataflowRegistry();
    StreamRegistry streamRegistry = dflRegistry.getStreamRegistry();
    StorageService storageService = service.getStorageService();
    DataflowDescriptor dflConfig = dflRegistry.getConfigRegistry().getDataflowDescriptor();
    DataSetDescriptor streamConfig  = dflConfig.getStreamConfig();
    Map<String, StorageConfig> storageConfigs = streamConfig.getStreams();
    for(Map.Entry<String, StorageConfig> entry : storageConfigs.entrySet()) {
      String name = entry.getKey();
      StorageConfig storageConfig = entry.getValue();
      Storage storage = storageService.getStorage(storageConfig);
      storage.refresh();
      if(!storage.exists()) {
        storageConfig.setPartitionStream(streamConfig.getParallelism());
        storageConfig.setReplication(streamConfig.getReplication());
        storage.create();
        storage.refresh();
      }
      Sink sink = storage.getSink();
      List<PartitionStreamConfig> pConfigs = sink.getPartitionStreamConfigs();
      streamRegistry.create(name, storageConfig, pConfigs);
    }
  }
  
  void initOperators() throws Exception {
    DataflowRegistry dflRegistry = service.getDataflowRegistry();
    OperatorRegistry operatorRegistry = dflRegistry.getOperatorRegistry();
    DataflowDescriptor dflConfig = dflRegistry.getConfigRegistry().getDataflowDescriptor();
    Map<String, OperatorDescriptor> operators = dflConfig.getOperators();
    for(Map.Entry<String, OperatorDescriptor> entry : operators.entrySet()) {
      String name = entry.getKey();
      OperatorDescriptor config = entry.getValue();
      operatorRegistry.create(name, config);
    }
  }

  void initDataStreamOperatorTasks() throws Exception {
    DataflowRegistry dflRegistry = service.getDataflowRegistry();
    DataflowDescriptor dflDescriptor = dflRegistry.getConfigRegistry().getDataflowDescriptor();
    Map<String, OperatorDescriptor> operators = dflDescriptor.getOperators();
    for(Map.Entry<String, OperatorDescriptor> entry : operators.entrySet()) {
      String opName = entry.getKey();
      OperatorDescriptor opDescriptor = entry.getValue();
      createDataStreamOperator(opName, opDescriptor);
    }
    dflRegistry.setRegistryReadyStatus();
  }
  
  void createDataStreamOperator(String opName, OperatorDescriptor opDescriptor) throws RegistryException {
    DecimalFormat SEQ_ID_FORMATTER = new DecimalFormat("0000");
    StreamRegistry streamRegistry = service.getDataflowRegistry().getStreamRegistry();
    DataflowTaskRegistry taskRegistry = service.getDataflowRegistry().getTaskRegistry();
    for(String input : opDescriptor.getInputs()) {
      List<PartitionStreamConfig> pConfigs = streamRegistry.getStreamInputPartitions(input);
      for(int i = 0; i < pConfigs.size(); i++) {
        PartitionStreamConfig pConfig = pConfigs.get(i);
        String taskId =  opName + ":" + input + "-" + SEQ_ID_FORMATTER.format(pConfig.getPartitionStreamId());
        DataStreamOperatorDescriptor dsOperatorDescriptor = new DataStreamOperatorDescriptor();
        dsOperatorDescriptor.setTaskId(taskId);
        dsOperatorDescriptor.setOperatorName(opName);
        dsOperatorDescriptor.setInput(input);
        dsOperatorDescriptor.setInputPartitionId(pConfig.getPartitionStreamId());
        dsOperatorDescriptor.setOutputs(opDescriptor.getOutputs());
        dsOperatorDescriptor.setOperator(opDescriptor.getOperator());
        dsOperatorDescriptor.setInterceptors(opDescriptor.getInterceptors());
        taskRegistry.offer(dsOperatorDescriptor);
      }
    }
  }
}