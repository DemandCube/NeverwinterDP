package com.neverwinterdp.swing.scribengin.dataflow;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.JScrollPane;
import javax.swing.JToolBar;
import javax.swing.ListSelectionModel;
import javax.swing.table.DefaultTableModel;

import org.jdesktop.swingx.JXTable;
import org.jdesktop.swingx.decorator.ColorHighlighter;
import org.jdesktop.swingx.decorator.HighlightPredicate;
import org.jdesktop.swingx.decorator.HighlighterFactory;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskRuntimeReport;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.scribengin.ScribenginCluster;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;
import com.neverwinterdp.util.text.DateUtil;

@SuppressWarnings("serial")
public class UIDataflowTaskReportView extends SpringLayoutGridJPanel implements UILifecycle {
  private String dataflowPath;
  private DataflowTaskReportJXTable taskReportTable;

  public UIDataflowTaskReportView(String dataflowPath) {
    this.dataflowPath = dataflowPath;
  }

  @Override
  public void onInit() throws Exception {
  }

  @Override
  public void onDestroy() throws Exception {
  }

  @Override
  public void onActivate() throws Exception {
    clear();
    Registry registry = ScribenginCluster.getCurrentInstance().getRegistry();
    if (registry == null) {
      addRow("No Registry Connection");
    } else {
      JToolBar toolbar = new JToolBar();
      toolbar.setFloatable(false);
      toolbar.add(new AbstractAction("Reload") {
        @Override
        public void actionPerformed(ActionEvent e) {
          taskReportTable.onRefresh();
        }
      });
      addRow(toolbar);

      taskReportTable = new DataflowTaskReportJXTable(dataflowPath);
      addRow(new JScrollPane(taskReportTable));
    }
    makeCompactGrid();
  }

  @Override
  public void onDeactivate() throws Exception {
    clear();
  }

  public class DataflowTaskReportJXTable extends JXTable {
    public DataflowTaskReportJXTable(String dataflowPath) throws Exception {
      setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
      DataflowTaskReportTableModel model = new DataflowTaskReportTableModel(dataflowPath);
      setModel(model);

      setVisibleRowCount(30);
      setVisibleColumnCount(8);
      setHorizontalScrollEnabled(true);
      setColumnControlVisible(true);
      addMouseListener(new MouseAdapter() {
        public void mouseClicked(MouseEvent e) {
          DataflowTaskReportTableModel model = (DataflowTaskReportTableModel) getModel();
          DataflowTaskRuntimeReport report = model.getDataflowTaskReportAt(getSelectedRow());
        }
      });
      setHighlighters(HighlighterFactory.createSimpleStriping());
      addHighlighter(new ColorHighlighter(HighlightPredicate.ROLLOVER_ROW, Color.BLACK, Color.WHITE));
    }
    
    public void onRefresh()  {
      DataflowTaskReportTableModel model = (DataflowTaskReportTableModel) getModel();
      try {
        model.onRefresh();
      } catch (Exception e) {
        MessageUtil.handleError("Cannot Reload The Report Information", e);
      }
    }
  }

  static class DataflowTaskReportTableModel extends DefaultTableModel {
    static String[] COLUMNS = {"Id", "Process", "Commit", "Start Time", "Finish Time"};

    String dataflowPath; 
    List<DataflowTaskRuntimeReport> reports;

    public DataflowTaskReportTableModel(String dataflowPath) throws Exception {
      super(COLUMNS, 0);
      this.dataflowPath = dataflowPath;
      onRefresh();
    }

    public DataflowTaskRuntimeReport getDataflowTaskReportAt(int selectedRow) {
      return reports.get(selectedRow);
    }

    public void onRefresh() throws Exception {
      Registry registry = ScribenginCluster.getCurrentInstance().getRegistry();
      reports = DataflowRegistry.getDataflowTaskRuntimeReports(registry, dataflowPath);
      //TODO: fix sort
      //Collections.sort(reports, DataflowTaskReport.COMPARATOR);
      getDataVector().clear();
      loadData();
      fireTableDataChanged();
    }
    
    void loadData() throws Exception {
      for(DataflowTaskRuntimeReport selRuntimeReport : reports) {
        DataflowTaskReport selReport = selRuntimeReport.getReport();
        Object[] cells = {
            selReport.getTaskId(), selReport.getProcessCount(), selReport.getAccCommitProcessCount(), 
            DateUtil.asCompactDateTime(selReport.getStartTime()), DateUtil.asCompactDateTime(selReport.getFinishTime())
        };
        addRow(cells);
      }
    }
  }
}