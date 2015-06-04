package com.neverwinterdp.swing.registry;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JToolBar;
import javax.swing.ListSelectionModel;
import javax.swing.table.DefaultTableModel;

import org.jdesktop.swingx.JXTable;
import org.jdesktop.swingx.decorator.ColorHighlighter;
import org.jdesktop.swingx.decorator.HighlightPredicate;
import org.jdesktop.swingx.decorator.HighlighterFactory;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityRegistry;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.scribengin.ScribenginCluster;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

//TODO: this should show a split plane, the above half should show the list of the activities
//The bottom half should show the detail information of an activity (the activity steps) and should reuse UIActivityView
@SuppressWarnings("serial")
public class UIActivitiesView extends SpringLayoutGridJPanel implements UILifecycle {

  private String              activityPath;
  private UIActivityStepsView activityStepsView;

  public UIActivitiesView(String activityPath) {
    this.activityPath = activityPath;
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
        }
      });
      addRow(toolbar);

      DataflowActivityJXTable activityListTable = new DataflowActivityJXTable(activityPath);
      activityStepsView = new UIActivityStepsView();
      JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(
          activityListTable), new JScrollPane(activityStepsView));
      splitPane.setOneTouchExpandable(true);
      splitPane.setDividerLocation(150);

      addRow(splitPane);
    }
    makeCompactGrid();
  }

  @Override
  public void onDeactivate() throws Exception {
    clear();
  }

  class DataflowActivityJXTable extends JXTable {
    public DataflowActivityJXTable(final String activityPath) throws Exception {
      setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
      DataflowActivityTableModel model = new DataflowActivityTableModel(activityPath);
      setModel(model);
      model.loadData();

      setVisibleRowCount(30);
      setVisibleColumnCount(8);
      setHorizontalScrollEnabled(true);
      setColumnControlVisible(true);
      addMouseListener(new MouseAdapter() {
        public void mouseClicked(MouseEvent e) {
          DataflowActivityTableModel model = (DataflowActivityTableModel) getModel();
          Activity activity = model.getRowAt(getSelectedRow());
          String rootPath = activityPath.substring(0, activityPath.lastIndexOf("/"));
          String path = rootPath + "/all/" + activity.getId() + "/activity-steps";
          try {
            activityStepsView.refresh(path);
          } catch (Exception exp) {
            MessageUtil.handleError(exp);
          }
        }
      });
      setHighlighters(HighlighterFactory.createSimpleStriping());
      addHighlighter(new ColorHighlighter(HighlightPredicate.ROLLOVER_ROW, Color.BLACK, Color.WHITE));
    }
  }

  static class DataflowActivityTableModel extends DefaultTableModel {
    static String[] COLUMNS = { "Id", "description", "type" };
    String          activityPath;
    List<Activity>  activities;

    public DataflowActivityTableModel(String activityPath) {
      super(COLUMNS, 0);
      this.activityPath = activityPath;
    }

    public Activity getRowAt(int rowIndex) {
      return activities.get(rowIndex);
    }

    void loadData() throws Exception {
      System.out.println("load data from path : " + activityPath);
      activities = getActivitiesList(activityPath);
      Collections.sort(this.activities);
      for (Activity activity : activities) {
        Object[] cells = { activity.getId(), activity.getDescription(), activity.getType() };
        addRow(cells);
      }
    }

    private List<Activity> getActivitiesList(String activityPath) throws RegistryException {
      Registry registry = ScribenginCluster.getCurrentInstance().getRegistry();
      String rootPath = activityPath.substring(0, activityPath.lastIndexOf("/"));
      List<String> paths = new ArrayList<String>();
      for (String childName : registry.getChildren(activityPath)) {
        paths.add(rootPath + "/all/" + childName);
      }
      return ActivityRegistry.getActivities(registry, paths);
    }
  }
}
