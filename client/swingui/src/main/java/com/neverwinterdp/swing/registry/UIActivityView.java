package com.neverwinterdp.swing.registry;

import java.awt.Component;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;
import javax.swing.JScrollPane;
import javax.swing.JToolBar;

import org.jdesktop.swingx.JXTaskPane;
import org.jdesktop.swingx.JXTaskPaneContainer;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.scribengin.ScribenginCluster;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIActivityView extends SpringLayoutGridJPanel implements UILifecycle {
  private String             activitiesRootPath;
  private String             activityNodeName;
  private ActivityInfoPanel  activityInfoPanel;
  private ActivityStepsPanel activityStepsPanel;

  // This should take a nodepath to filter the type of activity?
  public UIActivityView(String activityNodePath, String activityNodeName) {
    this.activitiesRootPath = activityNodePath.substring(0,
        activityNodePath.lastIndexOf("/activities") + "/activities".length());
    this.activityNodeName = activityNodeName;
  }

  public UIActivityView() {

  }

  @Override
  public void onInit() throws Exception {
  }

  @Override
  public void onDestroy() throws Exception {
  }

  @Override
  public void onActivate() throws Exception {
    refresh(activitiesRootPath, activityNodeName);
  }

  @Override
  public void onDeactivate() throws Exception {
    clear();
  }

  public void refresh(String activitiesRootPath, String activityNodeName) throws RegistryException {
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

      JXTaskPaneContainer tpc = new JXTaskPaneContainer();

      // adding ActivityInfoPanel into JXTaskPaneContainer
      activityInfoPanel = new ActivityInfoPanel();
      tpc.add(getJXTaskPane(activityInfoPanel, "Activity Info", false));

      // adding ActivityStepsPanel into JXTaskPaneContainer
      activityStepsPanel = new ActivityStepsPanel();
      tpc.add(getJXTaskPane(activityStepsPanel, "Activity Steps", true));

      addRow(new JScrollPane(tpc));
      String activityPath = activitiesRootPath + "/all/" + activityNodeName;
      Activity activity = registry.getDataAs(activityPath, Activity.class);
      activityInfoPanel.update(activity);
      activityStepsPanel.update(activitiesRootPath, activityNodeName);
    }
    revalidate();
    makeCompactGrid();
  }

  private JXTaskPane getJXTaskPane(Component obj, String name, boolean collapsed) {
    JXTaskPane jxTaskPane = new JXTaskPane(name);
    jxTaskPane.setName(name);
    jxTaskPane.setCollapsed(collapsed);
    jxTaskPane.add(obj);
    return jxTaskPane;
  }

  class ActivityInfoPanel extends SpringLayoutGridJPanel {
    public ActivityInfoPanel(Activity activity) {
      update(activity);
    }

    public ActivityInfoPanel() {
    }

    public void update(Activity activity) {
      clear();
      if (activity == null) {
        addRow("Select one of the activities above to view its details. ");
      } else {
        addRow("Activity id: ", activity.getId());
        addRow("Description", activity.getDescription());
        addRow("Step builder: ", activity.getActivityStepBuilder());
        addRow("Coordinator", activity.getCoordinator());
        addRow("Type: ", activity.getType());
      }
      makeCompactGrid();
      revalidate();
    }
  }

  class ActivityStepsPanel extends SpringLayoutGridJPanel {
    public ActivityStepsPanel(String activitiesRootPath, String activityNodeName) {
      update(activitiesRootPath, activityNodeName);
    }

    public ActivityStepsPanel() {
    }

    public void update(String activitiesRootPath, String activityNodeName) {
      UIActivityStepsView activityStepsView = new UIActivityStepsView();
      addRow(new JScrollPane(activityStepsView));
      try {
        activityStepsView.refresh(activitiesRootPath + "/all/" + activityNodeName
            + "/activity-steps");
      } catch (Exception e) {
        MessageUtil.handleError(e);
      }
      makeCompactGrid();
    }
  }

}
