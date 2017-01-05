package com.ruckuswireless.pentaho.kafka.producer;

import java.util.Arrays;
import java.util.Properties;
import java.util.TreeSet;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * UI for the Kafka Producer step
 *
 * @author Michael Spector
 */
public class KafkaProducerDialog extends BaseStepDialog implements StepDialogInterface {

	private KafkaProducerMeta producerMeta;
	private TextVar wTopicName;
	private CCombo wMessageField;
	private CCombo wKeyField;
	private TableView wProps;

	public KafkaProducerDialog(Shell parent, Object in, TransMeta tr, String sname) {
		super(parent, (BaseStepMeta) in, tr, sname);
		producerMeta = (KafkaProducerMeta) in;
	}

	public KafkaProducerDialog(Shell parent, BaseStepMeta baseStepMeta, TransMeta transMeta, String stepname) {
		super(parent, baseStepMeta, transMeta, stepname);
		producerMeta = (KafkaProducerMeta) baseStepMeta;
	}

	public KafkaProducerDialog(Shell parent, int nr, BaseStepMeta in, TransMeta tr) {
		super(parent, nr, in, tr);
		producerMeta = (KafkaProducerMeta) in;
	}

	public String open() {
		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
		props.setLook(shell);
		setShellImage(shell, producerMeta);

		ModifyListener lsMod = new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				producerMeta.setChanged();
			}
		};
		changed = producerMeta.hasChanged();

		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(Messages.getString("KafkaProducerDialog.Shell.Title"));

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Step name
		wlStepname = new Label(shell, SWT.RIGHT);
		wlStepname.setText(Messages.getString("KafkaProducerDialog.StepName.Label"));
		props.setLook(wlStepname);
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right = new FormAttachment(middle, -margin);
		fdlStepname.top = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top = new FormAttachment(0, margin);
		fdStepname.right = new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);
		Control lastControl = wStepname;

		// Topic name
		Label wlTopicName = new Label(shell, SWT.RIGHT);
		wlTopicName.setText(Messages.getString("KafkaProducerDialog.TopicName.Label"));
		props.setLook(wlTopicName);
		FormData fdlTopicName = new FormData();
		fdlTopicName.top = new FormAttachment(lastControl, margin);
		fdlTopicName.left = new FormAttachment(0, 0);
		fdlTopicName.right = new FormAttachment(middle, -margin);
		wlTopicName.setLayoutData(fdlTopicName);
		wTopicName = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wTopicName);
		wTopicName.addModifyListener(lsMod);
		FormData fdTopicName = new FormData();
		fdTopicName.top = new FormAttachment(lastControl, margin);
		fdTopicName.left = new FormAttachment(middle, 0);
		fdTopicName.right = new FormAttachment(100, 0);
		wTopicName.setLayoutData(fdTopicName);
		lastControl = wTopicName;

		RowMetaInterface previousFields;
		try {
			previousFields = transMeta.getPrevStepFields(stepMeta);
		} catch (KettleStepException e) {
			new ErrorDialog(shell, BaseMessages.getString("System.Dialog.Error.Title"),
					Messages.getString("KafkaProducerDialog.ErrorDialog.UnableToGetInputFields"), e);
			previousFields = new RowMeta();
		}

		// Message field
		Label wlMessageField = new Label(shell, SWT.RIGHT);
		wlMessageField.setText(Messages.getString("KafkaProducerDialog.MessageFieldName.Label"));
		props.setLook(wlMessageField);
		FormData fdlMessageField = new FormData();
		fdlMessageField.top = new FormAttachment(lastControl, margin);
		fdlMessageField.left = new FormAttachment(0, 0);
		fdlMessageField.right = new FormAttachment(middle, -margin);
		wlMessageField.setLayoutData(fdlMessageField);
		wMessageField = new CCombo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wMessageField.setItems(previousFields.getFieldNames());
		props.setLook(wMessageField);
		wMessageField.addModifyListener(lsMod);
		FormData fdMessageField = new FormData();
		fdMessageField.top = new FormAttachment(lastControl, margin);
		fdMessageField.left = new FormAttachment(middle, 0);
		fdMessageField.right = new FormAttachment(100, 0);
		wMessageField.setLayoutData(fdMessageField);
		lastControl = wMessageField;

		// Key Field
		Label wlKeyField = new Label(shell, SWT.RIGHT);
		wlKeyField.setText(Messages.getString("KafkaProducerDialog.KeyFieldName.Label"));
		props.setLook(wlKeyField);
		FormData fdlKeyField = new FormData();
		fdlKeyField.top = new FormAttachment(lastControl, margin);
		fdlKeyField.left = new FormAttachment(0, 0);
		fdlKeyField.right = new FormAttachment(middle, -margin);
		wlKeyField.setLayoutData(fdlKeyField);
		wKeyField = new CCombo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wKeyField.setItems(previousFields.getFieldNames());
		props.setLook(wKeyField);
		wKeyField.addModifyListener(lsMod);
		FormData fdKeyField = new FormData();
		fdKeyField.top = new FormAttachment(lastControl, margin);
		fdKeyField.left = new FormAttachment(middle, 0);
		fdKeyField.right = new FormAttachment(100, 0);
		wKeyField.setLayoutData(fdKeyField);
		lastControl = wKeyField;

		// Buttons
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString("System.Button.OK")); //$NON-NLS-1$
		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString("System.Button.Cancel")); //$NON-NLS-1$

		setButtonPositions(new Button[] { wOK, wCancel }, margin, null);

		// Kafka properties
		ColumnInfo[] colinf = new ColumnInfo[] {
				new ColumnInfo(Messages.getString("KafkaProducerDialog.TableView.NameCol.Label"),
						ColumnInfo.COLUMN_TYPE_TEXT, false),
				new ColumnInfo(Messages.getString("KafkaProducerDialog.TableView.ValueCol.Label"),
						ColumnInfo.COLUMN_TYPE_TEXT, false), };

		wProps = new TableView(transMeta, shell, SWT.FULL_SELECTION | SWT.MULTI, colinf, 1, lsMod, props);
		FormData fdProps = new FormData();
		fdProps.top = new FormAttachment(lastControl, margin * 2);
		fdProps.bottom = new FormAttachment(wOK, -margin * 2);
		fdProps.left = new FormAttachment(0, 0);
		fdProps.right = new FormAttachment(100, 0);
		wProps.setLayoutData(fdProps);

		// Add listeners
		lsCancel = new Listener() {
			public void handleEvent(Event e) {
				cancel();
			}
		};
		lsOK = new Listener() {
			public void handleEvent(Event e) {
				ok();
			}
		};
		wCancel.addListener(SWT.Selection, lsCancel);
		wOK.addListener(SWT.Selection, lsOK);

		lsDef = new SelectionAdapter() {
			public void widgetDefaultSelected(SelectionEvent e) {
				ok();
			}
		};
		wStepname.addSelectionListener(lsDef);
		wTopicName.addSelectionListener(lsDef);
		wMessageField.addSelectionListener(lsDef);
		wKeyField.addSelectionListener(lsDef);

		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(new ShellAdapter() {
			public void shellClosed(ShellEvent e) {
				cancel();
			}
		});

		// Set the shell size, based upon previous time...
		setSize(shell, 400, 350, true);

		getData(producerMeta, true);
		producerMeta.setChanged(changed);

		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
		return stepname;
	}

	/**
	 * Copy information from the meta-data input to the dialog fields.
	 */
	private void getData(KafkaProducerMeta producerMeta, boolean copyStepname) {
		if (copyStepname) {
			wStepname.setText(stepname);
		}
		wTopicName.setText(Const.NVL(producerMeta.getTopic(), ""));
		wMessageField.setText(Const.NVL(producerMeta.getMessageField(), ""));
		wKeyField.setText(Const.NVL(producerMeta.getKeyField(), ""));

		TreeSet<String> propNames = new TreeSet<String>();
		propNames.addAll(Arrays.asList(KafkaProducerMeta.KAFKA_PROPERTIES_NAMES));
		propNames.addAll(producerMeta.getKafkaProperties().stringPropertyNames());

		Properties kafkaProperties = producerMeta.getKafkaProperties();
		int i = 0;
		for (String propName : propNames) {
			String value = kafkaProperties.getProperty(propName);
			TableItem item = new TableItem(wProps.table, i++ > 1 ? SWT.BOLD : SWT.NONE);
			int colnr = 1;
			item.setText(colnr++, Const.NVL(propName, ""));
			String defaultValue = KafkaProducerMeta.KAFKA_PROPERTIES_DEFAULTS.get(propName);
			if (defaultValue == null) {
				defaultValue = "(default)";
			}
			item.setText(colnr++, Const.NVL(value, defaultValue));
		}

		wProps.removeEmptyRows();
		wProps.setRowNums();
		wProps.optWidth(true);

		wStepname.selectAll();
	}

	private void cancel() {
		stepname = null;
		producerMeta.setChanged(changed);
		dispose();
	}

	/**
	 * Copy information from the dialog fields to the meta-data input
	 */
	private void setData(KafkaProducerMeta producerMeta) {
		producerMeta.setTopic(wTopicName.getText());
		producerMeta.setMessageField(wMessageField.getText());
		producerMeta.setKeyField(wKeyField.getText());

		Properties kafkaProperties = producerMeta.getKafkaProperties();
		int nrNonEmptyFields = wProps.nrNonEmpty();
		for (int i = 0; i < nrNonEmptyFields; i++) {
			TableItem item = wProps.getNonEmpty(i);
			int colnr = 1;
			String name = item.getText(colnr++);
			String value = item.getText(colnr++).trim();
			if (value.length() > 0 && !"(default)".equals(value)) {
				kafkaProperties.put(name, value);
			} else {
				kafkaProperties.remove(name);
			}
		}
		wProps.removeEmptyRows();
		wProps.setRowNums();
		wProps.optWidth(true);

		producerMeta.setChanged();
	}

	private void ok() {
		if (KafkaProducerMeta.isEmpty(wStepname.getText())) {
			return;
		}
		setData(producerMeta);
		stepname = wStepname.getText();
		dispose();
	}
}
