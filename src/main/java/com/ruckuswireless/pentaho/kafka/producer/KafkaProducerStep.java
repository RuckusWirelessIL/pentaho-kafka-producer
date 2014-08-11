package com.ruckuswireless.pentaho.kafka.producer;

import java.util.Map.Entry;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * Kafka Producer step processor
 * 
 * @author Michael Spector
 */
public class KafkaProducerStep extends BaseStep implements StepInterface {

	public KafkaProducerStep(StepMeta stepMeta,
			StepDataInterface stepDataInterface, int copyNr,
			TransMeta transMeta, Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		KafkaProducerData data = (KafkaProducerData) sdi;
		if (data.producer != null) {
			data.producer.close();
			data.producer = null;
		}
		super.dispose(smi, sdi);
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi)
			throws KettleException {
		Object[] r = getRow();
		if (r == null) {
			setOutputDone();
			return false;
		}

		KafkaProducerMeta meta = (KafkaProducerMeta) smi;
		KafkaProducerData data = (KafkaProducerData) sdi;

		RowMetaInterface inputRowMeta = getInputRowMeta();

		if (first) {
			first = false;

			// Initialize Kafka client:
			if (data.producer == null) {
				Properties properties = meta.getKafkaProperties();
				Properties substProperties = new Properties();
				for (Entry<Object, Object> e : properties.entrySet()) {
					substProperties.put(e.getKey(), environmentSubstitute(e
							.getValue().toString()));
				}

				ProducerConfig producerConfig = new ProducerConfig(
						substProperties);
				logBasic(Messages.getString(
						"KafkaProducerStep.CreateKafkaProducer.Message",
						producerConfig.brokerList()));
				data.producer = new Producer<Object, Object>(producerConfig);
			}

			data.outputRowMeta = getInputRowMeta().clone();
			meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

			String inputField = environmentSubstitute(meta.getField());

			int numErrors = 0;
			if (Const.isEmpty(inputField)) {
				logError(Messages
						.getString("KafkaProducerStep.Log.FieldNameIsNull")); //$NON-NLS-1$
				numErrors++;
			}
			data.inputFieldNr = inputRowMeta.indexOfValue(inputField);
			if (data.inputFieldNr < 0) {
				logError(Messages.getString(
						"KafkaProducerStep.Log.CouldntFindField", inputField)); //$NON-NLS-1$
				numErrors++;
			}
			if (!inputRowMeta.getValueMeta(data.inputFieldNr).isBinary()) {
				logError(Messages.getString(
						"KafkaProducerStep.Log.FieldNotValid", inputField)); //$NON-NLS-1$
				numErrors++;
			}
			if (numErrors > 0) {
				setErrors(numErrors);
				stopAll();
				return false;
			}
			data.inputFieldMeta = inputRowMeta.getValueMeta(data.inputFieldNr);
		}

		try {
			byte[] message = data.inputFieldMeta
					.getBinary(r[data.inputFieldNr]);
			String topic = environmentSubstitute(meta.getTopic());

			data.producer
					.send(new KeyedMessage<Object, Object>(topic, message));
			if (isRowLevel()) {
				logRowlevel(Messages.getString(
						"KafkaProducerStep.Log.SendingData", topic,
						data.inputFieldMeta.getString(r[data.inputFieldNr])));
			}
		} catch (KettleException e) {
			if (!getStepMeta().isDoingErrorHandling()) {
				logError(Messages.getString(
						"KafkaProducerStep.ErrorInStepRunning", e.getMessage()));
				setErrors(1);
				stopAll();
				setOutputDone();
				return false;
			}
			putError(getInputRowMeta(), r, 1, e.toString(), null, getStepname());
		}
		return true;
	}

	public void stopRunning(StepMetaInterface smi, StepDataInterface sdi)
			throws KettleException {

		KafkaProducerData data = (KafkaProducerData) sdi;
		data.producer.close();
		data.producer = null;

		super.stopRunning(smi, sdi);
	}
}
