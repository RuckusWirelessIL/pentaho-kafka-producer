package com.ruckuswireless.pentaho.kafka.producer;

import java.io.UnsupportedEncodingException;
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
	private final static byte[] getUTFBytes(String source) {
		try {
			return source.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			return null;
		}
	}

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

			int numErrors = 0;

			String messageField = environmentSubstitute(meta.getMessageField());

			if (Const.isEmpty(messageField)) {
				logError(Messages
						.getString("KafkaProducerStep.Log.MessageFieldNameIsNull")); //$NON-NLS-1$
				numErrors++;
			}
			data.messageFieldNr = inputRowMeta.indexOfValue(messageField);
			if (data.messageFieldNr < 0) {
				logError(Messages.getString(
						"KafkaProducerStep.Log.CouldntFindField", messageField)); //$NON-NLS-1$
				numErrors++;
			}
			if (!inputRowMeta.getValueMeta(data.messageFieldNr).isBinary() &&
				!inputRowMeta.getValueMeta(data.messageFieldNr).isString()) {
				logError(Messages.getString(
						"KafkaProducerStep.Log.FieldNotValid", messageField)); //$NON-NLS-1$
				numErrors++;
			}
			data.messageIsString = inputRowMeta.getValueMeta(data.messageFieldNr).isString();
			data.messageFieldMeta = inputRowMeta.getValueMeta(data.messageFieldNr);


			String keyField = environmentSubstitute(meta.getKeyField());

			if (! Const.isEmpty(keyField)) {
				logBasic(Messages.getString("KafkaProducerStep.Log.UsingKey",
											keyField));

				data.keyFieldNr = inputRowMeta.indexOfValue(keyField);

				if (data.keyFieldNr < 0) {
					logError(Messages.getString(
												"KafkaProducerStep.Log.CouldntFindField", keyField)); //$NON-NLS-1$
					numErrors++;
				}
				if (!inputRowMeta.getValueMeta(data.keyFieldNr).isBinary() &&
					!inputRowMeta.getValueMeta(data.keyFieldNr).isString()) {
					logError(Messages.getString(
												"KafkaProducerStep.Log.FieldNotValid", keyField)); //$NON-NLS-1$
					numErrors++;
				}
				data.keyIsString = inputRowMeta.getValueMeta(data.keyFieldNr).isString();
				data.keyFieldMeta = inputRowMeta.getValueMeta(data.keyFieldNr);
			}

			if (numErrors > 0) {
				setErrors(numErrors);
				stopAll();
				return false;
			}
		}

		try {
			byte[] message = null;

			if (data.messageIsString) {
				message = getUTFBytes(data.messageFieldMeta.getString(r[data.messageFieldNr]));
			} else {
				message = data.messageFieldMeta.getBinary(r[data.messageFieldNr]);
			}
			String topic = environmentSubstitute(meta.getTopic());

			logBasic(Messages.getString("KafkaProducerStep.Log.SendingData",
					topic));
			if (isRowLevel()) {
				logRowlevel(data.messageFieldMeta.getString(r[data.messageFieldNr]));
			}

			if (data.keyFieldNr < 0) {
				data.producer.send(new KeyedMessage<Object, Object>(topic, message));
			} else {
				byte[] key = null;
				if (data.keyIsString) {
					key = getUTFBytes(data.keyFieldMeta.getString(r[data.keyFieldNr]));
				} else {
					key = data.keyFieldMeta.getBinary(r[data.keyFieldNr]);
				}

				data.producer
					.send(new KeyedMessage<Object, Object>(topic, key, message));
			}

			incrementLinesOutput();
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
		if (data.producer != null) {
			data.producer.close();
			data.producer = null;
		}
		super.stopRunning(smi, sdi);
	}
}
