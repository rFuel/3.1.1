package com.unilibre.rfuel;

import com.unilibre.commons.APImsg;
import com.unilibre.commons.uCommons;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

public class BufferedMessage {
    public Message message;
    public long timestamp;
    public String queueName;
    public String taskPhase;
    public String payload;
    public String corr;

    public BufferedMessage(Message message, long timestamp, String queueName, String taskPhase) {
        this.message = message;
        this.timestamp = timestamp;
        this.queueName = queueName;
        this.taskPhase = taskPhase;
        try {
            if (message instanceof TextMessage) {
                this.payload = ((TextMessage) message).getText();
            } else {
                this.payload = message.toString();  // Fallback
            }
        } catch (JMSException e) {
            this.payload = "<<error extracting payload>>";
        }
        this.payload = this.payload.replaceAll("\\r?\\n", "");
        APImsg.instantiate();
        uCommons.MessageToAPI(this.payload);  // assuming you extract it
        this.corr = APImsg.APIget("correlationid");
    }

}