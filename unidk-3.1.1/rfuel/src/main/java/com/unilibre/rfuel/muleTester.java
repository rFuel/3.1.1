package com.unilibre.rfuel;


import com.unilibre.commons.msgCommons;
import com.unilibre.commons.uCommons;
import okhttp3.*;
import java.io.IOException;
import java.util.ArrayList;

public class muleTester {

    public static ArrayList<String> messages = new ArrayList<>();
    private static String host = "";
    private static boolean skipIt = false;

    public static void main(String[] args) {

        host = System.getProperty("host", "NO-HOST");
        skipIt = System.getProperty("skip", "").toLowerCase().equals("true");
        uCommons.uSendMessage("*");
        uCommons.uSendMessage("*");
        uCommons.uSendMessage("----------------------------------------------------------------------------------");
        uCommons.uSendMessage("rFuel test of Anypoint connector");
        uCommons.uSendMessage("process task=[any]");
        uCommons.uSendMessage("     on  que=[any]");
        uCommons.uSendMessage("Starting muleTester()");
        uCommons.uSendMessage("=================================================");
        uCommons.uSendMessage("*");

        BuildMessagePool();

        boolean doHttp = true;
        if (doHttp) {
            new muleTester();
        } else {
            DoCurl();
        }
        System.exit(0);
    }

    public muleTester() {
        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        OkHttpClient client = new OkHttpClient();
        String tmp, tmp1, pfx = "        ", url = "http://"+host+":8081/jms";
        System.out.println("Posting to " + url);
        int eom = messages.size(), lx;

        for (int m=0; m<eom; m++) {
            lx = String.valueOf(m).length();
            tmp1 = pfx.substring(lx, pfx.length());
            tmp = msgCommons.jsonifyMessage(messages.get(m));
            System.out.println(tmp1 + m + ": " + tmp);
            if (skipIt) continue;
            RequestBody body = RequestBody.create(JSON, tmp);
            Request request = new Request.Builder()
                    .url(url)
                    .post(body)
                    .build();
            Call call = client.newCall(request);
            try {
                Response response = call.execute();
                System.out.println("response: " + response.body().string());
                response.close();
                response = null;
            } catch (IOException e) {
                tmp = e.getMessage();
                if (e.getCause() != null) tmp += "  " + e.getCause();
                System.out.println("   error: " + tmp);
                tmp = "";
            }
            request = null;
            body = null;
            call = null;
        }
        client = null;
        JSON = null;
    }

    private static void DoCurl() {
        boolean isWindows = false, showOutput=true;
        String opsys = System.getProperty("os.name");
        if (opsys.toLowerCase().contains("windows")) isWindows = true;

        String command = "curl -X POST  -H \"Content-Type: application/json\" ";
        String http = " \"http://"+ host +":8081/jms\" ";
        String cmd, tmp;
        int eom = messages.size();
        for (int m=0; m<eom; m++) {

            tmp = msgCommons.jsonifyMessage(messages.get(m));
            if (isWindows) {
                tmp = tmp.replaceAll("\"","\\\\\"");
            }
            cmd = command + " -d \"" + tmp + "\" " + http;
            uCommons.nixExecute(cmd, true);
        }
    }

    public static void BuildMessagePool() {
        messages = new ArrayList<>();
        messages.add("task<is>055<tm>Correlationid<is>Clear-Test-Files-SRTN<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/PrepareTestFiles.msv<tm>payload<is><tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>format<is>json<tm>Correlationid<is>Create-uCATALOG-uCLRTEST<tm>replyto<is>TestResults<tm>use.$file$<is>uCATALOG<tm>mscat<is>UPLQA/AllPurposeUpdater.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"uCLRTEST\",\"RECORD\":\"exec-x-CLR.TESTFILES\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>format<is>json<tm>Correlationid<is>Create-VOC-CLR.TESTFILES<tm>replyto<is>TestResults<tm>use.$file$<is>VOC<tm>mscat<is>UPLQA/AllPurposeUpdater.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"CLR.TESTFILES\",\"RECORD\":\"PA@FM@HUSH ON@FM@CLEAR.FILE TEST.DATA@FM@CLEAR.FILE TEST.DATA2@FM@HUSH OFF\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>TRUE<tm>\n");
        messages.add("task<is>055<tm>Correlationid<is>Clear-Test-Files-EXEC<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/ClearTestFiles.msv<tm>payload<is><tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>1<tm>format<is>json<tm>Correlationid<is>Create-Test-Record<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord.msv<tm>payload<is>{\"InBound\": {\"BaseData\": {\"ID\": \"1\",\"Firstname\": \"MyFirstName\",\"Email\": \"create.me@unilibre.com.au\",\"DoB\": \"12-11-1971\",\"Phone\": \"0400112233\",\"Surname\": \"MyFamilyName\",\"CardNumber\": \"1111222233334444\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>TRUE<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA2<tm>use.$mv$<is>1<tm>format<is>json<tm>Correlationid<is>Create-Test-JOIN-Record<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord.msv<tm>payload<is>{\"InBound\": {\"BaseData\": {\"ID\": \"1\",\"Surname\": \"Join has worked\",\"Email\": \"\",\"DoB\": \"\",\"Phone\": \"\",\"Firstname\": \"100@VM@200@VM@300@VM@400\",\"CardNumber\": \"\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>1<tm>format<is>json<tm>Correlationid<is>Create-Test-Record-2<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord-2.msv<tm>payload<is>{\"InBound\": {\"BaseData\": {\"ID\": \"2\",\"Alias\": \"0412123456@VM@my.email@payid.com.au\",\"Status\": \"ACTIVE@VM@INACTIVE\",\"Type\": \"TYPE1@VM@TYPE2\",\"Method\": \"sms@VM@email\",\"StatusReason1\": \"InUse@VM@Closed\",\"StatusReason2\": \"@VM@\",\"Account\": \"S33@VM@S11\",\"BSB\": \"880-601@VM@880-601\",\"ExtAccount\": \"123456@VM@654321\",\"cDate\": \"2018-01-20@VM@2018-01-21\",\"cTime\": \"10:01@VM@09:10\",\"lastUsedDate\": \"2018-05-12@VM@2018-05-13\",\"lastUsedTime\": \"15:25@VM@11:20\",\"xferDate\": \"2018-05-12@VM@2018-05-13\",\"xferTime\": \"15:20@VM@11:15\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>1<tm>format<is>json<tm>Correlationid<is>Create-Test-Record-1-M-M<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord-3.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"3\",\"Company\":\"Unilbre\",\"StaffID\":\"1@VM@2\",\"Name\":\"Consultant1@VM@Consultant2\",\"Contact\":\"0400112233@SM@consultant1@unilibre.com.au@VM@0433221100\",\"ContactType\":\"mobile@SM@email@VM@mobile\",\"Location\":\"Brisbane@VM@Perth\",\"State\":\"QLD@VM@WA\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>1<tm>format<is>json<tm>Correlationid<is>Create-Test-Record-5<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord-2.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"5\",\"Alias\":\"0412123456\",\"Status\":\"ACTIVE\",\"Type\":\"TYPE1\",\"Method\":\"sms\",\"StatusReason1\":\"InUse\",\"StatusReason2\":\"\",\"Account\":\"S33\",\"BSB\":\"880-601\",\"ExtAccount\":\"123456\",\"cDate\":\"2018-01-20\",\"cTime\":\"10:01\",\"lastUsedDate\":\"2018-05-12\",\"lastUsedTime\":\"15:25\",\"xferDate\":\"2018-05-12\",\"xferTime\":\"15:20\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>1<tm>format<is>json<tm>Correlationid<is>Create-Test-Record-11<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"11\",\"Firstname\":\"MyFirstName\",\"Email\":\"create.me@unilibre.com.au\",\"DoB\":\"12-11-1971\",\"Phone\":\"0400112233\",\"Surname\":\"MyFamilyName\",\"CardNumber\":\"1111222233334444\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>1<tm>format<is>json<tm>Correlationid<is>Create-PUID-Test-Record<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"50\",\"Firstname\":\"Given1@VM@Given2@VM@Given3@VM@Given4\",\"DoB\":\"PUID1@VM@PUID2\",\"Surname\":\"Surname1@VM@Surname2\",}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>050<tm>format<is>xml<tm>map<is>UPLQA/uRest/ReadTestRecord.map<tm>item<is>1<tm>use.$mv$<is>N<tm>showlineage<is>true<tm>replyto<is>TestResults<tm>wraptask<is>true<tm>correlationid<is>1stReadTest<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>-1<tm>format<is>json<tm>Correlationid<is>Minus-1-Append<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"1\",\"Firstname\":\"MyBasicAppend\",\"Email\":\"append@unilibre.com.au\",\"DoB\":\"12-11-1972\",\"Phone\":\"0400445566\",\"Surname\":\"AnotherFamily\",\"CardNumber\":\"11221133113441155\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>050<tm>format<is>xml<tm>map<is>UPLQA/uRest/ReadTestRecord.map<tm>item<is>1<tm>use.$mv$<is>N<tm>use.$sv$<is>N<tm>use.$av$<is>N<tm>correlationid<is>ReadAfterAppend<tm>showlineage<is>true<tm>replyto<is>TestResults<tm>wraptask<is>true<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>1<tm>u2p.subs.vm<is>~VM~<tm>format<is>json<tm>Correlationid<is>Insert-Values-Test<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"1\",\"Firstname\":\"~VM~MyInsertedName\",\"Email\":\"~VM~inserted@unilibre.com.au\",\"DoB\":\"~VM~12-11-1972\",\"Phone\":\"~VM~0400778899\",\"Surname\":\"~VM~\",\"CardNumber\":\"~VM~11229933993449955\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>050<tm>format<is>xml<tm>map<is>UPLQA/uRest/ReadTestRecord.map<tm>item<is>1<tm>use.$mv$<is>N<tm>correlationid<is>ReadAfterInsert-NOassoc-Dense<tm>showlineage<is>true<tm>replyto<is>TestResults<tm>wraptask<is>true<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>050<tm>format<is>xml<tm>map<is>UPLQA/uRest/ReadTestRecord.map<tm>item<is>1<tm>use.$mv$<is>N<tm>correlationid<is>ReadAfterInsert-NOassoc-Sparse<tm>showlineage<is>true<tm>replyto<is>TestResults<tm>wraptask<is>true<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>sparse<is>true<tm>\n");
        messages.add("task<is>050<tm>format<is>xml<tm>map<is>UPLQA/uRest/assocReadTestRecord.map<tm>item<is>1<tm>use.$mv$<is>N<tm>correlationid<is>ReadAfterInsert-Assoc-Sparse<tm>showlineage<is>true<tm>replyto<is>TestResults<tm>wraptask<is>true<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>sparse<is>true<tm>\n");
        messages.add("task<is>050<tm>format<is>xml<tm>map<is>UPLQA/uRest/assocReadTestRecord.map<tm>item<is>1<tm>use.$mv$<is>N<tm>correlationid<is>ReadAfterInsert-Assoc-Dense<tm>showlineage<is>true<tm>replyto<is>TestResults<tm>wraptask<is>true<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>sparse<is>false<tm>\n");
        messages.add("task<is>050<tm>use.$MV$<is>1<tm>replyto<is>TestResults<tm>correlationID<is>BasicRead-Text<tm>map<is>UPLQA/uRest/BasicText.map<tm>item<is>11<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>050<tm>use.$MV$<is>1<tm>replyto<is>TestResults<tm>correlationID<is>BasicRead-XML<tm>map<is>UPLQA/uRest/BasicRead-XML.map<tm>item<is>11<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>050<tm>use.$MV$<is>1<tm>replyto<is>TestResults<tm>format<is>json<tm>correlationID<is>BasicRead-JSON<tm>map<is>UPLQA/uRest/BasicRead-JSON.map<tm>item<is>11<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>050<tm>replyto<is>TestResults<tm>correlationID<is>Nested-1-1<tm>map<is>UPLQA/uRest/Nested.map<tm>item<is>5<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>050<tm>replyto<is>TestResults<tm>correlationID<is>Nested-1-m<tm>map<is>UPLQA/uRest/Nested.map<tm>item<is>2<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>050<tm>replyto<is>TestResults<tm>correlationID<is>Nested-1-m-m-Dense<tm>map<is>UPLQA/uRest/Nested1MM.map<tm>item<is>3<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>050<tm>replyto<is>TestResults<tm>correlationID<is>Nested-1-m-m-Sparse<tm>map<is>UPLQA/uRest/Nested1MM.map<tm>item<is>3<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>sparse<is>true<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>055<tm>sHost<is>src/aws2-rfuel<tm>correlationID<is>BasicExecute<tm>replyto<is>TestResults<tm>format<is>xml<tm>mscat<is>UPLQA/SubrTest.msv<tm>payload<is><InBound><BaseData><ID>11</ID><Surname>Unilibre</Surname></BaseData></InBound><tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>050<tm>formt<is>xml<tm>map<is>UPLQA/uRest/ReadFunctionTest.map<tm>item<is>1<tm>use.$mv$<is>1<tm>sparse<is>true<tm>showlineage<is>false<tm>replyto<is>TestResults<tm>wraptask<is>true<tm>correlationid<is>ReadFunctionTest<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>050<tm>sHost<is>src/aws2-rfuel<tm>sparse<is>true<tm>replyto<is>TestResults<tm>correlationID<is>SparseValues<tm>map<is>UPLQA/uRest/SparseValues.map<tm>item<is>50<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>050<tm>wraptask<is>false<tm>sHost<is>src/aws2-data<tm>replyto<is>TestResults<tm>correlationID<is>Cross-Account-Reader<tm>map<is>UPLQA/uRest/UF0001-Reader.map<tm>item<is>1299<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>showlineage<is>true<tm>sparse<is>true<tm>\n");
        messages.add("task<is>055<tm>format<is>xml<tm>mscat<is>UPLQA/DirtyUpdate.msv<tm>correlationID<is>Dirty-Updates-off-xml<tm>replyTo<is>TestResults<tm>debug<is>false<tm>payload<is><?xml version=\"1.0\" encoding=\"UTF-8\"?><Heartbeat><ID>pulse-6ef51311-c763-4df6-bc7d-a6af942ccad4</ID><date>2018-10-17</date><time>15:08:53</time></Heartbeat><tm>u2p.dirty.updates<is>0<tm>u2p.allow.append<is>false<tm>\n");
        messages.add("task<is>055<tm>format<is>xml<tm>mscat<is>UPLQA/DirtyUpdate.msv<tm>correlationID<is>Dirty-Updates-off-json<tm>replyTo<is>TestResults<tm>debug<is>false<tm>payload<is>{\"Heartbeat\":{\"ID\":\"pulse-6ef51311-c763-4df6-bc7d-a6af942ccad4\",\"date\":\"2018-10-17\",\"time\":\"15:08:53\"}}<tm>u2p.dirty.updates<is>0<tm>u2p.allow.append<is>false<tm>\n");
        messages.add("task<is>055<tm><tm>format<is>json<tm>Correlationid<is>Subroutine-Q-Pointer<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/Q-Pointer-SRTN.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"Account\":\"DATA\",\"File\":\"CLIENT\",\"Qpointer\":\"A.CRAZY.QPOINTER\",\"ID\":\"51\"}}}<tm>\n");

        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$op$<is>+<tm>format<is>json<tm>Correlationid<is>Valid-Math(+)-Write<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/MathTester.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"51\",\"Value\":\"323\"}}}<tm>u2p.dirty.updates<is>true<tm>u2p.allow.append<is>false<tm>\n");
        messages.add("task<is>050<tm>replyto<is>TestResults<tm>correlationID<is>Valid-Math(+)-Read<tm>map<is>UPLQA/uRest/MathReader.map<tm>item<is>51<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$op$<is>-<tm>format<is>json<tm>Correlationid<is>alid-Math(-)<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/MathTester.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"51\",\"Value\":\"111\"}}}<tm>u2p.dirty.updates<is>true<tm>u2p.allow.append<is>false<tm>\n");
        messages.add("task<is>050<tm>replyto<is>TestResults<tm>correlationID<is>Valid-Math(-)<tm>map<is>UPLQA/uRest/MathReader.map<tm>item<is>51<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$op$<is>*<tm>format<is>json<tm>Correlationid<is>MathTester<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/MathTester.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"51\",\"Value\":\"14\"}}}<tm>u2p.dirty.updates<is>true<tm>u2p.allow.append<is>false<tm>\n");
        messages.add("task<is>050<tm>replyto<is>TestResults<tm>correlationID<is>Valid-Math(*)<tm>map<is>UPLQA/uRest/MathReader.map<tm>item<is>51<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$op$<is>/<tm>format<is>json<tm>Correlationid<is>MathTester<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/MathTester.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"51\",\"Value\":\"14\"}}}<tm>u2p.dirty.updates<is>true<tm>u2p.allow.append<is>false<tm>\n");
        messages.add("task<is>050<tm>replyto<is>TestResults<tm>correlationID<is>Valid-Math(*)<tm>map<is>UPLQA/uRest/MathReader.map<tm>item<is>51<tm>wraptask<is>false<tm>");
    }

}
