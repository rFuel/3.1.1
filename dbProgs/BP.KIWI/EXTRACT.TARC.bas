0001:       PROMPT ""
0002:       *
0003:       * EXTRACT.TARC {mode} {list}
0004:       *
0005:       OPEN "uSCRIPTS" TO SCRIPTIO ELSE STOP "uSCRIPTS Open error"
0006:       OPEN "DICT", "uSCRIPTS" TO SCRIPT_D ELSE STOP "uSCRIPTS Open error"
0007:       UCMD = @SENTENCE
0008:       UCMD = EREPLACE(UCMD, " ", @FM)
0009:       LOCATE("EXTRACT.TARC", UCMD; FND) ELSE STOP
0010:       MODE = UCMD<FND+1>
0011:       IF MODE = "" THEN MODE = "READY"
0012:       LIST = UCMD<FND+2>
0013:       *
0014:       PIF = "S4"                         ; * Product In Focus
0015:       CURL = "/opt/freeware/bin/curl -u admin:admin -d 'body=$$' "
0016:       CURL:= '"http://10.24.83.100:8161/api/message/010_Start_001'
0017:       CURL:= '?type=queue"'
0018:       MSG = ""
0019:       MSG:= "TASK<is>010<tm>"
0020:       MSG:= "SQLTABLE<is>TRAN_ARC<tm>"
0021:       MSG:= "DACCT<is>MASK<tm>"
0022:       MSG:= "REPLYTO<is>019_Finish<tm>"
0023:       MSG:= "SELECT<is>SELECT $$ LIKE ...S4/... OR LIKE ...S4....<tm>"
0024:       MSG:= "ITEM<is>*<tm>"
0025:       MSG:= "COLPFX<is>F_<tm>"
0026:       MSG:= "U2FILE<is>$$<tm>"
0027:       MSG:= "SCHEMA<is>poc<tm>"
0028:       MSG:= "PROCEED<is>true<tm>"
0029:       MSG:= "CORRELATIONID<is>$$-Extract<tm>"
0030:       MSG:= "RAWSEL<is>WHERE srcFile = '\''$$'\''<tm>"
0031:       MSG:= "RUNTYPE<is>INCR<tm>"
0032:       MSG:= "SQLDB<is>rFuelExtract<tm>"
0033:       MSG:= "MAP<is>s4/shell.map<tm>"
0034:       MSG:= "LIST<is>dsd/TRAN.ARC.dsd<tm>"
0035:       MAX = 20
0036:       IF LIST # "" THEN
0037:          EXECUTE "GET-LIST ":LIST CAPTURING JUNK
0038:       END ELSE
0039:          EXECUTE "SELECT TARC.TODO" CAPTURING JUNK
0040:       END
0041:       CRT "Extract groups of ":MAX:" files from a list of ":@SELECTED:"  in 5 minute intervals"
0042:       MASTER.STOP = 0
0043:       LOOP UNTIL MASTER.STOP DO
0044:          FNO = 1
0045:          LOOP
0046:             IF FNO > MAX THEN EXIT
0047:             READNEXT ID ELSE MASTER.STOP = 1 ; EXIT
0048:             IF FIELD(ID, "-", 1) # MODE THEN CONTINUE
0049:             ID = FIELD(ID, "-", 2)
0050:             *
0051:             OPEN ID:"_MASK.TAKE" TO TEMPIO ELSE
0052:                EXECUTE "CREATE.FILE ":ID:"_MASK.TAKE 30 64BIT"
0053:                OPEN ID:"_MASK.TAKE" TO TEMPIO ELSE
0054:                   CRT "Cannot create ":ID:"_MASK.TAKE"
0055:                   STOP
0056:                END
0057:             END
0058:             CLOSE TEMPIO
0059:             OPEN ID:"_MASK.LOADED" TO TEMPIO ELSE
0060:                EXECUTE "CREATE.FILE ":ID:"_MASK.LOADED 30 64BIT"
0061:                OPEN ID:"_MASK.LOADED" TO TEMPIO ELSE
0062:                   CRT "Cannot create ":ID:"_MASK.LOADED"
0063:                   STOP
0064:                END
0065:             END
0066:             CLOSE TEMPIO
0067:             *
0068:             MESSAGE = EREPLACE(MSG, "$$", ID)
0069:             COMMAND = EREPLACE(CURL, "$$", MESSAGE)
0070:             WRITE COMMAND ON SCRIPTIO, "sendtorfuel.sh"
0071:             EXE = 'sh -c"./uSCRIPTS/sendtorfuel.sh"'
0072:             EXECUTE EXE CAPTURING JUNK
0073:             IF INDEX(JUNK, "Message sent", 1) THEN
0074:                EXE = "CNAME TARC.TODO READY-":ID:", DONE-":ID
0075:                EXECUTE EXE CAPTURING JUNK
0076:                EXE = "CNAME TARC.TODO ":ID:", DONE-":ID
0077:                EXECUTE EXE CAPTURING JUNK
0078:             END
0079:             CRT FNO, ID
0080:             FNO += 1
0081:          REPEAT
0082:          EXECUTE "CLEARSELECT" CAPTURING JUNK
0083:          IF FNO > 1 THEN FNO -= 1
0084:          CRT "Done: ":FNO:" files sent to rFuel"
0085:          READV WAIT.TIME FROM SCRIPT_D, "WAIT", 1 ELSE WAIT.TIME = 300
0086:          WAIT.TIME += 0
0087:          IF NOT(MASTER.STOP) THEN SLEEP WAIT.TIME
0088:       REPEAT
0089:       STOP
0090:    END
