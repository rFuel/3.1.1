      PROMPT ""
      *
      * RUN BP.UPL TARC.SCRIPTS
      *
      OPEN "uSCRIPTS" TO SCRIPTIO ELSE STOP "uSCRUIPTS Open error"
      OPEN "DICT", "uSCRIPTS" TO SCRIPT_D ELSE STOP "uSCRUIPTS Open error"
      *
      PIF = "S4"                         ; * Product In Focus
      CURL = "curl -u admin:admin -d 'body=$$' "
      CURL:= '"http://10.24.83.100:8161/api/message/010_Start_001'
      CURL:= '?type=queue"'
      MSG = ""
      MSG:= "TASK<is>010<tm>"
      MSG:= "RUNTYPE<is>INCR<tm>"
      MSG:= "DACCT<is>MASK<tm>"
      MSG:= "U2FILE<is>$$<tm>"
      MSG:= "ITEM<is>*<tm>"
      MSG:= "COLPFX<is>F_<tm>"
      MSG:= "REPLYTO<is>019_Finish<tm>"
      MSG:= "SELECT<is>SELECT $$ LIKE ...S4/... OR LIKE ...S4....<tm>"
      MSG:= "RAWSEL<is>WHERE srcFile = '\''$$'\''<tm>"
      MSG:= "SQLDB<is>rFuelExtract<tm>"
      MSG:= "SCHEMA<is>poc<tm>"
      MSG:= "SQLTABLE<is>TRAN.ARC<tm>"                                  ;*  ALWAYS TRAN.ARC
      MSG:= "PROCEED<is>true<tm>"
      MSG:= "CORRELATIONID<is>Extract-$$<tm>"
      MSG:= "MAP<is>s4/shell.map<tm>"
      MSG:= "LIST<is>dsd/TRAN.ARC.dsd<tm>"                              ;*  ALWAYS TRAN.ARC.dsd
      *
      EXECUTE "SELECT TARC.TODO" CAPTURING JUNK
      CRT
      CRT
      CRT "Create rFuel messages for all TARC files in TARC.TODO"
      CRT "-----------------------------------------------------"
      *
      LOGDESC = 'echo sending request : '
      SCRIPT = ""
      CNT = 0
      LOOP
         READNEXT ID ELSE EXIT
         ID = FIELD(ID, "-", 2)
         CNT += 1
         *
         OPEN ID:"_MASK.TAKE" TO TEMPIO ELSE
            EXECUTE "CREATE.FILE ":ID:"_MASK.TAKE 30 64BIT"
            OPEN ID:"_MASK.TAKE" TO TEMPIO ELSE
               CRT "Cannot create ":ID:"_MASK.TAKE"
               STOP
            END
         END
         CLOSE TEMPIO
         OPEN ID:"_MASK.LOADED" TO TEMPIO ELSE
            EXECUTE "CREATE.FILE ":ID:"_MASK.LOADED 30 64BIT"
            OPEN ID:"_MASK.LOADED" TO TEMPIO ELSE
               CRT "Cannot create ":ID:"_MASK.LOADED"
               STOP
            END
         END
         CLOSE TEMPIO
         *
         MESSAGE = EREPLACE(MSG, "$$", ID)
         COMMAND = EREPLACE(CURL, "$$", MESSAGE)
         SCRIPT<-1> = LOGDESC:OCONV(CNT, "MD0,") "R#6":"   ":ID
         SCRIPT<-1> = COMMAND:@FM:"sleep 2"
      REPEAT
      WRITE SCRIPT ON SCRIPTIO, "TRAN.ARC.sh"
      EXECUTE "CLEARSELECT" CAPTURING JUNK
      CRT "Done: "
      STOP
   END
