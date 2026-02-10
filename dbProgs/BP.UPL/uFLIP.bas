$INCLUDE I_Prologue
      MSG=""
      OPEN "BP.UPL" TO BP.UPL THEN
         READ DBT FROM BP.UPL, "DBT" ELSE DBT="UV"
         READ STP FROM BP.UPL, "STOP" ELSE STP=""
         IF STP = "stop" THEN MSG = "STOP switch set on"
         WRITE "started" ON BP.UPL, "FLIP.CTL"
      END ELSE
         MSG = "BP.UPL cannot be accessed!"
      END
      IF MSG#"" THEN PRINT MSG ; CALL uLOGGER(DBT, MSG); STOP
      SL="THROW-ERROR"
      IF DBT="UV" THEN SL = "&SAVEDLISTS&"
      IF DBT="UD" THEN SL = "SAVEDLISTS"
***   oCMD = CMD
***   CMD = TRIM(@SENTENCE):" @@ @@ @@ @@ @@ @@ @@ "
***   CONVERT " " TO @FM IN CMD
***   uLST=""; RTYP=""
***   LOCATE("uFLIP", CMD; POS) THEN
***      FILE = CMD<POS+1>
***      KEY = CMD<POS+2>
***      IF INDEX(FILE:KEY, "@@", 1) THEN
***         MSG = "Error in command ":oCMD
***         CALL uLOGGER(DBT, MSG)
***         STOP
***      END
***   END
      OPEN SL TO SLIST ELSE
         MSG = "Cannot open ":SL
         PRINT MSG
         CALL uLOGGER(DBT, MSG)
         STOP
      END
      IF DBT="UV" THEN
         EXECUTE "SET.TERM.TYPE LENGTH 9999" CAPTURING JUNK
      END ELSE
         EXECUTE "UDT.OPTIONS 37 OFF" CAPTURING JUNK
      END
      rQM=2500
      CTR=0
      END.SW=0; NO.SEL=0
      LOOP UNTIL END.SW DO
         EXE = "SELECT ":SL:" WITH @ID LIKE uFLIP..."
         EXECUTE EXE CAPTURING JUNK
         CNT=0
         LOOP
            READNEXT KEY ELSE EXIT
            READU REC FROM SLIST, KEY LOCKED
               GO END..LOOP..B
            END ELSE 
               GO END..LOOP..B
            END
            FILE = FIELD(KEY, "_", 2)
            IF NOT(INDEX(FILE, ".LOADED", 1)) THEN FILE = FILE:".LOADED"
            OPEN FILE TO LOADIO ELSE
               PRINT "Cannot open ":FILE
               GO END..LOOP
            END
            CNT+=1
            EOI = DCOUNT(REC, @FM)
            SHOW.AT = INT(EOI/10)
            SCNT=0
            PRINT TIMEDATE():" >> ":KEY:"   has ":EOI:" records"
            FOR I = 1 TO EOI
               CTR+=1
               SCNT+=1
               LINE = REC<I>
               IF TRIM(LINE)="" THEN CONTINUE
               UID = FIELD(LINE, "!!", 1)
               LNE = LINE[LEN(UID)+3, LEN(LINE)]
               CONVERT TILDE TO CHAR(254) IN LNE
               CONVERT "`" TO CHAR(253) IN LNE
               CONVERT "^" TO CHAR(252) IN LNE
               CALL uMD5(HASH, REC)
               IF HASH#"" THEN 
                  WRITE HASH ON LOADIO, UID
                  CTR+=1
                  IF CTR > rQM THEN 
                     RQM; RQM; RQM; RQM
                     CTR=0
                     READ STP FROM BP.UPL, "STOP" ELSE STP=""
                     IF STP = "stop" THEN MSG = "STOP switch set on"
                  END
               END
               IF (SCNT > SHOW.AT) THEN
                  PRINT TIMEDATE():SCNT "R#12":" flipped"
                  SCNT=0
               END
            NEXT I
            PRINT TIMEDATE():SCNT "R#12":" flipped ---------------------"
END..LOOP:
            DELETE SLIST, KEY
END..LOOP..B:
            RELEASE
         REPEAT
         PRINT TIMEDATE():" --------------------------------------------"
         IF NO.SEL > 10 THEN END.SW=1
         IF CNT=0 THEN NO.SEL+=1; RQM; RQM; RQM; RQM; RQM ELSE NO.SEL=0
      REPEAT
      WRITE "" ON BP.UPL, "FLIP.CTL"
      PRINT TIMEDATE():" finished ******************************************"
      CLOSE
      STOP
   END
