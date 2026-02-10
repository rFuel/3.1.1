      * -------------------------------------------------------
      * NB: This program is loaded, compiled and run by SetupDB
      *     Purpose: Open files on the remote UV host to test
      *              connectivity. This should be run at the end
      *              of SetupDB.jar and can be run by hand.
      * -------------------------------------------------------
      *
      OPEN "MD" TO MD ELSE STOP "Cannot open MD ???"
      PASS = "PASS"
      FAIL = "FAIL"
      ULINE = "---------------------------------------------------------------------------------------------------"
      SP4 = "    "
      VERBOSE = 0
      FAILED  = 0
      TRY.TO.WRITE = 0
      *
      CMD = EREPLACE(SENTENCE(), " ", @FM)
      IF CMD<1> = "DEBUG" THEN
         CMD = DELETE(CMD, 1,0,0)
         CMD = DELETE(CMD, 1,0,0)
      END
      CHK = UPCASE(CMD)
      LOCATE "-V" IN CHK SETTING vFND THEN
         CMD = DELETE(CMD, vFND,0,0)
         CHK = DELETE(CHK, vFND,0,0)
         VERBOSE = 1
      END
      LOCATE "-W" IN CHK SETTING wFND THEN
         CMD = DELETE(CMD, wFND,0,0)
         CHK = DELETE(CHK, wFND,0,0)
         TRY.TO.WRITE = 1
      END
      LOCATE "-H" IN CHK SETTING wFND THEN
         CRT "Usage: PING {-V} {-W} {-H} {filename}"
         CRT "       -H:  this help message"
         CRT "       -V:  verbose"
         CRT "       -W:  try to write to remote file"
         STOP
      END
      CHK = ""
      *
      XOF = ""
      IF DCOUNT(CMD, @FM) > 1 THEN
         FILE = CMD<DCOUNT(CMD, @FM)>
         READ CHK FROM MD, FILE ELSE CHK = ""
         IF CHK<1>[1,1] = "D" THEN
            * Yup, its a file, defined in the MD
            * good to go
            GOSUB CHECK..THIS..FILE
            IF NOT(FAILED) THEN CRT (PASS:" ":ULINE) "L#60"
         END ELSE
            CRT "Usage: PING {filename} or just PING"
            CRT "[":FILE:"] is not defined in the MD - it is not a file."
         END
      END ELSE 
         GOSUB CHECK..ALL..FILES
      END
      IF VERBOSE THEN CRT ""
      IF VERBOSE THEN CRT ""
      IF VERBOSE THEN CRT "Done."
      STOP
      *
      * -------------------------------------------------------
      *
CHECK..ALL..FILES:
      OPEN "BP.UPL" TO BP.UPL ELSE STOP "Cannot open BP.UPL"
      READ REM.FILES FROM BP.UPL, "REMOTE.FILES" ELSE STOP "REMOTRE.FILES is not in BP.UPL"
      EOF = DCOUNT(REM.FILES, @FM)
      FOR I = 1 TO EOF
         XOF = I "R#2":" of ":EOF
         FILE = REM.FILES<I>
         IF FILE = "" THEN CONTINUE
         READ CHK FROM MD, FILE ELSE CHK = ""
         IF CHK<1>[1,1] = "D" THEN
            GOSUB CHECK..THIS..FILE
            IF NOT(FAILED) THEN CRT (PASS:" ":ULINE) "L#60"
         END ELSE 
            CRT "[":FILE:"] is not defined in the MD - it is not a file."
         END
         IF VERBOSE THEN CRT ""
      NEXT I
      RETURN
      *
      * -------------------------------------------------------
      *
CHECK..THIS..FILE:
      FAILED  = 0
      IF VERBOSE THEN CRT ""
      IF VERBOSE THEN CRT ULINE "L#60"
      CRT ("Checking ":XOF:"    ":FILE:" ":STR('.',40)) "L#60":" "
      IF VERBOSE THEN CRT
      IF VERBOSE THEN CRT "1.  SR.FILE.OPEN"
      ERR = ""
      CALL SR.FILE.OPEN(ERR, FILE, HANDLE)
      IF ERR # "" THEN
         IF VERBOSE THEN
            CRT "    > File Open error: ":ERR
            CRT "    > Check U2-SET and U2-VIEW parameters."
            CRT "    > You may need to DELETE-FILE ":FILE
         END
         CRT SP4:FAIL
         FAILED  = 1
         * no point in doing any more checks
         RETURN
      END ELSE
         IF VERBOSE THEN CRT SP4:PASS
      END
      *
      IF VERBOSE THEN CRT ""
      EXE = "SELECT ":FILE:" (G10"
      IF VERBOSE THEN CRT "2.  Direct access via select + readnext + read"
      IF VERBOSE THEN CRT "    > ":EXE
      EXECUTE EXE CAPTURING OUTPUT RTNLIST SEL.LIST
      OUTPUT = EREPLACE(OUTPUT, @FM, " ")
      IF VERBOSE THEN CRT "    > command output: [":OUTPUT:"]"
      SAVE.ID = ""
      CNT = 0
      IF VERBOSE THEN CRT "    > READNEXT then READ"
      LOOP
         READNEXT ID FROM SEL.LIST ELSE EXIT
         CNT += 1
         READ CHK FROM HANDLE, ID SETTING ERR.CODE ELSE
            IF VERBOSE THEN
               CRT "    > File read error: [":ERR,CODE:"]"
               CRT "    > It may be a problem at the remote end."
               CRT "    > Check U2-SET and U2-VIEW parameters."
               CRT "    > You may need to DELETE-FILE ":FILE
            END
            CRT SP4:FAIL
            FAILED  = 1
            * no point in doing any more checks
            RETURN
         END
         IF SAVE.ID = "" THEN SAVE.ID = ID
      REPEAT
      IF CNT = 10 THEN
         IF VERBOSE THEN CRT SP4:PASS
      END ELSE
         IF VERBOSE THEN CRT "    > requested 10 but read ":CNT:" items."
         IF VERBOSE THEN CRT "    > WARNING on file select test"
      END
      *
      IF TRY.TO.WRITE THEN
         IF VERBOSE THEN CRT ""
         IF VERBOSE THEN CRT "3.  Security check - attempt WRITE operation"
         READ RECORD FROM HANDLE, SAVE.ID SETTING ERR.CODE THEN
            OKAY = 0
            WRITE RECORD ON HANDLE, SAVE.ID ON ERROR OKAY = 1
            IF OKAY THEN
            IF VERBOSE THEN CRT SP4:PASS:"      ... cannot write to ":FILE
            END ELSE
               IF VERBOSE THEN
                  CRT "    > Security test FAIL"
                  CRT "    > Chck U2-VIEW parameter for Update [Y/N] - should be 'N'"
                  CRT "    > Recommend DELETE-FILE ":FILE:"  and redo U2-VIEW"
               END
               CRT SP4:FAIL
               FAILED  = 1
               * no point in doing any more checks
               RETURN
            END
         END ELSE
            IF VERBOSE THEN
               CRT "    > File read error: [":ERR,CODE:"]"
               CRT "    > It may be a problem at the remote end."
               CRT "    > Check U2-SET and U2-VIEW parameters."
               CRT "    > You may need to DELETE-FILE ":FILE
            END
            CRT SP4:FAIL
            FAILED  = 1
            * no point in doing any more checks
            RETURN
         END
      END
      *
      RETURN
   END
