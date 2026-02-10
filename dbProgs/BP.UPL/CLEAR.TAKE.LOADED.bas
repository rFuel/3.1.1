      * CLEAR.TAKE.LOADED file [file] [file]
      * Pass in as many file names as needed (without source account or TAKE/LOADED)
      * It will check the TAKE and LOADED files exist or create them.
      * It then clears the TAKE and LOADED files.
      * 02-02-2026: ------------------------------------------------------------------------
      * Use BP.UPL and not UPL.CONTROL 
      *
      * in the future, KB may have different source accounts with different migrations
      * enable use of account*file
      * this change allows;
      *     1. existing preuni commands to remain unchanged
      *     2. new ones to use account*file 
      * ------------------------------------------------------------------------------------
      *
      PROMPT ""
      OPEN "VOC"    TO VOC     ELSE STOP "BP.UPL is missing"
      OPEN "BP.UPL" TO CONTROL ELSE STOP "BP.UPL is missing"
      READ ACCT.LIST FROM CONTROL, "@SOURCE.ACCOUNT" ELSE STOP "@SOUCE.ACCOUNT is missing from BP.UPL"
      EOA = DCOUNT(ACCT.LIST, @FM)
      *
      CHK.VOC = "upl_CTL_CHECK"
      CMD = EREPLACE(@SENTENCE, " ", @FM)
      LOCATE("CLEAR.TAKE.LOADED", CMD; FND) ELSE STOP
      FND += 1
      FILES = ""
      EOL = DCOUNT(CMD, @FM)
      FOR I = FND TO EOL
         FILES<-1> = CMD<I>
      NEXT I
      * --------------------------------------------------------------
      J = 0
      LOOP
         J += 1
         IF J > EOA THEN EXIT
         SRC.ACCT = ACCT.LIST<J>
         IF SRC.ACCT = "" THEN CONTINUE
         * ----------------------------------------------------------
         QREC = "Q":@FM:SRC.ACCT:@FM:"VOC"
         WRITE QREC ON VOC, CHK.VOC
         OPEN CHK.VOC TO OKAYIO THEN EXISTS=1 ELSE EXISTS=0
         CLOSE OKAYIO
         OKAYIO = ""
         IF NOT(EXISTS) THEN CONTINUE
         * ----------------------------------------------------------
         I = 0
         LOOP
            I += 1
            IF FILES<I> = "" THEN EXIT
            * -------------------------------------------------------
            FNAME = FILES<I>:"_":SRC.ACCT:".TAKE"
            IF INDEX(FNAME, "*", 1) THEN
               FNAME = FIELD(FILES<I>, "*", 2):"_":FIELD(FILES<I>, "*", 1):".TAKE"
            END
            CALL SR.FILE.OPEN(ERR, FNAME, CHECKIO)
            IF ERR = "" THEN
               EXE = "CLEAR.FILE ":FNAME
               EXECUTE EXE CAPTURING JUNK
               CALL SR.FILE.CLOSE(ERR, FNAME)
            END ELSE
               EXE = "CREATE.FILE ":FNAME:" 30 64BIT"
               EXECUTE EXE CAPTURING JUNK
            END
            * -------------------------------------------------------
            FNAME = FILES<I>:"_":SRC.ACCT:".LOADED"
            IF INDEX(FNAME, "*", 1) THEN
               FNAME = FIELD(FILES<I>, "*", 2):"_":FIELD(FILES<I>, "*", 1):".TAKE"
            END
            CALL SR.FILE.OPEN(ERR, FNAME, CHECKIO)
            IF ERR = "" THEN
               EXE = "CLEAR.FILE ":FNAME
               EXECUTE EXE CAPTURING JUNK
               CALL SR.FILE.CLOSE(ERR, FNAME)
            END ELSE
               EXE = "CREATE.FILE ":FNAME:" 30 64BIT"
               EXECUTE EXE CAPTURING JUNK
            END
         REPEAT
      REPEAT
      STOP
   END
