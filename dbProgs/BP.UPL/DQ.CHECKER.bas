      PROMPT ""
      OPEN "BP.UPL" TO CONTROL ELSE STOP "CONTROL open error"
      ERRORS = ""
      WRITE ERRORS ON CONTROL, "DQ.ERRORS"
      FILES = ""
      CMD = EREPLACE(@SENTENCE, " ", @FM)
      LOCATE("DQ.CHECKER", CMD; FND) ELSE STOP "????"
      FND += 1
      EOL = DCOUNT(CMD, @FM)
      FOR I = FND TO EOL
         FILES<-1> = CMD<I>
      NEXT I
      F=0
      LOOP
         F += 1
         FNAME = FILES<F>
      UNTIL FNAME = "" DO
         *
         * Get a list of files to inspect
         *
         IF INDEX(FNAME, "*", 1) > 0 THEN
            * e.g. TRAN.ARC*
            GOSUB SELECT..FILES
         END ELSE
            FLIST = FNAME
         END
         EOL = DCOUNT(FLIST, @FM)
         FOR I = 1 TO EOL
            OPEN FLIST<I> TO IOFILE ELSE CRT FLIST<I>:" cannot be opened" ; CONTINUE
            CRT FLIST<I>
            SELECT IOFILE
            LOOP
               READNEXT ID ELSE EXIT
               READ REC FROM IOFILE, ID ELSE CONTINUE
               FOR C = 0 TO 31
                  IF INDEX(REC, CHAR(C), 1) > 0 THEN
                     ERRORS<-1> = FLIST<I>:@VM:ID:@VM:C
                     CRT " ", ID, C
                  END
               NEXT C
            REPEAT
            WRITEV ERRORS ON CONTROL, "DQ.ERRORS", -1
            ERRORS = ""
            CLOSE IOFILE
         NEXT I
      REPEAT
      CLOSE
      CRT "Done."
      STOP
SELECT..FILES:
      FL = EREPLACE(FNAME, "*", "...")
      EXE = "SELECT VOC LIKE ":FL
      EXECUTE EXE CAPTURING JUNK
      FLIST=""
      LOOP
         READNEXT ID ELSE EXIT
         FLIST<-1> = ID
      REPEAT
      RETURN
   END
