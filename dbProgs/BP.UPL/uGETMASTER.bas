      DUMMY = @(0,0)
$INCLUDE I_Prologue
      * ---------------------------------------------------------
      * uMASTER
      * ------------------------------
      * 1  Select Statement
            * * 2  Nselect Statement
      * 3  Source or UV Account name  e.g. ACCOUNT
      * 4  Target or Local file name  e.g. ACCOUNT_DATA.TAKE
      * 5  Running  99=idle     01=running
      * 6  Start Time
      * 7  .
      * 8  .
      * 9  Status      1=active    0=inactive
      * 10 Stop flag   1=stop everything
      * ---------------------------------------------------------
      * DICT uMASTER DONCTROL
      * ------------------------------
      * 1  Maximum Phantoms
      * 2  UV Account (data)
      * 3  No Wait  
      * 4  Stop flag   1=stop everything
      * ---------------------------------------------------------
      LOG.KEY = "uGETMASTER":@FM
      LOG.LEVEL=0
      MSG = "Starting: ":TIMEDATE()
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      *
      OPEN "&PH&" TO PH ELSE 
         MSG = "&PH& open error"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
         STOP
      END
      OPEN 'DICT', 'uMASTER' TO MASTER.DICT ELSE
         MSG = "OPEN ERROR - DICT uMASTER"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
         STOP
      END
      OPEN 'uMASTER' TO MASTER.DATA ELSE
         MSG = "OPEN ERROR - uMASTER"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
         STOP
      END
      GOSUB CLEAN..UP
      MIDNIGHT = ICONV('23:59:59', 'MTS')
      STP.FL = ""
      LOOP UNTIL STP.FL # "" DO
         GOSUB GET..FILES
         GOSUB CHECK..CONTROL
         IF STP.FL # "" THEN CONTINUE
         *
         I = 1
         LOOP
            ID = PLIST<I>
            GOSUB CHECK..TIME
         UNTIL ID = '' DO
            GOSUB CLEAN..UP
            GOSUB COUNT..RUNNING         ; * Sets NBR.PH
            READ MREC FROM MASTER.DATA, ID ELSE CONTINUE
            IF MREC<5> = "01" THEN I+=1 ; CONTINUE
            IF MREC<10> = "1" THEN I+=1 ; CONTINUE
            READV FILENAME FROM MASTER.DATA, ID, 3 ELSE
               MSG = "uMASTER item [":ID:"] could not be read"
               CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
               CONTINUE
            END
            MSG = 'Processing (':ID:') ':FILENAME:'  >>    '
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
            GOSUB CHECK..CONTROL
            IF STP.FL THEN GO END..PROG
            IF MREC<7> # '' THEN UVACCT=MREC<3>
            IF NBR.PH < MAX.PH THEN
               EXE = "PHANTOM uGETDATA ":ID:" ":UVACCT:' ':NOWAIT
               CALL uLOGGER(LOG.LEVEL, LOG.KEY:EXE)
               EXECUTE EXE CAPTURING JUNK
               I += 1
            END ELSE
               PRINT MSG:'Max of ':MAX.PH:' reached ... sleeping for a while'
               FOR T.OUT = 1 TO 5
                  GOSUB CLEAN..UP      ;  RQM
                  GOSUB CHECK..CONTROL ;  RQM
               NEXT T.OUT
            END
         REPEAT
         GOSUB CLEAN..UP
         GOSUB COUNT..RUNNING
         MSG = '.... End-of-List .... '
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
         STP.FL = 1
      REPEAT
END..PROG:
      MSG = '--------------------------[ END ] ---------------------------'
      PRINT
      PRINT MSG
      LOOP
         READNEXT ID ELSE EXIT
      REPEAT
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      STOP
* -------------------------------------------------------
CHECK..CONTROL:
      READ CONTROL FROM MASTER.DICT, "CONTROL" ELSE CONTROL = 0
      MAX.PH = CONTROL<1>+0
      UVACCT = CONTROL<2>
      NOWAIT = CONTROL<3>
      STP.FL = CONTROL<5>
      RETURN
* -------------------------------------------------------
GET..FILES:
      PLIST = ""
      EXE = "SSELECT uMASTER BY F3"
      EXECUTE EXE CAPTURING OUT
      LOOP
         READNEXT ID ELSE EXIT
         PLIST<-1> = ID
      REPEAT
      WRITE PLIST ON MASTER.DICT, "PLIST"
      ID = ""
      RETURN
* -------------------------------------------------------
COUNT..RUNNING:
      CMD = 'SH -c"ps -ef | grep uGET | grep -v grep"'      ;* UniVerse
*     CMD = '!ps -ef | grep uGET | grep -v grep'            ;* UniData
      EXECUTE CMD CAPTURING OUT
      NBR.PH = COUNT(OUT, @FM)
      RETURN
* -------------------------------------------------------
CLEAN..UP:
      SELECT PH
      LOOP
         READNEXT pID ELSE EXIT
         READ PREC FROM PH, pID ELSE CONTINUE
         DEL.FL=0
         IF INDEX(PREC, '[ END ]', 1) THEN 
            DEL.FL=1
         END ELSE
            IF INDEX(PREC, '[ABORT]', 1) THEN 
               DEL.FL=1
            END ELSE
               IF INDEX(PREC, '[ERROR]', 1) THEN DEL.FL=1
            END
         END
         IF DEL.FL THEN DELETE PH, pID
      REPEAT
      RETURN
* -------------------------------------------------------
CHECK..TIME:
      RIGHTNOW = ICONV(OCONV(TIME(), 'MTS'), 'MTS')
      TIMEDIFF = MIDNIGHT - RIGHTNOW
      IF TIMEDIFF < 1200 THEN
         EXECUTE "STOP.ALL"
      END
      RETURN
   END
