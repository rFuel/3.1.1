      PROMPT ""
      OPEN "CSV" TO CSV.IO ELSE STOP "CSV missing"
      OPEN "UPL.CONTROL" TO CONTROL.IO ELSE STOP "UPL.CONTROL missing"
      READ CONTROL FROM CONTROL.IO, "SCRUB.FILES" ELSE STOP "SCRUB.FILES missing"
      REMOTE = "RFUELPOC2"
      GET.PHS= 'SH -c"ps -ef | grep phantom | grep -v grep"'
      MAX.PHS= 7
      PIF = "S4"                         ; * Product In Focus
      *
      SCRUB  = "PHANTOM SANITISE-V2 FROM ":REMOTE:" FILE $ LIST & CSV TRAN.ARC.csv JOB #"
      PIFSEL = "SELECT REAL.$ LIKE ...":PIF:" OR LIKE ...":PIF:"/... OR LIKE ...":PIF:"...."
      *
      NBR.LINES = DCOUNT(CONTROL, @FM)
      CRT "processing ":NBR.LINES:" files from SCRUB.FILES in UPL.CONTROL"
      *
      PCT = 0
      JNO = 0
      LOOP
         PCT += 1
         IF PCT > NBR.LINES THEN EXIT
         LINE = CONTROL<PCT>
         IF LINE[1,1] = "#" THEN CONTINUE
         IF LINE[1,1] = "*" THEN CONTINUE
         FILE = LINE<1,1>
         LIST = LINE<1,2>
         CSV  = LINE<1,3>
         CRT                     ;* space things out a little
         CRT "preparing ":PCT "R#5":" of ":NBR.LINES "L#5":"  : ":FILE
         *
         READ CHK FROM CSV.IO, CSV ELSE CRT SPACE(26):": ":CSV:" is not in CSV - cannot process!"; CONTINUE
         *
         CRT SPACE(26):": SET-FILE ":REMOTE:" ":FILE:" REAL.":FILE
         EXECUTE "SET-FILE ":REMOTE:" ":FILE:" REAL.":FILE CAPTURING JUNK
         *
         * Scrub and take ONLY the product-in-focus records.
         *
         SEL = EREPLACE(PIFSEL, "$", FILE)
         CRT SPACE(26):": ":SEL
         EXECUTE SEL CAPTURING JUNK RTNLIST SLIST
         USE.LIST = LIST
         CRT SPACE(26):": SAVE.LIST ":USE.LIST
         EXECUTE "SAVE.LIST ":USE.LIST CAPTURING JUNK PASSLIST SLIST
         SLOT = MAX.PHS
         SKIP = 0
         CRT SPACE(26):": ... waiting on a phantom slot"
         LOOP WHILE SLOT => MAX.PHS
            GOSUB GET..SLOT
            IF SKIP THEN EXIT
            IF SLOT < MAX.PHS THEN EXIT
         REPEAT
         IF NOT(SKIP) THEN
            JNO += 1
            CMD = EREPLACE(SCRUB, "$", FILE)
            CMD = EREPLACE(CMD ,  "#", JNO)
            CMD = EREPLACE(CMD ,  "&", USE.LIST)
            CRT SPACE(26):": processing ":CMD
            EXECUTE CMD CAPTURING JUNK
         END
      REPEAT
      CRT "Scrubbing has finished"
      STOP
      *
      * -----------------------------------------------------------------------
      *
GET..SLOT:
      EXECUTE GET.PHS CAPTURING RESULT
      IF INDEX(RESULT ,FILE:" ", 1) THEN SKIP = 1 ; SLOT = MAX.PHS ; RETURN
      SLOT = DCOUNT(RESULT, @FM)
      RETURN
      *
   END