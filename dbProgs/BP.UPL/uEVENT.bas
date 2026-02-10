$INCLUDE I_Prologue
      * --------------------------------------------------------------- *
      * Use this program to register USER trigger events for a file     *
      *  NB: You absolutly MUST be in the account that defines the file *
      *                                                                 *
      * Syntax is:                                                      *
      *  uEVENT ON [file] [action] [time] [event] [tName] [srtn]        *
      *  <3>   [file]   the name of the file as defined in the VOC      *
      *  <4>   [action] is LOAD, CREATE, REMOVE or LIST                 *
      *  <5>   [time]   is BEFORE or AFTER                              *
      *  <6>   [event]  is INSERT, UPDATE or DELETE                     *
      *  <7>   [tName]  is the unique name of the trigger (e.g. CHK01)  *
      *  <8>   [srtn]   is the cataloged subroutine name to invoke      *
      * --------------------------------------------------------------- *
$IFDEF isRT
      VFILE = "MD"
      CMD = SENTENCE()
$ELSE
      VFILE = "VOC"
      CMD = @SENTENCE
$ENDIF
      MSG = ""
      *
      CALL SR.FILE.OPEN(ERR, VFILE, VOC)
      IF ERR #"" THEN MSG = "Cannot open ":VFILE ; GO END..PROG
      CALL SR.FILE.OPEN(ERR, "TRIGGERS", TRIGGERS)
      IF ERR #"" THEN MSG = "Cannot open TRIGGERS" ; GO END..PROG
      *
      VALID.ACTNS = "LOAD":@FM:"CREATE":@FM:"REMOVE":@FM:"LIST"
      VALID.WHENS = "BEFORE":@FM:"AFTER"
      VALID.WHATS = "INSERT":@FM:"UPDATE":@FM:"DELETE"
      *
      CMD   = EREPLACE(CMD, " ", @FM)
      FILE  = CMD<3>
      ACTN  = CMD<4>
      WHEN  = CMD<5>
      WHAT  = CMD<6>
      NAME  = CMD<7>
      SRTN  = CMD<8>
      *
      CALL SR.FILE.OPEN(ERR, FILE, CHECKIO)
      IF ERR #"" THEN MSG = "Cannot find [":FILE:"]" ; GO END..PROG
      *
      LOCATE(ACTN, VALID.ACTNS; POS) ELSE
         MSG = "Invalid action [":ACTN:"]"
         GO END..PROG
      END
      LOCATE(WHEN, VALID.WHENS; POS) ELSE
         MSG = "Invalid event timing [":WHEN:"]"
         GO END..PROG
      END
      LOCATE(WHAT, VALID.WHATS; POS) ELSE
         MSG = "Invalid event type [":WHAT:"]"
         GO END..PROG
      END
      *
      * Does the event-name exist ?
      *
      READ T.INDX FROM TRIGGERS, NAME ELSE T.INDX=""
      IF T.INDX#"" AND ACTN#"REMOVE" THEN
         MSG = "Invalid: event [":NAME:"] already exists."
         GO END..PROG
      END
      *
      * Does the SRTN exist and is it a cataloged subr ?
      *
      READ CHK FROM VOC, SRTN ELSE CHK = ""
      IF CHK<1>#"V" THEN
         CALL SR.FILE.OPEN(ERR, "GLOBAL.CATDIR", GCAT)
         IF ERR #"" THEN MSG = "Cannot find [GLOBAL.CATDIR]" ; GO END..PROG
         READ CHK FROM GCAT, NAME THEN OKAY=1 ELSE OKAY=0
         IF NOT(OKAY) THEN
            MSG = "Invalid: the subroutine [":SRTN:"] is not cataloged."
            GO END..PROG
         END
      END
      *
      EVTKEY = FILE:"*":WHEN:"*":WHAT
      READ EVTLIST FROM TRIGGERS, EVTKEY ELSE EVTLIST=""
      *
      EOI = DCOUNT(EVTLIST, @FM)
      FOR A = 1 TO EOI
         IF EVTLIST<A, 1> = NAME THEN
            MSG = "Invalid: event [":NAME:"] already exists."
            GO END..PROG
         END
         IF EVTLIST<A, 2> = SRTN THEN
            CRT "WARNING: ":SRTN:" is also used by ":EVTLIST<A, 1>
         END
      NEXT A
      EVTLIST<-1> = NAME:@VM:SRTN
      WRITE EVTLIST ON TRIGGERS, EVTKEY
      *
      EXE = ""
      IF ACTN="LOAD" THEN
         GOSUB LOAD..EXISTING..TRIGGERS
      END
      IF ACTN="CREATE" THEN
         EXE = "CREATE TRIGGER ":NAME:" ":WHEN:" ":WHAT:" ON ":FILE:" FOR EACH ROW CALLING ":SRTN:";"
      END
      IF ACTN="REMOVE" THEN
         EXE = "DROP TRIGGER ":FILE:" ":NAME
      END
      IF ACTN="LIST" THEN
         EXE = "LIST.SICA ":FILE
      END
      IF EXE#"" THEN EXECUTE EXE
END..PROG:
      IF MSG#"" THEN CRT ; CRT MSG
      CRT
      STOP
      * ----------------------------------------------------------------
LOAD..EXISTING..TRIGGERS:
      CRT "Clear the TRIGGERS file."
      EXECUTE "CLEAR-FILE TRIGGERS"
      CRT "Select ALL local files and load existing triggers."
      EXEC = "SELECT ":VFILE:' WITH F1 = "F"' 
      CRT "  ":EXEC
      EXECUTE EXEC
      LOOP
         READNEXT FILEID ELSE EXIT
         EXEC = "LIST.SICA ":FILEID
         EXECUTE EXEC CAPTURING JUNK
         JUNK = JUNK[INDEX(JUNK, "LIST.SICA", 1), LEN(JUNK)]
         *
         TNAME = ""
         SNAME = ""
         EVENT = ""
         ETIME = ""
         EOA = DCOUNT(JUNK, @FM)
         FOR A = 1 TO EOA
            LINE = JUNK<A>
            EOM = DCOUNT(LINE, @VM)
            FOR M = 1 TO EOM
               LINE = JUNK<A,M>
               EOS = DCOUNT(LINE, @SM)
               FOR S = 1 TO EOS
                  LINE = TRIM(TRIMF(JUNK<A,M,S>))
                  LPARTS = EREPLACE(LINE, " ", @FM)
                  WORD = UPCASE(LPARTS<1>)
                  IF WORD = "TRIGGER" THEN TNAME = LPARTS<2>
                  IF WORD = "CALLS" THEN 
                     SNAME = LPARTS<2>
                     ETIME = UPCASE(LPARTS<5>)
                     EVENT = UPCASE(LPARTS<6>)
                  END
                  IF TNAME#"" AND SNAME#"" AND ETIME#"" AND EVENT#"" THEN
                     GOSUB DO..TRIGGER
                  END
               NEXT S
            NEXT M
         NEXT A
      REPEAT
      RETURN
   * -------------------------------
DO..TRIGGER:
      TNAME = EREPLACE(TNAME, '"', "")
      TNAME = EREPLACE(TNAME, "'", "")
      SNAME = EREPLACE(SNAME, '"', "")
      SNAME = EREPLACE(SNAME, "'", "")
      CHR = SNAME[1,1]
      IF CHR = "*" OR CHR = "!" THEN SNAME = SNAME[2, LEN(SNAME)]
      INSLINE = TNAME:@VM:SNAME
      EVTKEY = FILEID:"*":ETIME:"*":EVENT
      READ EVTLIST FROM TRIGGERS, EVTKEY THEN
         WRITE INSLINE ON TRIGGERS, EVTKEY
         CRT "   ":FILEID:"   ":EVTKEY
         WRITE EVTKEY ON TRIGGERS, TNAME
      END ELSE
         CRT "Logic Error: ":EVTKEY:" already exists!"
         STOP
      END
      TNAME="" ; SNAME="" ; ETIME="" ; EVENT=""
      RETURN
   END
