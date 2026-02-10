      *  
      *  Synopsis:
      *     usage: a SANITISE FROM {account} [NEW-IDS] [NO-CLEAR]
      *          : b SANITISE FROM {account} FILE {file} LIST {save-list} CSV {csv-name} [NEW-IDS] [NO-CLEAR] [DEBUG]
      *  
      *     setup:   set-file for BP.UPL OBJ.UPL and CSV
      *  
      * ----------------------------------------------------
      EXECUTE "CLEARCOMMON" CAPTURING OUTPUT
      EXECUTE "CLEARPROMPTS" CAPTURING OUTPUT
      EXECUTE "WHO" CAPTURING OUTPUT
      THIS.ACCT = FIELD(OUTPUT, " ", 2)
      *
      CMD = EREPLACE(@SENTENCE, " ", @FM)          ; * this means no spaces in input values !!
      LOCATE("SANITISE-V2", CMD; FND) ELSE STOP "Error! Wrong command structure"
      FOR F = 1 TO FND
         CMD = DELETE(CMD, 1, 0, 0)
      NEXT F
      *
      * ----------------------------------------------------
      *
      LOCATE("FROM", CMD; FND) ELSE STOP "Error! Cannot find FROM {account}"
      REAL.ACCT = CMD<FND+1>
      IF REAL.ACCT = "" THEN STOP "Error! Cannot find FROM {account}"
      *
      LOCATE("FILE", CMD; FND)      THEN s.FILE = CMD<FND+1> ELSE s.FILE = ""
      LOCATE("LIST", CMD; FND)      THEN s.LIST = CMD<FND+1> ELSE s.LIST = ""
      LOCATE("CSV",  CMD; FND)      THEN s.CSV  = CMD<FND+1> ELSE s.CSV  = ""
      LOCATE("NEW-IDS",  CMD; FND)  THEN NEW.IDS = 1 ELSE NEW.IDS = 0
      LOCATE("NO-CLEAR", CMD; FND)  THEN NOCLEAR = 1 ELSE NOCLEAR = 0
      LOCATE("DEBUG", CMD; FND)     THEN DBG = 1 ELSE DBG = 0
      *
      *  NEW-IDS needs a lot of testing before release
      *     a) how to carry new id into other files
      *     b) will it actually help PII ???
      *
      MAX = 999999999
      ERR = ""
      NEW.LIST = ""                      ; *  <1> = old ids  and <2> = new ids
      CHECK.SCRUBBED = 0
      *
      CRT "Preparing Test Data in [":THIS.ACCT:"] ----------------------------"
      OPEN "VOC" TO VOC ELSE STOP "VOC"
      OPEN "&SAVEDLISTS&" TO LIST.CONTROL ELSE STOP "&SAVEDLISTS&"
      OPEN "BP.UPL" TO BP.UPL ELSE STOP "BP.UPL"
      OPEN "CSV" TO CSV ELSE STOP "CSV"
      *
      DATE.ADD = RND(149)
      IF RND(9) < 5 THEN DATE.ADD = 0 - DATE.ADD
      GOSUB CHECK..FILES
      *
      F = 0
      LOOP
         F += 1
         FILE = FLIST<F>
         SEL = SLIST<F>
         MAP = CLIST<F>
      UNTIL FILE = "" DO
         IF MAP = "" THEN MAP = FILE:".csv"
         READ INSTR FROM CSV, MAP ELSE
            CRT "Skipping - cannot find ":MAP
            CONTINUE
         END
         CRT "Sanitising ":FILE
         OPEN "REAL.":FILE TO REALF ELSE STOP "Cannot open REAL.":FILE
         OPEN FILE TO LOCALF ELSE STOP "Cannot open ":FILE
         IF SEL # "" THEN
            EXECUTE SEL CAPTURING JUNK
         END ELSE
            SELECT REALF
         END
         CNT = 0
         LOOP
            READNEXT ITEM.ID ELSE EXIT
            IF NEW.IDS THEN
               NEW.ITEM = ""
               LOCATE(ITEM.ID, NEW.LIST, 1; NID.POS) THEN
                  NEW.ITEM = NEW.LIST<2, NID.POS>
               END ELSE
                  NEW.LIST<1,-1> = ITEM.ID
                  LOCATE(ITEM.ID, NEW.LIST, 1; NID.POS) ELSE STOP "NEW-ID logic failure"
               END
               IF NEW.ITEM = "" THEN
                  EOX = LEN(ITEM.ID)
                  FOR X = 1 TO EOX
                     NEW.ITEM := RND(9)  ; * random nbumber between 0 and 9
                  NEXT X
                  NEW.LIST<2,NID.POS> = NEW.ITEM
               END
            END ELSE
               NEW.ITEM = ITEM.ID
            END
            *
            READ RECORD FROM REALF, ITEM.ID ELSE CONTINUE
            *
            IF NOCLEAR THEN
               READ OLDREC FROM LOCALF, NEW.ITEM THEN
                  HAS.CHANGED=0
                  GOSUB CHECK..FOR..CHANGES
                  IF HAS.CHANGED THEN
                     WRITE RECORD ON LOCALF, NEW.ITEM
                  END
                  CNT += 1 ; CONTINUE
               END
            END
            *
            GOSUB SCRUB..RECORD
            WRITE RECORD ON LOCALF, NEW.ITEM
            CNT += 1
            C1000 = CNT / 100000
            INT.CNT = INT(C1000)
            IF INT.CNT = C1000 THEN CRT CNT:" -"
         REPEAT
         CRT CNT:" ":FILE:" records sanitised."
         CLOSE REALF
         CLOSE LOCALF
      REPEAT
END..PROG:
      CRT " "
      CRT "-------------- Done ---------------"
      STOP
      * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - *
CHECK..FOR..CHANGES:
      * OLDREC: the scrubbed record
      * RECORD: the live record
      * INSTR : the scrub instructions
      * -----------------------------------------------------------
      EOI = DCOUNT(INSTR, @FM)
      FOR I = 1 TO EOI
         LINE = INSTR<I>
         IF LINE[1,1] = "#" THEN CONTINUE
         LINE = EREPLACE(LINE, ",", @VM)
         AA = LINE<1,2>                   ; * handle "n"
         MM = LINE<1,3>                   ; * handle "n"
         SS = LINE<1,4>                   ; * handle "n"
         ACTION = UPCASE(LINE<1,5>)
      NEXT I
      RETURN
      * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - *
SCRUB..RECORD:
      IF NEW.IDS THEN
         RECORD = EREPLACE(RECORD, ITEM.ID, NEW.ITEM)
      END
      EOI = DCOUNT(INSTR, @FM)
      FOR I = 1 TO EOI
         LINE = INSTR<I>
         IF LINE[1,1] = "#" THEN CONTINUE
         LINE = EREPLACE(LINE, ",", @VM)
         AA = LINE<1,2>                   ; * handle "n"
         MM = LINE<1,3>                   ; * handle "n"
         SS = LINE<1,4>                   ; * handle "n"
         ACTION = UPCASE(LINE<1,5>)
         *
         GOSUB SCRUB..LINE
         *
      NEXT I
      RETURN
      * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - *
SCRUB..LINE:
      BEGIN CASE
         CASE ACTION = "TEXT"
            CHK = AA
            GOSUB HANDLE..N
            IF NOT(NUM(FF)) THEN RETURN
            IF TT = MAX THEN TT = DCOUNT(RECORD, @FM)
            IF FF + 0 = 0 THEN FF = 1
            IF TT < FF THEN TT = FF
            AFF = FF ; ATT = TT
            FOR A = AFF TO ATT
               CHK = MM
               GOSUB HANDLE..N
               IF NOT(NUM(FF)) THEN RETURN
               IF TT = MAX THEN TT = DCOUNT(RECORD<A>, @VM)
               IF FF + 0 = 0 THEN FF = 1
               IF TT < FF THEN TT = FF
               MFF = FF ; MTT = TT
               FOR M = MFF TO MTT
                  CHK = SS
                  GOSUB HANDLE..N
                  IF NOT(NUM(FF)) THEN CONTINUE
                  IF TT = MAX THEN TT = DCOUNT(RECORD<A,M>, @SM)
                  IF FF + 0 = 0 THEN FF = 1
                  IF TT < FF THEN TT = FF
                  SFF = FF ; STT = TT
                  FOR S = SFF TO STT
                     newV = ""
                     oldV = RECORD<A,M,S>
                     IF oldV = "" THEN CONTINUE
                     CALL SR.SCRAMBLESTRING(oldV, newV )
                     RECORD<A,M,S> = newV
                     IF DBG THEN CRT ITEM.ID:" <":A:",":M:",":S:">  action [":ACTION:"]  old [":oldV:"]   new [":newV:"]"
                  NEXT S
               NEXT M
            NEXT A
         CASE ACTION = "DATE"
            CHK = AA
            GOSUB HANDLE..N
            IF NOT(NUM(FF)) THEN RETURN
            IF TT = MAX THEN TT = DCOUNT(RECORD, @FM)
            IF FF + 0 = 0 THEN FF = 1
            IF TT < FF THEN TT = FF
            AFF = FF ; ATT = TT
            FOR A = AFF TO ATT
               CHK = MM
               GOSUB HANDLE..N
               IF NOT(NUM(FF)) THEN CONTINUE
               IF TT = MAX THEN TT = DCOUNT(RECORD<A>, @VM)
               IF FF + 0 = 0 THEN FF = 1
               IF TT < FF THEN TT = FF
               MFF = FF ; MTT = TT
               FOR M = MFF TO MTT
                  CHK = SS
                  GOSUB HANDLE..N
                  IF NOT(NUM(FF)) THEN RETURN
                  IF TT = MAX THEN TT = DCOUNT(RECORD<A>, @SM)
                  IF FF + 0 = 0 THEN FF = 1
                  IF TT < FF THEN TT = FF
                  SFF = FF ; STT = TT
                  FOR S = SFF TO STT
                     IF NUM(RECORD<A,M,S>) THEN
                        newV = ""
                        oldV = RECORD<A,M,S>
                        IF oldV = "" THEN CONTINUE
                        IF NOCLEAR THEN
                            newV = OLDREC<A,M,S>
                        END ELSE
                            newV = oldV + DATE.ADD
                        END
                        RECORD<A,M,S> = newV
                        IF DBG THEN CRT ITEM.ID:" <":A:",":M:",":S:">  action [":ACTION:"]  old [":oldV:"]   new [":newV:"]"
                     END ELSE
                        CRT FILE:" ":ITEM.ID:" <":A:",":M:",":S:"> is not a DATE !!"
                     END
                  NEXT S
               NEXT M
            NEXT A
         CASE ACTION = "NUMBER"
            CHK = AA
            GOSUB HANDLE..N
            IF NOT(NUM(FF)) THEN RETURN
            IF TT = MAX THEN TT = DCOUNT(RECORD, @FM)
            IF FF + 0 = 0 THEN FF = 1
            IF TT < FF THEN TT = FF
            AFF = FF ; ATT = TT
            FOR A = AFF TO ATT
               CHK = MM
               GOSUB HANDLE..N
               IF NOT(NUM(FF)) THEN CONTINUE
               IF TT = MAX THEN TT = DCOUNT(RECORD<A>, @VM)
               IF FF + 0 = 0 THEN FF = 1
               IF TT < FF THEN TT = FF
               MFF = FF ; MTT = TT
               FOR M = MFF TO MTT
                  CHK = SS
                  GOSUB HANDLE..N
                  IF NOT(NUM(FF)) THEN RETURN
                  IF TT = MAX THEN TT = DCOUNT(RECORD<A>, @SM)
                  IF FF + 0 = 0 THEN FF = 1
                  IF TT < FF THEN TT = FF
                  SFF = FF ; STT = TT
                  FOR S = SFF TO STT
                     IF NUM(RECORD<A,M,S>) THEN
                        newV = ""
                        oldV = RECORD<A,M,S>
                        IF oldV = "" THEN CONTINUE
                        CALL SR.SCRUBNUMBER(oldV, newV)
                        RECORD<A,M,S> = newV
                        IF DBG THEN CRT ITEM.ID:" <":A:",":M:",":S:">  action [":ACTION:"]  old [":oldV:"]   new [":newV:"]"
                     END ELSE
                        CRT FILE:" ":ITEM.ID:" <":A:",":M:",":S:"> is not NUMBER !!"
                     END
                  NEXT S
               NEXT M
            NEXT A
         CASE ACTION = "NARR"
            CHK = AA
            GOSUB HANDLE..N
            IF NOT(NUM(CHK)) THEN RETURN
            IF TT = MAX THEN TT = DCOUNT(RECORD, @FM)
            IF FF + 0 = 0 THEN FF = 1
            IF TT < FF THEN TT = FF
            AFF = FF + 0 ; ATT = TT + 0
            FOR A = AFF TO ATT
               CHK = MM
               GOSUB HANDLE..N
               IF NOT(NUM(FF)) THEN CONTINUE
               IF FF + 0 = 0 THEN FF = 1
               IF TT < FF THEN TT = FF
               IF TT = MAX THEN TT = DCOUNT(RECORD<A>, @VM)
               MFF = FF ; MTT = TT
               FOR M = MFF TO MTT
                  CHK = SS
                  GOSUB HANDLE..N
                  IF NOT(NUM(FF)) THEN CONTINUE
                  IF FF + 0 = 0 THEN FF = 1
                  IF TT < FF THEN TT = FF
                  IF TT = MAX THEN TT = DCOUNT(RECORD<A>, @VM)
                  SFF = FF ; STT = TT
                  FOR S = SFF TO STT
                     newV = ""
                     oldV = RECORD<A,M,S>
                     IF oldV = "" THEN CONTINUE
                     CALL SR.SCRUBNARRATIVE(oldV, newV)
                     RECORD<A,M,S> = newV
                     IF DBG THEN CRT ITEM.ID:" <":A:",":M:",":S:">  action [":ACTION:"]  old [":oldV:"]   new [":newV:"]"
                  NEXT S
               NEXT M
            NEXT A
      END CASE
      RETURN
      * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - *
HANDLE..N:
      * CHK is set prior to calling this subroutine
      * return FF and TT as from and to range
      * if CHK is not numeric, return it and handle error.
      IF NUM(CHK) THEN
         FF = CHK + 0
         TT = FF
      END ELSE
         IF INDEX(UPCASE(CHK), "N", 1) THEN
            CHK = EREPLACE(UPCASE(CHK), "N", "")
            CHK = EREPLACE(UPCASE(CHK), "-", "")
            FF = CHK + 0
            TT = MAX
         END ELSE
            FF = "ERROR"
            TT = "ERROR"
            RETURN
         END
      END
      RETURN
      * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - *
CHECK..FILES:
      FLIST = ""
      SLIST = ""
      CLIST = ""
      CRT "Checking database file:-"
      * CONTROL is a list of files, one file per attribute.      *
      READ CONTROL FROM BP.UPL, "SANITISE-FILES" ELSE CONTROL = ""
      IF (s.FILE # "") THEN
         CONTROL = ""
         CONTROL<1,1> = s.FILE
         IF (s.LIST # "") THEN
            CONTROL<1,2> = "GET-LIST ":s.LIST
         END
         IF s.CSV # "" THEN
            CONTROL<1,3> = s.CSV
         END
      END
      F = 0
      LOOP
         F += 1
         LINE = TRIM(CONTROL<F>)
      UNTIL LINE = "" DO
         IF LINE[1,1] = "#" THEN CONTINUE
         ID = LINE<1,1>                  ; * File name
         SEL= LINE<1,2>                  ; * special select cmd for file
         MAP= LINE<1,3>                  ; * special csv name from file
         CRT "  >   ":ID
         DNAME = ID
         PNAME = "REAL.":ID
         OPEN ID TO JUNKIO THEN
            IF NOCLEAR THEN
               * do no clearing
               NULL
            END ELSE
               STATUS JSTAT FROM JUNKIO THEN
                  IF JSTAT<26> = "" THEN
                     CRT "   Clearing ":ID
                     CLEARFILE JUNKIO       ; * clear the recipient file
                     CLOSE JUNKIO
                  END ELSE
                     * part files
                     CLOSE JUNKIO
                     EOJ = DCOUNT(JSTAT<26>, @VM)
                     FOR J = 1 TO EOJ
                        PPNAME = JSTAT<26,J>
                        CRT "   Clearing ":PPNAME
                        OPEN PPNAME TO JUNKIO ELSE
                           CRT "      failed"
                           CONTINUE
                        END
                        CLEARFILE JUNKIO
                        CLOSE JUNKIO
                     NEXT J
                  END
               END ELSE
                  CRT "Cannot STAT ":DNAME
                  STOP
               END
            END
         END ELSE
            CRT "   Creating ":ID
            EXE = "CREATE.FILE ":ID:" 30 64BIT"
            EXECUTE EXE CAPTURING OUT
            F -= 1
            CONTINUE
         END
         IF NOCLEAR THEN
            CF.TRY=0
            LOOP
               OPEN ID:".SCRUB" TO SCRUBIO THEN
                  CHECK.SCRUBBED = 1
                  EXIT
               END ELSE
                  IF CF.TRY => 2 THEN CHECK.SCRUBBED = 0; EXIT
                  EXECUTE "CREATE.FILE ":ID:".SCRUB 30 64BIT" CAPTURING JUNK
                  CF.TRY += 1
               END
            REPEAT
         END
         Q.PTR = "Q"
         Q.PTR<2> = REAL.ACCT
         Q.PTR<3> = ID
         WRITE Q.PTR ON VOC, PNAME
         OPEN PNAME TO CHKIO ELSE STOP ID:" does not exist in ":REAL.ACCT
         CLOSE CHKIO
         FLIST<-1> = ID                   ; * File name
         SLIST<-1> = SEL                  ; * Select - default is empty
         CLIST<-1> = MAP                  ; * csv name - default is empty
      REPEAT
      CRT "Done checking files"
      RETURN
      * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - *
   END
