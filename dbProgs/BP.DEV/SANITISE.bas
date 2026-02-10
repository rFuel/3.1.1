$INCLUDE I_Prologue
      *                                                     
      *  Synopsis:                                          
      *     usage:   SANITISE FROM {account} [NEW-IDS]      
      *     setup:   set-file for BP.UPL OBJ.UPL and CSV    
      *                                                     
      * ----------------------------------------------------
      EXECUTE "CLEARCOMMON" CAPTURING OUTPUT
      EXECUTE "WHO" CAPTURING OUTPUT
      THIS.ACCT = FIELD(OUTPUT, " ", 2)
      *
      CMD = EREPLACE(@SENTENCE, " ", @FM)    ;* this means no spaces in account names !!
      LOCATE("FROM", CMD; FND) ELSE STOP "Error! usage is SANITISE FROM {account}"
      REAL.ACCT = CMD<FND+1>
      IF REAL.ACCT = "" THEN STOP "Error! usage is SANITISE FROM {account}"
      *
      *  NEW-IDS needs a lot of testing before release      
      *     a) how to carry new id into other files         
      *     b) will it actually help PII ???                
      *
      LOCATE("NEW-IDS", CMD; FND) THEN NEW.IDS = 1 ELSE NEW.IDS = 0
      *
      ERR         = ""
      NEW.LIST    = ""     ;*  <1> = old ids  and <2> = new ids
      *
      CRT "Preparing Test Data in [":THIS.ACCT:"] ----------------------------"
      OPEN "VOC"           TO VOC          ELSE STOP "VOC   open error"
      OPEN "&SAVEDLISTS&"  TO LIST.CONTROL ELSE STOP "&SAVEDLISTS&   open error"
      OPEN "BP.UPL"        TO BP.UPL       ELSE STOP "BP.UPL   open error"
      OPEN "CSV"           TO CSV          ELSE STOP "CSV   open error"
      OPEN "SCRUB.CARDS"   TO SCRUB.CARDS ELSE STOP "SCRUB.CARDS   open error"
      *
      DATE.ADD = RND(149)
      IF RND(9) < 5 THEN DATE.ADD = 0 - DATE.ADD
      GOSUB INITIALISE
      *
      F = 0
      LOOP
         F += 1
         FILE = FLIST<F>
         SEL  = SLIST<F>
         MAP  = CLIST<F>
      UNTIL FILE = "" DO
         IF MAP = "" THEN MAP = FILE:".csv"
         READ INSTR FROM CSV, MAP ELSE
            CRT "Skipping - cannot find ":FILE:".csv"
            CONTINUE
         END
         CRT "Sanitising ":FILE
         OPEN "REAL.":FILE TO REALF  ELSE STOP "Cannot open REAL.":FILE
         OPEN FILE         TO LOCALF ELSE STOP "Cannot open ":FILE
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
                     NEW.ITEM := RND(9)   ;* random nbumber between 0 and 9
                  NEXT X
                  NEW.LIST<2,NID.POS> = NEW.ITEM
               END
            END ELSE
               NEW.ITEM = ITEM.ID
            END
            READ RECORD FROM REALF, ITEM.ID ELSE CONTINUE
            GOSUB SCRUB..RECORD
            WRITE RECORD ON LOCALF, NEW.ITEM
            CNT += 1
            C1000 = CNT / 1000
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
SCRUB..RECORD:
      IF NEW.IDS THEN
         RECORD = EREPLACE(RECORD, ITEM.ID, NEW.ITEM)
      END
      MAX = 999999999
      EOI = DCOUNT(INSTR, @FM)
      FOR I = 1 TO EOI
          LINE = INSTR<I>
          IF LINE[1,1] = "#" THEN CONTINUE
          LINE = EREPLACE(LINE, ",", @VM)
          A = LINE<1,2>    ;* handle "n"
          M = LINE<1,3>    ;* handle "n"
          S = LINE<1,4>    ;* handle "n"
          ACTION = UPCASE(LINE<1,5>)
          *
          BEGIN CASE
            CASE ACTION = "TEXT"
               CHK = A
               GOSUB HANDLE..N
               IF NOT(NUM(FF)) THEN
                  CRT FILE:" ":ITEM.ID:" <":A:",":M:",":S:"> attribute is invalid !!"
                  CONTINUE
               END
               IF TT = MAX THEN TT = DCOUNT(RECORD, @FM)
               IF FF + 0 = 0 THEN FF = 1
               IF TT < FF THEN TT = FF
               AFF = FF ; ATT = TT
               FOR A = AFF TO ATT
                  CHK = M
                  GOSUB HANDLE..N
                  IF NOT(NUM(FF)) THEN
                     CRT FILE:" ":ITEM.ID:" <":A:",":M:",":S:"> mv is invalid !!"
                     CONTINUE
                  END
                  IF TT = MAX THEN TT = DCOUNT(RECORD<A>, @VM)
                  IF FF + 0 = 0 THEN FF = 1
                  IF TT < FF THEN TT = FF
                  MFF = FF ; MTT = TT
                  FOR M = MFF TO MTT
                     CHK = S
                     GOSUB HANDLE..N
                     IF NOT(NUM(FF)) THEN
                        CRT FILE:" ":ITEM.ID:" <":A:",":M:",":S:"> sv is invalid !!"
                        CONTINUE
                     END
                     IF TT = MAX THEN TT = DCOUNT(RECORD<A>, @SM)
                     IF FF + 0 = 0 THEN FF = 1
                     IF TT < FF THEN TT = FF
                     SFF = FF ; STT = TT
                     FOR S = SFF TO STT
                        newV = ""
                        oldV = RECORD<A,M,S>
*?*                     CALL SR.SCRUBSTRING(RECORD<A,M,S>, newV)
                        CALL SR.SCRAMBLESTRING(RECORD<A,M,S>, newV )
                        oldV = RECORD<A,M,S>
                        RECORD<A,M,S> = newV
***                     CRT ITEM.ID:" <":A:",":M:",":S:">  action [":ACTION:"]  old [":oldV:"]   new [":newV:"]"
                     NEXT S
                  NEXT M
               NEXT A
            CASE ACTION = "DATE"
               CHK = A
               GOSUB HANDLE..N
               IF NOT(NUM(FF)) THEN
                  CRT FILE:" ":ITEM.ID:" <":A:",":M:",":S:"> attribute is invalid !!"
                  CONTINUE
               END
               IF TT = MAX THEN TT = DCOUNT(RECORD, @FM)
               IF FF + 0 = 0 THEN FF = 1
               IF TT < FF THEN TT = FF
               AFF = FF ; ATT = TT
               FOR A = AFF TO ATT
                  CHK = M
                  GOSUB HANDLE..N
                  IF NOT(NUM(FF)) THEN
                     CRT FILE:" ":ITEM.ID:" <":A:",":M:",":S:"> mv is invalid !!"
                     CONTINUE
                  END
                  IF TT = MAX THEN TT = DCOUNT(RECORD<A>, @VM)
                  IF FF + 0 = 0 THEN FF = 1
                  IF TT < FF THEN TT = FF
                  MFF = FF ; MTT = TT
                  FOR M = MFF TO MTT
                     CHK = S
                     GOSUB HANDLE..N
                     IF NOT(NUM(FF)) THEN
                        CRT FILE:" ":ITEM.ID:" <":A:",":M:",":S:"> sv is invalid !!"
                        CONTINUE
                     END
                     IF TT = MAX THEN TT = DCOUNT(RECORD<A>, @SM)
                     IF FF + 0 = 0 THEN FF = 1
                     IF TT < FF THEN TT = FF
                     SFF = FF ; STT = TT
                     FOR S = SFF TO STT
                        IF NUM(RECORD<A,M,S>) THEN
                           oldV = RECORD<A,M,S>
                           RECORD<A,M,S> = newV
                           newV = OLDv + DATE.ADD
                           RECORD<A,M,S> = newV
***                        CRT ITEM.ID:" <":A:",":M:",":S:">  action [":ACTION:"]  old [":oldV:"]   new [":newV:"]"
                        END ELSE
                           CRT FILE:" ":ITEM.ID:" <":A:",":M:",":S:"> is not a DATE !!"
                        END
                     NEXT S
                  NEXT M
               NEXT A
            CASE ACTION = "NUMBER"
               CHK = A
               GOSUB HANDLE..N
               IF NOT(NUM(FF)) THEN
                  CRT FILE:" ":ITEM.ID:" <":A:",":M:",":S:"> attribute is invalid !!"
                  CONTINUE
               END
               IF TT = MAX THEN TT = DCOUNT(RECORD, @FM)
               IF FF + 0 = 0 THEN FF = 1
               IF TT < FF THEN TT = FF
               AFF = FF ; ATT = TT
               FOR A = AFF TO ATT
                  CHK = M
                  GOSUB HANDLE..N
                  IF NOT(NUM(FF)) THEN
                     CRT FILE:" ":ITEM.ID:" <":A:",":M:",":S:"> mv is invalid !!"
                     CONTINUE
                  END
                  IF TT = MAX THEN TT = DCOUNT(RECORD<A>, @VM)
                  IF FF + 0 = 0 THEN FF = 1
                  IF TT < FF THEN TT = FF
                  MFF = FF ; MTT = TT
                  FOR M = MFF TO MTT
                     CHK = S
                     GOSUB HANDLE..N
                     IF NOT(NUM(FF)) THEN
                        CRT FILE:" ":ITEM.ID:" <":A:",":M:",":S:"> sv is invalid !!"
                        CONTINUE
                     END
                     IF TT = MAX THEN TT = DCOUNT(RECORD<A>, @SM)
                     IF FF + 0 = 0 THEN FF = 1
                     IF TT < FF THEN TT = FF
                     SFF = FF ; STT = TT
                     FOR S = SFF TO STT
                        IF NUM(RECORD<A,M,S>) THEN
                           newV = ""
                           CALL SR.SCRUBNUMBER(RECORD<A,M,S>, newV)
                           oldV = RECORD<A,M,S>
                           RECORD<A,M,S> = newV
***                        CRT ITEM.ID:" <":A:",":M:",":S:">  action [":ACTION:"]  old [":oldV:"]   new [":newV:"]"
                        END ELSE
                           CRT FILE:" ":ITEM.ID:" <":A:",":M:",":S:"> is not NUMBER !!"
                        END
                     NEXT S
                  NEXT M
               NEXT A
            CASE ACTION = "NARR"
               CHK = A
               GOSUB HANDLE..N
               IF NOT(NUM(CHK)) THEN
                  CRT FILE:" ":ITEM.ID:" <":A:",":M:",":S:"> attribute is invalid !!"
                  CONTINUE
               END
               IF TT = MAX THEN TT = DCOUNT(RECORD, @FM)
               IF FF + 0 = 0 THEN FF = 1
               IF TT < FF THEN TT = FF
               AFF = FF + 0 ; ATT = TT + 0
               FOR A = AFF TO ATT
                  CHK = M + 0
                  GOSUB HANDLE..N
                  IF NOT(NUM(FF)) THEN
                     CRT FILE:" ":ITEM.ID:" <":A:",":M:",":S:"> mv is invalid !!"
                     CONTINUE
                  END
                  IF FF + 0 = 0 THEN FF = 1
                  IF TT < FF THEN TT = FF
                  IF TT = MAX THEN TT = DCOUNT(RECORD<A>, @VM)
                  MFF = FF ; MTT = TT
                  FOR M = MFF TO MTT
                     CHK = S
                     GOSUB HANDLE..N
                     IF NOT(NUM(FF)) THEN
                        CRT FILE:" ":ITEM.ID:" <":A:",":M:",":S:"> sv is invalid !!"
                        CONTINUE
                     END
                     IF FF + 0 = 0 THEN FF = 1
                     IF TT < FF THEN TT = FF
                     IF TT = MAX THEN TT = DCOUNT(RECORD<A>, @VM)
                     SFF = FF ; STT = TT
                     FOR S = SFF TO STT
                        newV = ""
                        CALL SR.SCRUBNARRATIVE(RECORD<A,M,S>, newV)
                        oldV = RECORD<A,M,S>
                        RECORD<A,M,S> = newV
***                     CRT ITEM.ID:" <":A:",":M:",":S:">  action [":ACTION:"]  old [":oldV:"]   new [":newV:"]"
                     NEXT S
                  NEXT M
               NEXT A
            CASE ACTION = "CARD"
               * keep the 1st 8 digits and scrub the rest
               * keep a copy of the card number in SCRUB.CARDS
               * replace keys and instances of card numbers in all records.
               
         END CASE
      NEXT I
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
INITIALISE:
      FLIST = ""
      SLIST = ""
      CLIST = ""
      CARDS = ""
      CRT "Checking database file:-"
      * CONTROL is a list of files, one file per attribute.      *
      READ CONTROL FROM BP.UPL, "SANITISE-FILES" ELSE CONTROL = ""
      F = 0
      LOOP
         F += 1
         LINE = TRIM(CONTROL<F>)
      UNTIL LINE = "" DO
         ID = LINE<1,1>    ;* File name
         SEL= LINE<1,2>    ;* special select cmd for file
         MAP= LINE<1,3>    ;* special csv name from file
         CRT "  >   ":ID
         DNAME = ID
         PNAME = "REAL.":ID
         OPEN ID TO JUNKIO THEN
            CRT "   Clearing ":ID
            CLEARFILE JUNKIO     ;* clear the recipient file
         END ELSE
            CRT "   Creating ":ID
            EXE = "CREATE.FILE ":ID:" 30"
            EXECUTE EXE CAPTURING OUT
            F -= 1
            CONTINUE
         END
         CLOSE JUNKIO
         Q.PTR = "Q"
         Q.PTR<2> = REAL.ACCT
         Q.PTR<3> = ID
         WRITE Q.PTR ON VOC, PNAME
         OPEN PNAME TO CHKIO ELSE STOP ID:" does not exist in ":REAL.ACCT
         CLOSE CHKIO
         FLIST<F> = ID        ;* File name
         SLIST<F> = SEL       ;* Select - default is empty
         CLIST<F> = MAP       ;* csv name - default is empty
      REPEAT
      CRT "Done checking files"  
      RETURN
      * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - * - *
   END

