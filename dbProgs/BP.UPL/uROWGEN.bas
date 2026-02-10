$INCLUDE I_Prologue
      PROMPT ""
      DBT="UV"
******DBT="UD"
      MAXFILES = 10
      OPEN 'ConfDir' TO IOFILE ELSE STOP "NO access to ConfDir"
      NCC = 'MD0'
10:   *
      PRINT 'WHICH U2 FILE SHALL I POPULATE ':
      INPUT FNAME
      IF FNAME = '' OR FNAME = 'Q' OR FNAME = 'END' THEN STOP
      READ TMPL FROM IOFILE, FNAME:'.csv' ELSE
         PRINT 'CANNOT FIND CSV FILE ':FNAME:'.csv'
         PRINT
         PRINT
         GO 10
      END
      IF FNAME[1,4] = 'DICT' THEN
         DICT=1
         DCT = 'DICT'
         DAT = FNAME[6,999]
      END ELSE
         DICT=0
         DCT = ''
         DAT = FNAME
      END
      TEMP=DAT ; PREFIX='' ; STRING = ',;:!@' ; LX = LEN(STRING) ; I = 1
      LOOP UNTIL I > LX OR PREFIX # '' DO
         IF INDEX(DAT, STRING[I,1], 1) THEN
            DAT = FIELD(TEMP, STRING[I,1], 1)
            PREFIX = FIELD(TEMP, STRING[I,1], 2)
         END
         I+=1
      REPEAT
      FNAME = TRIM(DCT:' ':DAT)
      CSV = FNAME:'.CSV'
12:   *
      OPEN DCT, DAT TO UVFILE ELSE
         PRINT 'CANNOT OPEN U2 FILE ':FNAME
         PRINT 'CREATE IT (Y/N) ': ; INPUT ANS
         IF UPCASE(ANS)[1,1]='Y' THEN
            GOSUB CREATE..FILE
            GO 12
         END
         GO 10
      END
      *
50:   *
      *
      PRINT 'HOW MANY ROWS SHOULD I CREATE ':
      INPUT NBR.ROWS
      IF NOT(NUM(NBR.ROWS)) OR NBR.ROWS='' THEN STOP
      BEGIN CASE
         CASE NBR.ROWS < 1
            PRINT 'ERROR: MUST BE > 1'
            GO 50
         CASE NBR.ROWS > 1000
            PRINT 'WARNING: MAY BLOW UP THE U2DB !!'
      END CASE
      *
60:   *
      *
      PRINT '[R]ECONCILIATION OR [S]AMPLE DATA (R/S) ':
      INPUT RS
      BEGIN CASE
         CASE RS = 'R'
            RECON=1
         CASE RS = 'S'
            RECON=0
         CASE RS = 'Q' OR RS = 'END'
            STOP
         CASE 1
            PRINT ; PRINT '        R OR S ONLY '
            GO 60
      END CASE
      *
70:   *
      *
      IF DICT AND PREFIX='' THEN
         PRINT 'DICT PREFIX (E.G. ADJUST)  ':
         INPUT PREFIX
         IF UPCASE(PREFIX)='Q' THEN STOP
         IF PREFIX='^' THEN GOTO 50
      END
      *
99:   *
      *
      PRINT 'CONTINUE (Y/N) ': ; INPUT ANS
      IF UPCASE(ANS) # 'Y' THEN STOP
      *
      IF NOT(DICT) AND PREFIX='' THEN GOSUB CLEARFILE..UVFILE
      PRINT '... CREATING NEW ROWS'
      EOI = DCOUNT(TMPL, @FM)
      DTE = ICONV('01-01-2000', 'D4')
      TIME.VAR = ICONV('09:00:00', 'MTS')
      TXT = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz'
      LTX = LEN(TXT)
      NBR = '0123456789'
      INFO = INT(NBR.ROWS/10)
      *
      * LOOP TO CREATE RECORDS
      *
      CNT=0
      FOR ROWID = 1 TO NBR.ROWS
         REC = ''
         NBR = RND(99999)
         DVR = RND(1000)
         IF PREFIX#'' THEN USE.ID = PREFIX:ROWID ELSE USE.ID=ROWID
         FOR I = 1 TO EOI
            LINE = TMPL<I>
            LINE = CONVERT(',', @VM, LINE)
            CHK = LINE<1,1>
            IF TRIMF(CHK)[1,1] = '#' THEN CONTINUE
            IF TRIMF(CHK)[1,1] = '[' THEN CONTINUE
            IF TRIMF(CHK)[1,1] = '<' THEN CONTINUE
            IF LINE<1,2> < 0 THEN CONTINUE
            *
            ATR = LINE<1,2>
            MV = LINE<1,3>
            SV = LINE<1,4>
            CNV = UPCASE(LINE<1,5>)
            *
            IF MV = '' OR MV = 0 THEN MV = 1
            IF SV = '' OR SV = 0 THEN SV = 1
            *
            ATR.CNT = ATR
            MV.CNT = MV
            SV.CNT = SV
            USE="A"
            *
            IF INDEX(UPCASE(ATR), 'N', 1) THEN
               USE='A'
               INVAR = ATR
               GOSUB UNPACK..DATUM..DEFINITION
               ATR.CNT = TO.POS
               ATR = FROM.POS
            END
            IF INDEX(UPCASE(MV), 'N', 1) THEN
               USE='M'
               INVAR = MV
               GOSUB UNPACK..DATUM..DEFINITION
               MV.CNT = TO.POS
               MV = FROM.POS
            END
            IF INDEX(UPCASE(SV), 'N', 1) THEN
               USE='S'
               INVAR = SV
               GOSUB UNPACK..DATUM..DEFINITION
               SV.CNT = TO.POS
               SV = FROM.POS
            END
            *
            FOR ATR.IDX = ATR TO ATR.CNT
               FOR MV.IDX = MV TO MV.CNT
                  FOR SV.IDX = SV TO SV.CNT
                     IF CNV[1,1] = '!' THEN
                        IF USE='A' THEN CT=ATR.IDX ELSE IF USE='M' THEN CT=MV.IDX ELSE CT=SV.IDX
                        VAL=(CT:'....') "L#4":'222233334444'
                     END ELSE
                        IF RECON THEN
                           VAL = 'F(':ATR.IDX:'.':MV.IDX:'.':SV.IDX:')'
                        END ELSE
                           GOSUB CONV..DATA
                        END
                     END
                     IF ATR THEN REC<ATR.IDX, MV.IDX, SV.IDX> = VAL
                  NEXT SV.IDX
               NEXT MV.IDX
            NEXT ATR.IDX
            *
         NEXT I
         WRITE REC ON UVFILE, USE.ID
         CNT+=1 ; IF NOT(MOD(CNT,INFO)) THEN PRINT CNT
      NEXT ROWID
      *
      PRINT ; PRINT 'DONE'
      GOTO 10
      *
      * ----------------------------------------------------- *
      *
CREATE..FILE:
      PRINT '[D]YNAMIC OR [P]ART  ':
      INPUT DP
      IF DP # 'D' THEN DP='P'
      IF DP='P' THEN
         IF DBT="UD" THEN 
            PRINT "SORRY - PARTFILES ON UNIDATA ARE NOT READY FOR USE"
            GO CREATE..FILE
         END
INIT..005:
         PRINT 'HOW MANY PARTS ':
         INPUT ANS
         IF NOT(NUM(ANS)) OR ANS='' THEN GO INIT..005
         MAXFILES = ANS
         PASS=0
INIT..010:
         OPEN FNAME TO LOCALDATA ELSE
            IF NOT(PASS) THEN
               CFCMD = 'CREATE-FILE ':FNAME
               DYNAM = ' DYNAMIC' ; DISTF = ''
               ALGOR = 'INTERNAL ':'"':"FIELD(@ID, '|-|', 1); "
               PTHEN = ' THEN ' ; PELSE = ' ELSE '
               PIFAT = 'IF @1<' ; PTHOU = '00000' ; PASS+=1
               FOR FN = 1 TO MAXFILES
                  CMD = CFCMD:FN:DYNAM
                  EXECUTE CMD CAPTURING JUNK
                  DISTF := FNAME:FN:' ':FN:' '
                  ALGOR := PIFAT:FN:PTHOU:PTHEN:FN:PELSE
               NEXT FN
               ALGOR := "'ERROR'"
               EXE = 'DEFINE.DF ':FNAME:' ':DISTF:ALGOR
               EXECUTE EXE CAPTURING JUNK
               PASS+=1
               GO INIT..010
            END ELSE
               PRINT "ERROR: YOU'LL HAVE TO DELETE ":FNAME:' BY HAND ' ; STOP
            END
         END
      END ELSE
         IF DBT="UV" THEN EXECUTE 'CREATE.FILE ':FNAME:' DYNAMIC'
         IF DBT="UD" THEN EXECUTE 'CREATE.FILE ':FNAME:' 5,2 DYNAMIC'
      END
      EXE = 'SELECT ':FNAME ; EXECUTE EXE CAPTURING OUT ; CNT=0
      LOOP
         READNEXT ID ELSE EXIT
         CNT+=1 ; DELETE LOCALDATA, ID
      REPEAT
      CLOSE LOCALDATA
      RETURN
      *
      * ----------------------------------------------------- *
      *
CLEARFILE..UVFILE:
      PRINT 'REMOVING OLD RECORDS ...'
      PRINT '... SELECTING'
      EXE = 'SELECT ':FNAME
      EXECUTE EXE
      PRINT '... DELETING'
      LOOP
         READNEXT ID ELSE EXIT
         DELETE UVFILE, ID
      REPEAT
      RETURN
      *
      * ----------------------------------------------------- *
      *
CONV..DATA:
      CNV = CNV<1,1>
      BEGIN CASE
         CASE CNV[2,1] = 'MT'
            VAL = OCONV(TIME.VAR+DVR, CNV)
         CASE CNV[1,1] = 'M'
            VAL = ICONV(NBR, CNV)
         CASE CNV[1,1] = 'D'
            VAL = DTE+DVR
         CASE UPCASE(CNV)[1,1] = 'F'
            TMP = CONVERT(CNV[2,1], @VM, USE.ID)
            TMP<1,CNV[3,9]> = STR(CNV[3,9], 4)
            USE.ID = CONVERT(@VM, CNV[2,1], TMP)
         CASE CNV[1,1] = '['
            VAL = NBR[RND(6)+1,5]
         CASE CNV = ''
            VAL = ''
            EOK = RND(50)
            IF EOK < 10 THEN EOK = 10
            IF EOK > 50 THEN EOK = 50
            FOR K = 1 TO EOK
               JNK = K/5
               IF INT(JNK) = JNK THEN
                  STR = ' '
               END ELSE
                  STR = TXT[RND(LTX), 1]
               END
               VAL := STR
            NEXT K
         CASE 1
            VAL = 'UNKNOWN CNV (':CNV:') '
            PRINT VAL:' AT CSV LINE # ':I
      END CASE
      RETURN
      *
      * ----------------------------------------------------------------
      *
UNPACK..DATUM..DEFINITION:
      *
      * INVAR SHOULD LOOK LIKE X-N
      *       WHERE X IS A NUMBER !!!
      *
      FROM.POS = FIELD(INVAR, '-', 1)
      IF OCONV(ICONV(FROM.POS,NCC),NCC) # FROM.POS THEN FROM.POS = 1
      TO.POS = RND(5)
      RETURN
   END

