      *
      * ----------------------------------------------------------------
      * Strip input parameters from @SENTENCE                           
      *                                                                 
      *  Usage:   uANALYSER file=CLIENT  {more params in the future}    
      * ----------------------------------------------------------------
      CMD = CONVERT(" ", @FM, @SENTENCE)
      PROG= "uANALYSER"
      LOCATE(PROG, CMD; POS) ELSE STOP "WRONG program"
      KEYS = ""
      VALS = ""
      EOI = DCOUNT(CMD, @FM)
      FOR I = POS TO EOI
         KVP= CMD<I>
         IF COUNT(KVP, "=") THEN
            KEYS<-1> = UPCASE(FIELD(KVP, "=", 1))
            VALS<-1> = FIELD(KVP, "=", 2)
         END
      NEXT I
      * ----------------------------------------------------------------
      LOCATE("FILE", KEYS; POS) ELSE STOP "No FILE to analyse"
      ERR = "" ; FNAME = VALS<POS>
      * ----------------------------------------------------------------
      CALL SR.FILE.OPEN (ERR, FNAME, FILEIO)
      IF ERR # "" THEN STOP "Cannot open [":FNAME:"]"
      * ----------------------------------------------------------------
      CALL SR.FILE.OPEN (ERR, "uSTRUCTURES", STATIO)
      IF ERR # "" THEN STOP "Cannot open uSTRUCTURES"
      * ----------------------------------------------------------------
      PRINT "Cleaning old results for ":FNAME
      EXE = "SELECT uSTRUCTURES LIKE ":FNAME:"..."
      EXECUTE EXE CAPTURING JUNK
      LOOP
         READNEXT ID ELSE EXIT
         DELETE STATIO, ID
      REPEAT
      * ----------------------------------------------------------------
      PRINT "Producing new results for ":FNAME
      MAX.A = 0; MAX.M = 0; MAX.S = 0
***      PRINT "   Find maximum AV, MV and SV values"
***      SELECT FILEIO
***      LOOP
***         READNEXT ID ELSE EXIT
***         READ REC FROM FILEIO, ID ELSE CONTINUE
***         EOA = DCOUNT(REC, @FM)
***         IF EOA > MAX.A THEN MAX.A = EOA
***         FOR A = 1 TO EOA
***            EOM = DCOUNT(REC<A>, @VM)
***            IF EOM > MAX.M THEN MAX.M = EOM
***            FOR M = 1 TO EOM
***               EOS = DCOUNT(REC<A, M>, @SM)
***               IF EOS > MAX.S THEN MAX.S = EOS
***            NEXT M
***         NEXT A
***      REPEAT
      PRINT "   Analyse data in each position"
      CNT=0; KEYS=""; VALS=""
      SELECT FILEIO
      LOOP
         READNEXT ID ELSE EXIT
         READ REC FROM FILEIO, ID ELSE CONTINUE
         CNT+=1
         IF CNT/1000 = INT(CNT/1000) THEN PRINT CNT
         EOA = DCOUNT(REC, @FM)
         GOSUB A.COUNT
         FOR A = 1 TO EOA              ;* MAX.A
            AVALS = REC<A>
            EOM = DCOUNT(AVALS, @VM)
            GOSUB M.COUNT
            FOR M = 1 TO EOM           ;* MAX.M
               MVALS = REC<A,M>
               EOS = DCOUNT(MVALS, @SM)
               GOSUB S.COUNT
               FOR S = 1 TO EOS        ;* MAX.S
                  DAT = REC<A,M,S>
                  KEYS=""; VALS=""
                  GOSUB WhatIsDAT
                  GOSUB D.TYPE
               NEXT S
            NEXT M
         NEXT A
      REPEAT
      PRINT "Done."
      PRINT "Processed ":CNT:" records. "
      PRINT "See uANALYSER ":FNAME:" for results"
      STOP
      *
      * ------------------------------------------------------
      *
A.COUNT:
      A.KEY = FNAME:"*L*0*"
      READ ATS FROM STATIO, A.KEY ELSE ATS = ""
      LOCATE(EOA, ATS, 1; aPOS) THEN
         ATS<2,aPOS>+=1
      END ELSE
         ATS<1, -1> = EOA
         ATS<2, -1> = 1
      END
      WRITE ATS ON STATIO, A.KEY
      RETURN
M.COUNT:
      M.KEY = FNAME:"*L*":A:"*"
      READ MVS FROM STATIO, M.KEY ELSE MVS = ""
      LOCATE(EOM, MVS, 1; aPOS) THEN
         MVS<2,aPOS>+=1
      END ELSE
         MVS<1, -1> = EOM
         MVS<2, -1> = 1
      END
      WRITE MVS ON STATIO, M.KEY
      RETURN
S.COUNT:
      S.KEY = FNAME:"*L*":A:"*":M:"*"
      READ SVS FROM STATIO, S.KEY ELSE SVS = ""
      LOCATE(EOS, SVS, 1; aPOS) THEN
         SVS<2,aPOS>+=1
      END ELSE
         SVS<1, -1> = EOS
         SVS<2, -1> = 1
      END
      WRITE SVS ON STATIO, S.KEY
      RETURN
D.TYPE:
      S.KEY = FNAME:"*F*":A:"*":M:"*":S:"*"
      READ DTS FROM STATIO, S.KEY ELSE DTS = ""
      EOX = DCOUNT(KEYS, @FM)
      FOR X = 1 TO EOX
         KEY = KEYS<X>
         VAL = VALS<X>
         LOCATE(KEY, DTS, 1; aPOS) THEN
            DTS<2,aPOS> += VAL
         END ELSE
            DTS<1, -1> = KEY
            DTS<2, -1> = VAL
         END
      NEXT X
      WRITE DTS ON STATIO, S.KEY
      RETURN
WhatIsDAT:
      ****************************
      ** DTE   Dates             *
      ** ENC   Encrypted         *
      ** EMT   Empty             *
      ** FFT   Free Form Text    *
      ** INT   Integer           *
      ** TME   Time              *
      ** VAL   Numeric Value     *
      ****************************
      IF DAT = "" THEN 
         LOCATE("EMT", KEYS; aPOS) THEN
            VALS<aPOS>+=1
         END ELSE
            KEYS<-1> = "EMT"
            VALS<-1> = 1
         END
         RETURN
      END
      * ------------------------------------------------------
      IF NUM(DAT) THEN
         IF INT(DAT) = DAT THEN
            LOCATE("INT", KEYS; aPOS) THEN
               VALS<aPOS>+=1
            END ELSE
               KEYS<-1>="INT"
               VALS<-1>=1
            END
         END ELSE
            LOCATE("DEC", KEYS; aPOS) THEN
               VALS<aPOS>+=1
            END ELSE
               KEYS<-1>="DEC"
               VALS<-1>=1
            END
         END
      END ELSE
         LOCATE("FFT", KEYS; aPOS) THEN
            VALS<aPOS>+=1
         END ELSE
            KEYS<-1>="FFT"
            VALS<-1>=1
         END
      END
      RETURN
      * ------------------------------------------------------
   END
