      SUBROUTINE SR.TRANBLOCK ( ERR, OLD, NEW )
$INCLUDE I_Prologue
      *
      *  Synopsis:                                                            
      *  07-10-20    Do not fail if the /0 is not on file - may be new or archived
      *              CALLs must return newV, not use the record<a,m,s> !!!    
      *                                                                       
      * ----------------------------------------------------------------------
      oldMBR = FIELD(OLD, "S", 1)
      oldMBR = FIELD(oldMBR, "L", 1)
      oldMBR = FIELD(oldMBR, "I", 1)
      newMBR = FIELD(NEW, "S", 1)
      newMBR = FIELD(newMBR, "L", 1)
      newMBR = FIELD(newMBR, "I", 1)
      *
      SCRUB.CODES = ""
      DBG = 0
      OPEN "BP.UPL" TO BP.UPL THEN
         READ SCRUB.CODES FROM BP.UPL, "CDR-TRAN-TYPES" ELSE SCRUB.CODES = ""
         READV DBG FROM BP.UPL, "CDR-DEBUG", 1 ELSE DBG=0
      END
      IF SCRUB.CODES = "" THEN
         SCRUB.CODES = "25~26~27~32~33~3K~3N~3P~3R~3S~3T~40~41~43~48~50~52~60~6E~6N~6P~6T~6X~71~73"
      END
      SCRUB.CODES = EREPLACE(SCRUB.CODES, "~", @FM)
      *
      ERR        = ""
      RABTRAN    = "RAB.TRAN"          ;  DEVTRAN     = "DEV.TRAN"
      RABTRANEXT = "RAB.TRAN.EXT"      ;  DEVTRANEXT  = "DEV.TRAN.EXT"
      RABPSEUDO  = "RAB.PSEUDO.TRAN"   ;  DEVPSEUDO   = "DEV.PSEUDO.TRAN"
      RABNPPOUT  = "RAB.NPP.PAYMENT.OUT"; DEVNPPOUT   = "DEV.NPP.PAYMENT.OUT"
      RABNPPIN   = "RAB.NPP.PAYMENT.IN" ; DEVNPPIN    = "DEV.NPP.PAYMENT.IN"
      RABPSEUDO  = "RAB.PSEUDO.TRAN"   ;  DEVPSEUDO   = "DEV.PSEUDO.TRAN"
      RABCDC     = ""                  ;  DEVCDC      = "DEV.CDC.ACCOUNT"
      * ------------------------------------------------------------
      CALL SR.FILE.OPEN (ERR, RABTRAN  , RAB.TRAN )
      IF ERR # "" THEN 
         ERR = RABTRAN:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, RABTRANEXT  , RAB.TRANEXT )
      IF ERR # "" THEN 
         ERR = RABTRANEXT:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, RABPSEUDO  , RAB.PSEUDO )
      IF ERR # "" THEN 
         ERR = RABPSEUDO:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, RABNPPOUT  , RAB.NPPOUT )
      IF ERR # "" THEN 
         ERR = RABNPPOUT:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, RABNPPIN  , RAB.NPPIN )
      IF ERR # "" THEN 
         ERR = RABNPPIN:" open failure"
         GO END..PROG
      END
      * ------------------------------------------------------------
      CALL SR.FILE.OPEN (ERR, DEVCDC  , DEV.CDC )
      IF ERR # "" THEN 
         ERR = DEVCDC:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, DEVTRAN  , DEV.TRAN )
      IF ERR # "" THEN 
         ERR = DEVTRAN:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, DEVTRANEXT  , DEV.TRANEXT )
      IF ERR # "" THEN 
         ERR = DEVTRANEXT:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, DEVPSEUDO  , DEV.PSEUDO )
      IF ERR # "" THEN 
         ERR = DEVPSEUDO:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, DEVNPPOUT  , DEV.NPPOUT )
      IF ERR # "" THEN 
         ERR = DEVNPPOUT:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, DEVNPPIN  , DEV.NPPIN )
      IF ERR # "" THEN 
         ERR = DEVNPPIN:" open failure"
         GO END..PROG
      END
      ** --
      FILR = STR("@", LEN(oldMBR))
      ** --
      * ------------------------------------------------------------
      *
      IF DBG THEN CRT "TRAN ":NEW
      READ NEWTRAN FROM DEV.TRAN, NEW THEN
         EOX = DCOUNT(NEWTRAN, @FM)
         FOR X = 17 TO EOX
            TTYP = NEWTRAN<X,1,1>
            LOCATE(TTYP, SCRUB.CODES; FND) THEN
               NARR = NEWTRAN<X,4,1>
               OLDV = NARR
               ** --
               NARR = EREPLACE(NARR, oldMBR, FILR)
               ** --
               newV = ""
               CALL SR.SCRUBNARRATIVE( NARR, newV )
               NARR = newV
               ** --
               NARR = EREPLACE(NARR, FILR, newMBR)
               ** --
               NEWTRAN<X,4,1> = NARR
               IF DBG AND OLDV # NARR THEN
                  CRT "  ":X:"  ":OLDV
                  CRT "  ":X:"  ":NARR
               END
            END
            NEWTRAN = EREPLACE(NEWTRAN, oldMBR, newMBR)
            RECORD = NEWTRAN
            GOSUB DO..NPP
         NEXT X
      END
      WRITE NEWTRAN ON DEV.TRAN, NEW
      IF DBG THEN CRT; CRT
      * ------------------------------------------------------------
      *
      oldID = OLD:"/0"
      newID = NEW:"/0"
      READ INDEX.REC FROM RAB.TRANEXT, oldID THEN
         WRITE INDEX.REC ON DEV.TRANEXT, newID
         *
         EOE = DCOUNT(INDEX.REC, @FM)
         FOR EXT.ID = 1 TO EOE 
            oldID = OLD:"/":EXT.ID
            newID = NEW:"/":EXT.ID
            READ RECORD FROM RAB.TRANEXT, oldID ELSE 
               CRT oldID:" not found in ":RABTRANEXT:"   ... empty record used in ":DEVTRANEXT
               CONTINUE
            END
            IF DBG THEN CRT "TRAN.EXT ":newID
            EOT = DCOUNT(RECORD, @FM)
            FOR T = 1 TO EOT
               TTYP = RECORD<T,1,1>
               LOCATE(TTYP, SCRUB.CODES; FND) THEN
                  NARR = RECORD<T,4,1>
                  OLDV = NARR
                  ** --
                  NARR = EREPLACE(NARR, oldMBR, FILR)
                  ** --
                  newV = ""
                  CALL SR.SCRUBNARRATIVE( NARR, newV )
                  NARR = newV
                  ** --
                  NARR = EREPLACE(NARR, FILR, newMBR)
                  ** --
                  RECORD<T,4,1> = NARR
                  IF DBG AND OLDV # NARR THEN
                     CRT "  ":T:"  ":OLDV
                     CRT "  ":T:"  ":NARR
                  END
               END
            NEXT T
            RECORD = EREPLACE(RECORD, oldMBR, newMBR)
            GOSUB DO..NPP
            WRITE RECORD ON DEV.TRANEXT, newID
         NEXT EXT.ID
      END
      IF DBG THEN CRT; CRT
      * ------------------------------------------------------------
      *
      READ NEWREC FROM DEV.CDC, NEW THEN
         PSLIST = NEWREC<15>
         EOPS = DCOUNT(PSLIST, @VM)
         FOR PS = 1 TO EOPS
            oldPSID = PSLIST<1, PS>
            newPSID = EREPLACE(oldPSID, OLD, NEW)
            READ RECORD FROM RAB.PSEUDO, oldPSID THEN
               EOT = DCOUNT(RECORD, @FM)
               FOR T = 1 TO EOT
                  TTYP = RECORD<T,1,1>
                  LOCATE(TTYP, SCRUB.CODES; FND) THEN
                     NARR = NEWTRAN<X,4,1>
                     ** --
                     NARR = EREPLACE(NARR, oldMBR, FILR)
                     ** --
                     newV = ""
                     CALL SR.SCRUBNARRATIVE( NARR, newV )
                     NARR = newV
                     ** --
                     NARR = EREPLACE(NARR, FILR, newMBR)
                     ** --
                     NEWTRAN<X,4,1> = NARR
                  END
               NEXT T
               RECORD = EREPLACE(RECORD, oldMBR, newMBR)
               GOSUB DO..NPP
               WRITE RECORD ON DEV.PSEUDO, newPSID
            END
         NEXT PS
      END
      *
      * ------------------------------------------------------------
END..PROG:
      IF ERR # "" THEN CRT "SR.TRANBLOCK   ":ERR
      RETURN
      *
      * ------------------------------------------------------------
DO..NPP:
      * ----------- [ Link NPP In / Out Data ] -------------
      EOA = DCOUNT(RECORD, @FM)
      FOR AV = 1 TO EOA
         NPPOUT.KEY = RECORD<AV,31,1>
         NPPIN..KEY = RECORD<AV,31,2>
         READ NPPOUT.REC FROM RAB.NPPOUT, NPPOUT.KEY THEN
            * NPP Out
            NPPOUT.REC = EREPLACE(NPPOUT.REC, oldMBR, newMBR)
            newV="" ; CALL SR.SCRAMBLESTRING (NPPOUT.REC<6,1,1>, newV)  ;  NPPOUT.REC<6,1,1>  = newV
            newV="" ; CALL SR.SCRUBNUMBER    (NPPOUT.REC<7,1,1>, newV)  ;  NPPOUT.REC<7,1,1>  = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPOUT.REC<8,1,1>, newV)  ;  NPPOUT.REC<8,1,1>  = newV
            newV="" ; CALL SR.SCRUBNUMBER    (NPPOUT.REC<19,1,1>, newV) ;  NPPOUT.REC<19,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPOUT.REC<19,1,1>, newV) ;  NPPOUT.REC<19,1,1> = newV
            newV="" ; CALL SR.SCRUBNUMBER    (NPPOUT.REC<20,1,1>, newV) ;  NPPOUT.REC<20,1,1> = newV
            newV="" ; CALL SR.SCRUBNUMBER    (NPPOUT.REC<21,1,1>, newV) ;  NPPOUT.REC<21,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPOUT.REC<22,1,1>, newV) ;  NPPOUT.REC<22,1,1> = newV
            newV="" ; CALL SR.SCRUBNUMBER    (NPPOUT.REC<26,1,1>, newV) ;  NPPOUT.REC<26,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPOUT.REC<26,1,1>, newV) ;  NPPOUT.REC<26,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPOUT.REC<27,1,1>, newV) ;  NPPOUT.REC<27,1,1> = newV
            newV="" ; CALL SR.SCRUBNUMBER    (NPPOUT.REC<30,1,1>, newV) ;  NPPOUT.REC<30,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPOUT.REC<31,1,1>, newV) ;  NPPOUT.REC<31,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPOUT.REC<32,1,1>, newV) ;  NPPOUT.REC<32,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPOUT.REC<33,1,1>, newV) ;  NPPOUT.REC<33,1,1> = newV
            newV="" ; CALL SR.SCRUBNUMBER    (NPPOUT.REC<39,1,1>, newV) ;  NPPOUT.REC<39,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPOUT.REC<39,1,1>, newV) ;  NPPOUT.REC<39,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPOUT.REC<43,1,1>, newV) ;  NPPOUT.REC<43,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPOUT.REC<52,2>, newV)   ;  NPPOUT.REC<52,2>   = newV
            newV="" ; CALL SR.SCRUBNUMBER    (NPPOUT.REC<52,2>, newV)   ;  NPPOUT.REC<52,2>   = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPOUT.REC<64,1,1>, newV) ;  NPPOUT.REC<64,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPOUT.REC<67,1,1>, newV) ;  NPPOUT.REC<67,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPOUT.REC<68,1,1>, newV) ;  NPPOUT.REC<68,1,1> = newV
            newV="" ; CALL SR.SCRUBNUMBER    (NPPOUT.REC<68,1,1>, newV) ;  NPPOUT.REC<68,1,1> = newV
            WRITE NPPOUT.REC ON DEV.NPPOUT, NPPOUT.KEY
         END
         READ NPPIN..REC FROM RAB.NPPIN , NPPIN..KEY THEN
            * NPP In 
            NPPIN..REC = EREPLACE(NPPIN..REC, oldMBR, newMBR)
            newV="" ; CALL SR.SCRAMBLESTRING (NPPIN..REC<6,1,1>, newV)  ;  NPPIN..REC<6,1,1>  = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPIN..REC<13,1,1>, newV) ;  NPPIN..REC<13,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPIN..REC<18,1,1>, newV) ;  NPPIN..REC<18,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPIN..REC<19,1,1>, newV) ;  NPPIN..REC<19,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPIN..REC<20,1,1>, newV) ;  NPPIN..REC<20,1,1> = newV
            newV="" ; CALL SR.SCRUBNUMBER    (NPPIN..REC<23,1,1>, newV) ;  NPPIN..REC<23,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPIN..REC<26,1,1>, newV) ;  NPPIN..REC<26,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPIN..REC<27,1,1>, newV) ;  NPPIN..REC<27,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPIN..REC<28,1,1>, newV) ;  NPPIN..REC<28,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPIN..REC<31,1,1>, newV) ;  NPPIN..REC<31,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPIN..REC<32,1,1>, newV) ;  NPPIN..REC<32,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPIN..REC<33,1,1>, newV) ;  NPPIN..REC<33,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPIN..REC<37,1,1>, newV) ;  NPPIN..REC<37,1,1> = newV
            newV="" ; CALL SR.SCRUBNUMBER    (NPPIN..REC<37,1,1>, newV) ;  NPPIN..REC<37,1,1> = newV
            newV="" ; CALL SR.SCRUBNUMBER    (NPPIN..REC<41,1,1>, newV) ;  NPPIN..REC<41,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPIN..REC<42,1,1>, newV) ;  NPPIN..REC<42,1,1> = newV
            newV="" ; CALL SR.SCRAMBLESTRING (NPPIN..REC<58,1,1>, newV) ;  NPPIN..REC<58,1,1> = newV
            WRITE NPPIN..REC ON DEV.NPPIN, NPPIN..KEY
         END
      NEXT AV
      *
      RETURN
   END


