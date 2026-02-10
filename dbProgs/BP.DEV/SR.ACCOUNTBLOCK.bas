      SUBROUTINE SR.ACCOUNTBLOCK ( ERR, OLD, NEW )
$INCLUDE I_Prologue
      *
      *  Synopsis:
      *  07-10-20    Do not fail on cdc.account - some accounts do'nt have it
      *              CALLs must return newV, not use the record<a,m,s> !!!
      *              ACCOUNT<29,n> = MBR nbrs so swap old for new then scramble
      *              ACCOUNT<30,n> MUST NOT be scrambled
      *
      * ----------------------------------------------------------------------
      ERR         = ""
      RABACCOUNT = "RAB.ACCOUNT"          ;  DEVACCOUNT = "DEV.ACCOUNT"
      RABCDCACCT = "RAB.CDC.ACCOUNT"      ;  DEVCDCACCT = "DEV.CDC.ACCOUNT"
      RABTRAN    = "RAB.TRAN"             ;  DEVTRAN    = "DEV.TRAN" 
      RABAFFP    = "RAB.AFFP"             ;  DEVAFFP    = "DEV.AFFP"
      * ------------------------------------------------------------
      oldMBR = FIELD(OLD, "S", 1)
      oldMBR = FIELD(oldMBR, "L", 1)
      oldMBR = FIELD(oldMBR, "I", 1)
      newMBR = FIELD(NEW, "S", 1)
      newMBR = FIELD(newMBR, "L", 1)
      newMBR = FIELD(newMBR, "I", 1)
      * ------------------------------------------------------------
      CALL SR.FILE.OPEN (ERR, RABACCOUNT  , RAB.ACCOUNT )
      IF ERR # "" THEN 
         ERR = RABACCOUNT:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, RABCDCACCT  , RAB.CDCACCT )
      IF ERR # "" THEN 
         ERR = RABCDCACCT:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, RABTRAN  , RAB.TRAN )
      IF ERR # "" THEN 
         ERR = RABTRAN:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, RABAFFP  , RAB.AFFP)
      IF ERR # "" THEN 
         ERR = RABAFFP:" open failure"
         GO END..PROG
      END
      * ------------------------------------------------------------
      CALL SR.FILE.OPEN (ERR, DEVACCOUNT  , DEV.ACCOUNT )
      IF ERR # "" THEN 
         ERR = DEVACCOUNT:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, DEVCDCACCT  , DEV.CDCACCT )
      IF ERR # "" THEN 
         ERR = DEVCDCACCT:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, DEVTRAN  , DEV.TRAN )
      IF ERR # "" THEN 
         ERR = DEVTRAN:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, DEVAFFP  , DEV.AFFP)
      IF ERR # "" THEN 
         ERR = DEVAFFP:" open failure"
         GO END..PROG
      END
      * ------------------------------------------------------------
      *
      READ NEWACCT FROM RAB.ACCOUNT, OLD  ELSE NEWACCT = ""
      READ NEWCDC  FROM RAB.CDCACCT, OLD  ELSE NEWCDC  = ""
      READ NEWTRAN FROM RAB.TRAN, OLD     ELSE NEWTRAN = ""
      *
      * ------------------------------------------------------------
      *
      newV = ""; CALL SR.SCRAMBLESTRING( NEWACCT<28,2,1>, newV )    ; NEWACCT<28,2,1> = newV
      newV = ""; CALL SR.SCRAMBLESTRING( NEWACCT<28,4,1>, newV )    ; NEWACCT<28,4,1> = newV
      * -----------------//
      newV = ""; CALL SR.SCRAMBLESTRING( NEWACCT<6>, newV )         ; NEWACCT<6>   = newV
      newV = ""; CALL SR.SCRAMBLESTRING( NEWACCT<17>, newV )        ; NEWACCT<17>  = newV
      newV = ""; CALL SR.SCRUBNUMBER   ( NEWACCT<18>, newV )        ; NEWACCT<18>  = newV
      newV = ""; CALL SR.SCRUBNUMBER   ( NEWACCT<23>, newV )        ; NEWACCT<23>  = newV
      newV = ""; CALL SR.SCRAMBLESTRING( NEWACCT<28,3,1>, newV )    ; NEWACCT<28,3,1> = newV
      newV = ""; CALL SR.SCRAMBLESTRING( NEWACCT<53>, newV )        ; NEWACCT<53>  = newV
      newV = ""; CALL SR.SCRUBSTRING   ( NEWACCT<54>, newV )        ; NEWACCT<54>  = newV
      newV = ""; CALL SR.SCRUBSTRING   ( NEWACCT<55>, newV )        ; NEWACCT<55>  = newV
      newV = ""; CALL SR.SCRAMBLESTRING( NEWACCT<58>, newV )        ; NEWACCT<58>  = newV
      newV = ""; CALL SR.SCRAMBLESTRING( NEWACCT<82>, newV )        ; NEWACCT<82>  = newV
      newV = ""; CALL SR.SCRAMBLESTRING( NEWACCT<155>, newV )       ; NEWACCT<155> = newV
      newV = ""; CALL SR.SCRUBNUMBER   ( NEWACCT<188>, newV )       ; NEWACCT<188> = newV
      newV = ""; CALL SR.SCRUBNUMBER   ( NEWACCT<189>, newV )       ; NEWACCT<189> = newV
      * -----------------//
      filr = STR("@", LEN(oldMBR))
      oldV = EREPLACE(NEWACCT<29>, oldMBR, filr)
      newV = ""; CALL SR.SCRUBNUMBER( oldV, newV )
      newV = EREPLACE(newV, filr, newMBR)
      NEWACCT<29> = newV
      * -----------------
      NEWACCT<35> = ""
      NEWACCT<36> = ""
      WRITE NEWACCT ON DEV.ACCOUNT, NEW
      * ------------------------------------------------------------
      *
      IF NEWCDC # "" THEN
         newV = ""; CALL SR.SCRUBNUMBER   ( NEWCDC<1>, newV )      ; NEWCDC<1> = newV
         newV = ""; CALL SR.SCRUBNUMBER   ( NEWCDC<9>, newV )      ; NEWCDC<9> = newV
         NEWCDC = EREPLACE(NEWCDC, oldMBR, newMBR)
         WRITE NEWCDC ON DEV.CDCACCT, NEW
      END
      * ------------------------------------------------------------
      *
      newV = ""; CALL SR.SCRAMBLESTRING( NEWTRAN<4,2,1>, newV)     ; NEWTRAN<4,2,1>  = newV
      FOR X = 1 TO 16
         NEWTRAN<X> = EREPLACE(NEWTRAN<X>, oldMBR, newMBR)
      NEXT X
      newV = ""; CALL SR.SCRUBNUMBER( NEWTRAN<4,8,1>, newV )       ; NEWTRAN<4,8,1> = newV
      WRITE NEWTRAN ON DEV.TRAN, NEW
      * ------------------------------------------------------------
      *
      TD = INDEX(NEW, "I", 1)    ;* Term Deposits ONLY !!!
      IF TD THEN
         EOX = DCOUNT(NEWTRAN<7>, @VM)
         FOR X = 1 TO EOX
            PID = "P":NEWTRAN<7,X,1>
            IID = "I":NEWTRAN<7,X,1>
            * -----------------//
            READ AFFPREC FROM RAB.AFFP, PID THEN 
               AFFPREC = EREPLACE(AFFPREC, oldMBR, newMBR)
               newV = ""; CALL SR.SCRAMBLESTRING( AFFPREC<5,3,1>, newV )  ; AFFPREC<5,3,1> = newV
               newV = ""; CALL SR.SCRAMBLESTRING( AFFPREC<6>, newV )      ; AFFPREC<6>     = newV
               newV = ""; CALL SR.SCRAMBLESTRING( AFFPREC<8>, newV )      ; AFFPREC<8>     = newV
               newV = ""; CALL SR.SCRAMBLESTRING( AFFPREC<19>, newV )     ; AFFPREC<19>    = newV
               newV = ""; CALL SR.SCRAMBLESTRING( AFFPREC<20>, newV )     ; AFFPREC<20>    = newV
               WRITE AFFPREC ON DEV.AFFP, PID
            END
            READ AFFPREC FROM RAB.AFFP, IID THEN 
               AFFPREC = EREPLACE(AFFPREC, oldMBR, newMBR)
               newV = ""; CALL SR.SCRAMBLESTRING( AFFPREC<5,3,1>, newV )  ; AFFPREC<5,3,1> = newV
               newV = ""; CALL SR.SCRAMBLESTRING( AFFPREC<6>, newV )      ; AFFPREC<6>     = newV
               newV = ""; CALL SR.SCRAMBLESTRING( AFFPREC<8>, newV )      ; AFFPREC<8>     = newV
               newV = ""; CALL SR.SCRAMBLESTRING( AFFPREC<19>, newV )     ; AFFPREC<19>    = newV
               newV = ""; CALL SR.SCRAMBLESTRING( AFFPREC<20>, newV )     ; AFFPREC<20>    = newV
               WRITE AFFPREC ON DEV.AFFP, IID
            END
         NEXT X
      END
      * -----------------//
      * ------------------------------------------------------------
END..PROG:
      IF ERR # "" THEN CRT "SR.ACCOUNTBLOCK   ":ERR
      RETURN
   END


