      SUBROUTINE SR.PAYMENTBLOCK ( ERR, OLD, NEW )
$INCLUDE I_Prologue
      *
      *  Synopsis:
      *  07-10-20    a) CALLs must return newV, not use the record<a,m,s> !!!
      *              b) Don't scramble<8> - it has the new mbrnbr in it
      *
      * ----------------------------------------------------------------------
      ERR         = ""
      * ------------------------------------------------------------
      *
      RABAFFP  = "RAB.AFFP"         ;  DEVAFFP = "DEV.AFFP"
      RABDDA   = "RAB.DES.DDA"      ;  DEVDDA  = "DEV.DES.DDA"
      RABREM   = "RAB.DES.REMITTER" ;  DEVREM  = "DEV.DES.REMITTER"
      RABCLIENT= ""                 ;  DEVCLIENT="DEV.CLIENT"
      * ------------------------------------------------------------
      CALL SR.FILE.OPEN (ERR, RABAFFP  , RAB.AFFP )
      IF ERR # "" THEN 
         ERR = RABAFFP:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, RABDDA  , RAB.DDA )
      IF ERR # "" THEN 
         ERR = RABDDA:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, RABREM  , RAB.REM )
      IF ERR # "" THEN 
         ERR = RABREM:" open failure"
         GO END..PROG
      END
      * ------------------------------------------------------------
      CALL SR.FILE.OPEN (ERR, DEVCLIENT  , DEV.CLIENT )
      IF ERR # "" THEN 
         ERR = DEVCLIENT:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, DEVAFFP  , DEV.AFFP )
      IF ERR # "" THEN 
         ERR = DEVAFFP:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, DEVDDA  , DEV.DDA )
      IF ERR # "" THEN 
         ERR = DEVDDA:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, DEVREM  , DEV.REM )
      IF ERR # "" THEN 
         ERR = DEVREM:" open failure"
         GO END..PROG
      END
      * ------------------------------------------------------------
      *
      READ CLIENT FROM DEV.CLIENT, NEW ELSE
         ERR = NEW:" not found in ":DEVCLIENT
         GO END..PROG
      END
      FKEYS = CLIENT<26>
      EOX = DCOUNT(FKEYS, @VM)
      FOR X = 1 TO EOX
         FKEY = "F":FKEYS<1,X>
         READ RECORD FROM RAB.AFFP, FKEY THEN
            FTYPE = RECORD<3>
            RECORD = EREPLACE(RECORD, OLD, NEW)       ;* client Ids
            * ----------------------------------------------------------------------------------
            BEGIN CASE
               CASE FTYPE=0
                  newV = "" ; CALL SR.SCRUBNUMBER    (RECORD<8,1,1>, newV) ; RECORD<8,1,1> = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<8,3,1>, newV) ; RECORD<8,3,1> = newV
               CASE FTYPE=1
                  newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<8,3,1>, newV) ; RECORD<8,3,1> = newV
               CASE FTYPE=2
                  newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<8>, newV)     ; RECORD<8>     = newV
               CASE FTYPE=3
                  NULL
               CASE FTYPE=4
                  newV = "" ; CALL SR.SCRUBNUMBER    (RECORD<8,1,1>, newV) ; RECORD<8,1,1> = newV
               CASE FTYPE=5
                  newV = "" ; CALL SR.SCRUBNUMBER    (RECORD<5,1,1>, newV) ; RECORD<5,1,1> = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<8,3,1>, newV) ; RECORD<8,3,1> = newV
               CASE FTYPE=6
                  newV = "" ; CALL SR.SCRUBNUMBER    (RECORD<8,2,1>, newV) ; RECORD<8,2,1> = newV
               CASE FTYPE=7
                  newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<8>, newV)     ; RECORD<8>     = newV
               CASE FTYPE=8
                  NULL
               CASE FTYPE=9
                  newV = "" ; CALL SR.SCRUBNUMBER    (RECORD<8>, newV)     ; RECORD<8>     = newV
               CASE 1
            END CASE
            * ----------------------------------------------------------------------------------
            newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<5,3,1>,newV); RECORD<5,3,1> = newV
            newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<9>, newV)   ; RECORD<9> = newV
            newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<10>, newV)  ; RECORD<10>= newV
            newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<11>, newV)  ; RECORD<11>= newV
            newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<15>, newV)  ; RECORD<15>= newV
            newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<19>, newV)  ; RECORD<19>= newV
            newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<20>, newV)  ; RECORD<20>= newV
            newV = "" ; CALL SR.SCRUBNUMBER    (RECORD<23>, newV)  ; RECORD<23>= newV
            newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<23>, newV)  ; RECORD<23>= newV
            newV = "" ; CALL SR.SCRUBNUMBER    (RECORD<26>, newV)  ; RECORD<26>= newV
            newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<26>, newV)  ; RECORD<26>= newV
            newV = "" ; CALL SR.SCRUBNUMBER    (RECORD<40>, newV)  ; RECORD<40>= newV
            newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<40>, newV)  ; RECORD<40>= newV
            newV = "" ; CALL SR.SCRUBNUMBER    (RECORD<42>, newV)  ; RECORD<42>= newV
            newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<42>, newV)  ; RECORD<42>= newV
            newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<37>, newV)  ; RECORD<37>= newV
            newV = "" ; CALL SR.SCRAMBLESTRING (RECORD<41,3,1>, newV )   ; RECORD<41,3,1>= newV
            WRITE RECORD ON DEV.AFFP, FKEY
         END
      NEXT X
      DKEYS = CLIENT<43>
      EOX = DCOUNT(DKEYS, @VM)
      FOR X = 1 TO EOX
         DKEY = DKEYS<1,X>
         RKEY = DKEY[INDEX(DKEY, "*", 1)+1, LEN(DKEY)]
         NKEY = EREPLACE(DKEY, OLD, NEW)
         READ RECORD FROM RAB.DDA, DKEY THEN
***         newV = "" ; CALL SR.SCRAMBLESTRING ( RECORD<6,1,1>, newV )    ; RECORD<6,1,1> = newV
            newV = "" ; CALL SR.SCRAMBLESTRING ( RECORD<12>, newV )       ; RECORD<12>    = newV
            RECORD = EREPLACE(RECORD, OLD, NEW)
            WRITE RECORD ON DEV.DDA, NKEY
         END
         READ RECORD FROM RAB.REM, RKEY THEN
***         newV = "" ; CALL SR.SCRAMBLESTRING ( RECORD<1,1,1>, newV )    ; RECORD<1,1,1> = newV
            RECORD = EREPLACE(RECORD, OLD, NEW)
            WRITE RECORD ON DEV.REM, RKEY
         END
      NEXT X
      CLIENT<43> = EREPLACE(CLIENT<43>, OLD, NEW)
      WRITE CLIENT ON DEV.CLIENT, NEW
      *
      * ------------------------------------------------------------
END..PROG:
      IF ERR # "" THEN CRT "SR.PAYMENTBLOCK   ":ERR
      RETURN
   END
