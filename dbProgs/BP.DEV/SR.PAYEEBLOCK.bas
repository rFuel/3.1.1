      SUBROUTINE SR.PAYEEBLOCK ( ERR, OLD, NEW )
$INCLUDE I_Prologue
      *
      *  Synopsis:                                                            
      *  07-10-20    DEV.PAYEE.INDEX ... needs to be DEV.RBI.PAYEE.INDEX      
      *  07-10-20    a) CALLs must return newV, not use the record<a,m,s> !!! 
      *  07-10-20    b) RBI.PAYEE.INDEX is a client_Id, NOT account_Id        
      *                                                                       
      * ----------------------------------------------------------------------
      ERR         = ""
      * ------------------------------------------------------------
      *
      RABPIDX  = "RAB.RBI.PAYEE.INDEX" ;  DEVPIDX  = "DEV.RBI.PAYEE.INDEX"
      RABPAYEE = "RAB.RBI.PAYEE"       ;  DEVPAYEE = "DEV.RBI.PAYEE"
      RABCLIENT= ""                    ;  DEVCLIENT= "DEV.CLIENT"
      RABIIDX  = "RAB.INTERNATIONAL.PAYEE.INDEX" ;  DEVIIDX  = "DEV.INTERNATIONAL.PAYEE.INDEX"
      RABIPAYEE= "RAB.INTERNATIONAL.PAYEE" ;  DEVIPAYEE= "DEV.INTERNATIONAL.PAYEE"
      * ------------------------------------------------------------
      CALL SR.FILE.OPEN (ERR, RABPIDX  , RAB.PIDX )
      IF ERR # "" THEN 
         ERR = RABPIDX:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, RABPAYEE  , RAB.PAYEE )
      IF ERR # "" THEN 
         ERR = RABPAYEE:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, RABIIDX  , RAB.IIDX )
      IF ERR # "" THEN 
         ERR = RABIIDX:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, RABIPAYEE  , RAB.IPAYEE )
      IF ERR # "" THEN 
         ERR = RABIPAYEE:" open failure"
         GO END..PROG
      END
      * ------------------------------------------------------------
      CALL SR.FILE.OPEN (ERR, DEVCLIENT  , DEV.CLIENT )
      IF ERR # "" THEN 
         ERR = DEVCLIENT:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, DEVPIDX  , DEV.PIDX )
      IF ERR # "" THEN 
         ERR = DEVPIDX:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, DEVPAYEE  , DEV.PAYEE )
      IF ERR # "" THEN 
         ERR = DEVPAYEE:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, DEVIIDX  , DEV.IIDX )
      IF ERR # "" THEN 
         ERR = DEVIIDX:" open failure"
         GO END..PROG
      END
      CALL SR.FILE.OPEN (ERR, DEVIPAYEE  , DEV.IPAYEE )
      IF ERR # "" THEN 
         ERR = DEVIPAYEE:" open failure"
         GO END..PROG
      END
      * ------------------------------------------------------------
      *
      READ CLIENT FROM DEV.CLIENT, NEW ELSE
         ERR = NEW:" not found in ":DEVCLIENT
         GO END..PROG
      END
      ACCLIST = CLIENT<40>
      EOX = DCOUNT(ACCLIST, @VM)
      FOR X = 1 TO EOX
         ACC = ACCLIST<1,X>
         oldID = OLD
         newID = NEW
         * Domestic Payees
         READ RECORD FROM RAB.PIDX, oldID THEN
            WRITE RECORD ON DEV.PIDX, newID
            EOT = DCOUNT(RECORD, @FM)
            FOR T = 1 TO EOT
               KEY = RECORD<T>
               READ OLDREC FROM RAB.PAYEE, KEY THEN
                  PTYPE = OLDREC<1,1,1>
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<2,1,1>, newV ) ; OLDREC<2,1,1>  = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<3,1,1>, newV ) ; OLDREC<3,1,1>  = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<4,1,1>, newV ) ; OLDREC<4,1,1>  = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<6,1,1>, newV ) ; OLDREC<6,1,1>  = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<7,1,1>, newV ) ; OLDREC<7,1,1>  = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING (OLDREC<10,1,1>, newV ) ; OLDREC<10,1,1> = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING (OLDREC<17,1,1>, newV ) ; OLDREC<17,1,1> = newV
                  BEGIN CASE
                     CASE PTYPE=1
                        newV = "" ; CALL SR.SCRUBNUMBER ( OLDREC<6,1,1>, newV ) ; OLDREC<6,1,1> = newV
                     CASE PTYPE=2
                        newV = "" ; CALL SR.SCRUBNUMBER ( OLDREC<5,1,1>, newV ) ; OLDREC<5,1,1> = newV
                        newV = "" ; CALL SR.SCRUBNUMBER ( OLDREC<6,1,1>, newV ) ; OLDREC<6,1,1> = newV
                        newV = "" ; CALL SR.SCRUBNUMBER ( OLDREC<10,1,1>, newV ); OLDREC<10,1,1>= newV
                     CASE PTYPE=3
                        newV = "" ; CALL SR.SCRUBNUMBER ( OLDREC<6,1,1>, newV ) ; OLDREC<6,1,1> = newV
                        newV = "" ; CALL SR.SCRUBNUMBER ( OLDREC<7,1,1>, newV ) ; OLDREC<7,1,1> = newV
                        newV = "" ; CALL SR.SCRUBNUMBER ( OLDREC<10,1,1>, newV ); OLDREC<10,1,1>= newV
                     CASE PTYPE=4
                        newV = "" ; CALL SR.SCRUBNUMBER ( OLDREC<4,1,1>, newV ) ; OLDREC<4,1,1> = newV
                        newV = "" ; CALL SR.SCRUBNUMBER ( OLDREC<6,1,1>, newV ) ; OLDREC<6,1,1> = newV
                        newV = "" ; CALL SR.SCRUBNUMBER ( OLDREC<7,1,1>, newV ) ; OLDREC<7,1,1> = newV
                        newV = "" ; CALL SR.SCRUBNUMBER ( OLDREC<10,1,1>, newV ); OLDREC<10,1,1>= newV
                     CASE PTYPE=9
                        newV = "" ; CALL SR.SCRUBNUMBER    ( OLDREC<16>, newV ) ; OLDREC<16> = newV
                        newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<16>, newV ) ; OLDREC<16> = newV
                        newV = "" ; CALL SR.SCRUBNUMBER    ( OLDREC<18>, newV ) ; OLDREC<18> = newV
                        newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<18>, newV ) ; OLDREC<18> = newV
                        newV = "" ; CALL SR.SCRUBNUMBER    ( OLDREC<19>, newV ) ; OLDREC<19> = newV
                  END CASE
                  OLDREC = EREPLACE(OLDREC, OLD, NEW)
                  WRITE OLDREC ON DEV.PAYEE, KEY
               END
            NEXT T
         END
         * Foreign Payees
         READ RECORD FROM RAB.IIDX, oldID THEN
            WRITE RECORD ON DEV.IIDX, newID
            EOT = DCOUNT(RECORD, @FM)
            FOR T = 1 TO EOT
               KEY = RECORD<T>
               READ OLDREC FROM RAB.IPAYEE, KEY THEN
                  OLDREC = EREPLACE(OLDREC, OLD, NEW)
***               newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<2,1,1>, newV )  ; OLDREC<2,1,1>  = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<3,1,1>, newV )  ; OLDREC<3,1,1>  = newV
                  newV = "" ; CALL SR.SCRUBNUMBER    ( OLDREC<7,1,1>, newV )  ; OLDREC<7,1,1>  = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<11,1,1>, newV ) ; OLDREC<11,1,1> = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<13,1,1>, newV ) ; OLDREC<13,1,1> = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<14,1,1>, newV ) ; OLDREC<14,1,1> = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<15,1,1>, newV ) ; OLDREC<15,1,1> = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<16,1,1>, newV ) ; OLDREC<16,1,1> = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<17,1,1>, newV ) ; OLDREC<17,1,1> = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<18,1,1>, newV ) ; OLDREC<18,1,1> = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<19,1,1>, newV ) ; OLDREC<19,1,1> = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<20,1,1>, newV ) ; OLDREC<20,1,1> = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<21,1,1>, newV ) ; OLDREC<21,1,1> = newV
                  newV = "" ; CALL SR.SCRAMBLESTRING ( OLDREC<26,1,1>, newV ) ; OLDREC<26,1,1> = newV
                  newV = "" ; CALL SR.SCRUBSTRING    ( OLDREC<28,1,1>, newV ) ; OLDREC<28,1,1> = newV
                  newV = "" ; CALL SR.SCRUBSTRING    ( OLDREC<29,1,1>, newV ) ; OLDREC<29,1,1> = newV
                  WRITE OLDREC ON DEV.IPAYEE, KEY
               END
            NEXT T
         END
      NEXT X
      *
      * ------------------------------------------------------------
END..PROG:
      IF ERR # "" THEN CRT "SR.PAYEEBLOCK   ":ERR
      RETURN
   END
