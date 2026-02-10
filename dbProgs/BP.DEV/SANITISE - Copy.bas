$INCLUDE I_Prologue
      *                                                     
      *  Synopsis:                                          
      *  07-10-20 a) RBI.USER not created !                 
      *           B) DEV.... file must be cleared !!        
      *                                                     
      * ----------------------------------------------------
      EXECUTE "CLEARCOMMON" CAPTURING OUTPUT
      CMD = "WHO"
      EXECUTE CMD CAPTURING OUTPUT
      THISACT = FIELD(OUTPUT, " ", 2)
      LOG.KEY     = "SANITISE":@FM
      THIS.ACCT   = THISACT
      LIVE.ACCT   = "RAB.DEV3"
      ERR         = ""
      NEW.LIST    = ""
      *
      CRT "Preparing Test Data in [":THIS.ACCT:"] ----------------------------"
      OPEN "VOC" TO VOC ELSE STOP "VOC"
      OPEN "&SAVEDLISTS&" TO LIST.CONTROL ELSE STOP "&SAVEDLISTS&"
      OPEN "BP.UPL" TO BP.UPL ELSE STOP "BP.UPL"
      *
      GOSUB CHECK..FILES
      *
      OPEN "CLIENT" TO CLIENT ELSE STOP "CLIENT"
      OPEN "RBI.USER" TO RBI.USER ELSE STOP "RBI.USER"
      *
      READ CONTROL FROM LIST.CONTROL, "RAB.BASE" ELSE
         ERR = "No items in RAB.BASE"
         CRT "*** FATAL *** ":ERR
         GO END..PROG
      END
      CRT " "
      C = 0
      LOOP
         C += 1
         OLD = CONTROL<C>
      UNTIL OLD = "" DO
         NEW = 7; TRY = 1
         LOOP
            FOR CLID = 2 TO 6
               NEW := RND(9)
            NEXT CLID
            LOCATE(NEW, NEW.LIST; FND) ELSE EXIT
            CRT "Retrying on ":OLD
            TRY += 1
            IF TRY > 5 THEN 
               CRT "   Unknown error"
               GO END..PROG
            END
         REPEAT
         CRT "Taking Client ":OLD:" and making ":NEW
         CALL SR.CLIENTBLOCK(ERR, OLD, NEW)
         IF ERR # "" THEN GO END..PROG
         READ CLIREC FROM CLIENT, NEW ELSE CLIREC = ""
         ACCLIST = CLIREC<40>:@VM:CLIREC<57>
         EOX = DCOUNT(ACCLIST, @VM)
         FOR X = 1 TO EOX
            ACC = ACCLIST<1,X>
            IF ACC = "" THEN CONTINUE
            oldACC = OLD:ACC
            newACC = NEW:ACC
            CALL SR.ACCOUNTBLOCK(ERR, oldACC, newACC)
            IF ERR # "" THEN GO END..PROG
            CALL SR.TRANBLOCK(ERR, oldACC, newACC)
            IF ERR # "" THEN GO END..PROG
         NEXT X
         CALL SR.PAYMENTBLOCK(ERR, OLD, NEW)
         IF ERR # "" THEN GO END..PROG
         CALL SR.PAYEEBLOCK(ERR, OLD, NEW)
         IF ERR # "" THEN GO END..PROG
         *
         REC = ""
         REC<1> = NEW
         REC<16> = "1"
         WRITE REC ON RBI.USER, NEW
      REPEAT
END..PROG:
      CRT " "
      CRT "-------------- Done ---------------"
      STOP
CHECK..FILES:
      CRT "Checking database file:-"
      READ FLIST FROM BP.UPL, "SANITISE-FILES" ELSE
         FLIST = "RBI.USER~CLIENT~CLIENT.AML~ACCOUNT~CDC.ACCOUNT~TRAN~TRAN.EXT"
         FLIST:= "~PSEUDO.TRAN~AFFP~DES.DDA~DES.REMITTER~RBI.PAYEE.INDEX"
         FLIST:= "~RBI.PAYEE~INTERNATIONAL.PAYEE.INDEX~INTERNATIONAL.PAYEE"
         FLIST:= "~NPP.PAYMENT.OUT~NPP.PAYMENT.IN~OVERNIGHT.TRAN"
      END
      *
      FLIST = EREPLACE(FLIST, "~", @FM)
      F = 0
      LOOP
         F += 1
         ID = TRIM(FLIST<F>)
      UNTIL ID = "" DO
         CRT "  >   ":ID
         DNAME = "DEV.":ID
         PNAME = "RAB.":ID
         OPEN DNAME TO JUNKIO ELSE
            CRT "   Creating ":DNAME
            EXE = "CREATE.FILE ":DNAME:" 30"
            EXECUTE EXE CAPTURING OUT
            GO CHECK..FILES
         END
         CLEARFILE JUNKIO
         CLOSE JUNKIO
         Q.PTR = "Q"
         Q.PTR<2> = LIVE.ACCT
         Q.PTR<3> = ID
         WRITE Q.PTR ON VOC, PNAME
         Q.PTR<2> = THIS.ACCT
         Q.PTR<3> = DNAME
         WRITE Q.PTR ON VOC, ID
      REPEAT
      CRT "Done with files"
      RETURN
   END

