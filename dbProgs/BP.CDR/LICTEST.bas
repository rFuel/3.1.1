      *
      FNAME = "CLIENT"
      FNAME<-1> = "ACCOUNT"
      FNAME<-1> = "AFFP"
      FNAME<-1> = "BPAY.BILLER.NAME"
      FNAME<-1> = "CLIENT"
      FNAME<-1> = "CLIENT.AML"
      FNAME<-1> = "CDC.ACCOUNT"
      FNAME<-1> = "DELINQUENCY"
      FNAME<-1> = "IC.CONTROL"
      FNAME<-1> = "L_RATES"
      FNAME<-1> = "NPP.PAYMENT.IN"
      FNAME<-1> = "NPP.PAYMENT.OUT"
      FNAME<-1> = "OVERNIGHT.TRAN"
      FNAME<-1> = "PSEUDO.TRAN"
      FNAME<-1> = "RBI.SMSOTP"
      FNAME<-1> = "RBI.SMSOTP.CLIENT"
      FNAME<-1> = "RBI.SMSOTP.USER"
      FNAME<-1> = "RBI.TOKEN"
      FNAME<-1> = "RBI.TOKEN.CLIENT"
      FNAME<-1> = "RBI.TOKEN.USER"
      FNAME<-1> = "RBI.USER"
      FNAME<-1> = "TRAN"
      FNAME<-1> = "TRAN.EXT"
      EOF = DCOUNT(FNAME, @FM)
      DIM FHANDLES(25)
      MAT FHANDLES = ""
      NBR = 0
      FOR I = 1 TO EOF
         IF FNAME<I> = "" THEN CONTINUE
         ERR = ""
         CALL SR.FILE.OPEN (ERR, FNAME<I>, FHANDLES(I))
         IF ERR = "" THEN
            CRT FNAME<I>:" is open."
         END ELSE
            CRT FNAME<I>:" did not open."
         END
        NBR += 1
      NEXT I
      FOR I = 1 TO EOF
         IF FHANDLES(I) # "" THEN CLOSE FHANDLES(I)
      NEXT I
      STOP
   END