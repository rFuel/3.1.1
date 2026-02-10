      COMMON /uLOGGER/ uSize, showdt, uMax, jlog
$INCLUDE I.SOCKET.COMMON
$INCLUDE I.COMMON
      * --------------------------------------------------------------------------- *
      * **** CANNOT use I_Prologue as it executes this program endless-looping **** *
      * --------------------------------------------------------------------------- *
      PROG = "CLEAR.COMMON"
$IFDEF isRT
      EXECUTE "NC.RESET rFuel"   CAPTURING JUNK
      EXECUTE "NC.RESET uSock"   CAPTURING JUNK
      EXECUTE "NC.RESET uLOGGER" CAPTURING JUNK
$ELSE
      CLEAR COMMON
$ENDIF
      uSize    = 0
      showdt   = 0
      uMax     = 0
      jlog     = 0
      TILDE = CHAR(126)
      FNAMES = ""
      LAST.USED = ""
      MAT FHANDLES = ""
      MAT MEMORY.VARS = ""
      DIM CALL.STRINGS(20) 
      uREQUESTS = ""
      uRESPONSES= ""
      sRESPONSES= ""
      COMO = ""
      RETURN.CODES = ""
      uCATALOG = ""
      UPL.LOGGING = 0
      INF.LOGGING = 0
      OPEN "BP.UPL" TO BP.UPL ELSE STOP
      READ DBT FROM BP.UPL, "DBT" ELSE DBT = "RT"
      WRITE DBT ON BP.UPL, "DBT"
      MAT sockPROPS = ""
      READ PARAMS FROM BP.UPL, "properties" ELSE PARAMS = ""
      MATPARSE sockPROPS FROM PARAMS
      pAns = ""; CALL SR.GET.PROPERTY("upl.logging", pAns) ; UPL.LOGGING = pAns
      pAns = ""; CALL SR.GET.PROPERTY("inf.logging", pAns) ; INF.LOGGING = pAns
      IF PARAMS="" THEN
         PARAMS<1> = UPL.LOGGING
         PARAMS<2> = INF.LOGGING
      END
      WRITE PARAMS ON BP.UPL, "properties"
      STOP
   END
