COMMON /uLOGGER/ uSize, showdt, uMax, jlog
$INCLUDE I.SOCKET.COMMON
$INCLUDE I.COMMON
      * note:  uLOGGER named common is shared across ALL UV rFuel installs.
      *        if you change this - watch our for UVMAX - 31 byte name common limit
      * --------------------------------------------------------------------------- *
      * **** CANNOT use I_Prologue - it send this program into an endless-loop **** *
      * --------------------------------------------------------------------------- *
      PROG = "CLEAR.COMMON"
$IFDEF isRT
      EXECUTE "NC.RESET rFuel"   CAPTURING JUNK
      EXECUTE "NC.RESET uSock"   CAPTURING JUNK
      EXECUTE "NC.RESET uLOGGER" CAPTURING JUNK
$ELSE
      * This clears the common but NOT the contents of the variables - GOOD !
      CLEAR COMMON
$ENDIF
      uSize    = 0
      showdt   = 0
      uMax     = 0
      jlog     = 0
      TILDE             = CHAR(126)
      rfENV             = ""
      FNAMES            = ""
      LAST.USED         = ""
      MAT FHANDLES      = ""
      MAT MEMORY.VARS   = ""
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
      READ DBT FROM BP.UPL, "DBT" ELSE DBT = "UV"
      WRITE DBT ON BP.UPL, "DBT"
      MAT sockPROPS = ""
      READ PARAMS FROM BP.UPL, "properties" ELSE PARAMS = ""
      MATPARSE sockPROPS FROM PARAMS
      rfENV = "temp"
      pAns = ""; CALL SR.GET.PROPERTY("upl.logging", pAns) ; UPL.LOGGING = 0 ;*   !!! NEVER !!!
      pAns = ""; CALL SR.GET.PROPERTY("inf.logging", pAns) ; INF.LOGGING = pAns
      pAns = ""; CALL SR.GET.PROPERTY("rfuel.env",   pAns) ; rfENV       = pAns
      pAns = ""
      IF rfENV = "" THEN rfENV = RFUEL.NC
      RELEASE  ;* all read-locks
      STOP
   END
