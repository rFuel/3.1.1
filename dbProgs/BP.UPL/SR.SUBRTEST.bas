      SUBROUTINE SR.SUBRTEST (RTN.CODE, INVAR1, INVAR2)
$INCLUDE I_Prologue
      ANS = "INVAR1: ":INVAR1:"    INVAR2: ":INVAR2
      RTN.CODE = ""
      INVAR2 = ANS
      INVAR1 = "Subroutine finished successfully"
      RETURN
   END

