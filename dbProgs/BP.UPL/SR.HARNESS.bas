      SUBROUTINE SR.HARNESS (RTN.CODE, subr, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14, V15, V16, V17, V18, V19, V20)
$INCLUDE I_Prologue
      DIM CALL.STRINGS(20) ; MAT CALL.STRINGS = ""
      cSUBR             = subr
      CALL.STRINGS(1)   = V1
      CALL.STRINGS(2)   = V2
      CALL.STRINGS(3)   = V3
      CALL.STRINGS(4)   = V4
      CALL.STRINGS(5)   = V5
      CALL.STRINGS(6)   = V6
      CALL.STRINGS(7)   = V7
      CALL.STRINGS(8)   = V8
      CALL.STRINGS(9)   = V9
      CALL.STRINGS(10)  = V10
      CALL.STRINGS(11)  = V11
      CALL.STRINGS(12)  = V12
      CALL.STRINGS(13)  = V13
      CALL.STRINGS(14)  = V14
      CALL.STRINGS(15)  = V15
      CALL.STRINGS(16)  = V16
      CALL.STRINGS(17)  = V17
      CALL.STRINGS(18)  = V18
      CALL.STRINGS(19)  = V19
      CALL.STRINGS(20)  = V20
      LOG.KEY = MEMORY.VARS(1):@FM
      LOG.MSG = "SR.HARNESS call to ":cSUBR
      CALL uLOGGER(5, LOG.KEY:LOG.MSG) 
      CALL @cSUBR(MAT CALL.STRINGS)
      RTN.CODE = CALL.STRINGS(1)
      LOG.MSG = "SR.HARNESS returned from ":cSUBR:" with return-code: ["RTN.CODE:"]"
      CALL uLOGGER(5, LOG.KEY:LOG.MSG) 
      V1    = '';* this is the subroutine
      V2    = CALL.STRINGS(2)
      V3    = CALL.STRINGS(3)
      V4    = CALL.STRINGS(4)
      V5    = CALL.STRINGS(5)
      V6    = CALL.STRINGS(6)
      V7    = CALL.STRINGS(7)
      V8    = CALL.STRINGS(8)
      V9    = CALL.STRINGS(9)
      V10   = CALL.STRINGS(10)
      V11   = CALL.STRINGS(11)
      V12   = CALL.STRINGS(12)
      V13   = CALL.STRINGS(13)
      V14   = CALL.STRINGS(14)
      V15   = CALL.STRINGS(15)
      V16   = CALL.STRINGS(16)
      V17   = CALL.STRINGS(17)
      V18   = CALL.STRINGS(18)
      V19   = CALL.STRINGS(19)
      V20   = CALL.STRINGS(20)
      RETURN
   END
