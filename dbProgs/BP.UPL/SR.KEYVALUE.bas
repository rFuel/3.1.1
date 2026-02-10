      SUBROUTINE SR.KEYVALUE (PROPS, KEY, VAL)
$INCLUDE I_Prologue
      *
      VAL = ""
      IF KEY = "" THEN RETURN
      *
      EOI = DCOUNT(PROPS, @FM)
      FOR I = 1 TO EOI
         cFIELD = PROPS<I>
         KY = FIELD(cFIELD, "=", 1)
         IF UPCASE(KY) = UPCASE(KEY) THEN VAL = FIELD(cFIELD, "=", 2) ; EXIT
      NEXT I
      RETURN
   END
