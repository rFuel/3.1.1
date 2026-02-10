      SUBROUTINE SR.READRECORD (ERR, FILE, ITEM, RECORD)
$INCLUDE I_Prologue
      *
      IF FILE[1,5] = "DICT " THEN
         OPEN "DICT", FILE[6, LEN(FILE)] TO IOFILE ELSE ERR = "No Such File"
      END ELSE
         OPEN FILE TO IOFILE ELSE ERR = "No Such File"
      END
      IF ERR # "" THEN GO END..SRTN
      READ RECORD FROM IOFILE, ITEM ELSE RECORD = ""
      DIM CALL.STRINGS(20); MAT CALL.STRINGS = ""
      CALL.STRINGS(2) = RECORD
      CALL SR.ZIP.RECORD (MAT CALL.STRINGS)
      RECORD = CALL.STRINGS(3)
      ERR    = CALL.STRINGS(1)
      * --------------------------------------------------------
END..SRTN:
      CLOSE IOFILE
      RETURN
   END
