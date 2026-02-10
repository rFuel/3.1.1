$INCLUDE I_Prologue
      OPEN "BP.UPL" TO BPUPL ELSE STOP "Where is BP.UPL?"     
      READ CSV FROM BPUPL, "http-return-codes.csv" ELSE STOP "http-return-codes.csv not found"
      CALL SR.FILE.OPEN(ERR, "RETURN.CODES", RETURN.CODES)
      IF ERR # "" THEN
         EXECUTE "CREATE-FILE RETURN.CODES DYNAMIC" CAPTURING JUNK
         CALL SR.FILE.OPEN(ERR, "RETURN.CODES", RETURN.CODES)
         IF ERR # "" THEN
            EXECUTE  "CREATE-FILE RETURN.CODES 30 3 4" CAPTURING JUNK
            CALL SR.FILE.OPEN(ERR, "RETURN.CODES", RETURN.CODES)
            IF ERR # "" THEN
               PRINT "uLOAD.RETURN.CODES: Cannot open or create RETURN.CODES"
               STOP
            END
         END
      END
      *
      * CSV = CONVERT(",", @VM, CSV)
      CONVERT "," TO @VM IN CSV
      EOI = DCOUNT(CSV, @FM)
      FOR I = 1 TO EOI
$IFDEF isRT
         WRITE CSV<I,2> ON RETURN.CODES, CSV<I,1> ON ERROR
            PRINT "uLOAD.RETURN.CODES: write failure for [":CSV<I,1>:"] on RETURN.CODES"
         END
$ELSE
         WRITE CSV<I,2> ON RETURN.CODES, CSV<I,1> ELSE
            PRINT "uLOAD.RETURN.CODES: write failure for [":CSV<I,1>:"] on RETURN.CODES"
         END
$ENDIF
      NEXT I
      STOP
   END

