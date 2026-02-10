      SUBROUTINE SR.APIDATA(ERR, NXT, COMMAND, VAR)
      *
      * rFuel sends data in key:value pairs e.g. name=NEW-NAME
      * must beware of = in the data portion e.g. field=the.answer.is=3
      *
      IF NUM(NXT) THEN
         * acquire by position - easiest
         NXT += 1
         VAR = COMMAND<NXT>
      END ELSE
         * acquire by reference - useful
         POS = INDEX(COMMAND, NXT:"=", 1)
         TMP = COMMAND[POS, LEN(COMMAND)]
         NXT = DCOUNT(COMMAND[1, POS+1], @FM)     ;* not safe !!
         VAR = TMP<1>
      END
      *
      POS = INDEX(VAR, "=", 1)
      IF POS THEN VAR = VAR[POS+1, LEN(VAR)]
      *
      RETURN
   END
