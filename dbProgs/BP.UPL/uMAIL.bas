      PROMPT ""
      ***************************************************************
      ** Usage: uMAIL -b{body of email} -s{Subject} -t{to-addresses}
      ** AIX tested
      ***************************************************************
      CMD = EREPLACE(@SENTENCE, " -", @FM)
      CMD<1> = EREPLACE(CMD<1>, " ", @FM)
      LOCATE("uMAIL", CMD; FND) ELSE STOP
      FND += 1
      EOC = DCOUNT(CMD, @FM)
      STDHDR = "\r\n\r\nMessage from rFuel\r\n\r\n\r\n"
      STDHDR:= "**************************\r\n"
      STDHDR:= "DO NOT REPLY TO THIS EMAIL\r\n"
      STDHDR:= "**************************\r\n\r\n\r\nMessage:\r\n"
      BODY = ""
      SUBJECT = ""
      TO.ADDYS = ""
      FOR I = FND TO EOC
         PARAM = CMD<I>[1,1]
         VALUE = CMD<I>[2,99999]
         BEGIN CASE
            CASE PARAM = "b"
               BODY = STDHDR:VALUE
            CASE PARAM = "s"
               SUBJECT = VALUE
            CASE PARAM = "t"
               TO.ADDYS = VALUE
         END CASE
      NEXT I
      IF TO.ADDYS="" THEN
         OPEN "UPL.CONTROL" TO UPL.CONTROL ELSE STOP "No UPL.CONTROL"
         READ TO.ADDYS FROM UPL.CONTROL, "NOTICES" ELSE STOP "No recipients"
         TO.ADDYS = EREPLACE(TO.ADDYS, @FM, ",")
      END
      SH.CMD = "sh -c 'echo ":'"':BODY:' $(date)" | mail -s "':SUBJECT:'" ':TO.ADDYS:"'"
      EXECUTE SH.CMD
      STOP
   END

