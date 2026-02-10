      * REALITY DB only
      * ------------------------------------------------------------- *
      *        Usage: MONITOR {account} {file} {ON/OFF}                
      * ------------------------------------------------------------- *
      OPEN "REMOTE.CONTROLS" TO CONTROL.DATA ELSE
         CRT
         CRT "Cannot open REMOTE.CONTROLS. Please create and restart this program"
         CRT
         CRT
         STOP
      END
      OPEN "MD" TO VOC ELSE
         CRT
         CRT "Cannot open MD"
         CRT
         STOP
      END
      READ LCAT FROM VOC, "rCDC" ELSE
         CRT "ERROR: cannot access rCDC in the local MD "
         CRT "       no action taken."
         STOP
      END
      *
      ACCT = SENTENCE(1)
      FILE = SENTENCE(2)
      FLAG = SENTENCE(3)
      *
      IF FLAG # "ON" AND FLAG # "OFF" THEN
         CRT 
         CRT " Usage: rMONITOR {accout} {file} {ON / OFF}"
         CRT 
         CRT "ERROR: use 'ON' or 'OFF' to set monitoring on/off"
         CRT "       your setting was ":FLAG
         CRT "       no action taken."
         CRT 
         STOP
      END
      CRT
      QFL = "/":ACCT:"/":FILE
      QMD = "/":ACCT:"/MD"
      OPEN QMD TO RVOC ELSE
         CRT "ERROR: cannot access ":QMD
         CRT "       no action taken."
         STOP
      END
      READ RCAT FROM RVOC, "rCDC" ELSE
         WRITE LCAT ON RVOC, "rCDC" ON ERROR
            CRT "ERROR: cannot write rCDC to ":QMD
            CRT "       no action taken."
            STOP
         END
         CRT "rCDC has now been cataloged in ":ACCT
      END
      *
      *
      CRT
      CRT "Actions tasken:"
      CRT
      CRT "  1. ":QFL:" has passed verification."
      CRT "  2. The pre-write trigger has been ":
      *
      OUTPUT = ""
      IF FLAG = "ON" THEN
         EXECUTE "CREATE-TRIGGER ":QFL:" rCDC WRITE" CAPTURING OUTPUT
         CRT "created."
      END
      *
      IF FLAG = "OFF" THEN
         EXECUTE "DELETE-TRIGGER ":QFL:" WRITE" CAPTURING OUTPUT
         CRT "deleted."
      END
      CRT
      EOO = DCOUNT(OUTPUT, @FM)
      FOR O = 1 TO EOO
         CRT "::> ":OUTPUT<O>
      NEXT O
      *
      CLOSE VOC
      CLOSE RVOC
      CLOSE CONTROL.DATA
      CRT
      STOP
   END
