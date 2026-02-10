      SUBROUTINE SR.QPOINTER (RTN.CODE, ACCOUNT, remoteFILE, vocNAME, itemID, ANS)
$INCLUDE I_Prologue
      *
      * Example call strings ("", "DATA" ,"CLIENT","Adummy", "11", "")
      * ------------------------------------------------------------------------------
      RTN.CODE = ""
      ANS = "Run-Time error"
      *
$IFDEF isRT
      VFILE = "MD"
$ELSE
      VFILE = "VOC"
$ENDIF
      OPEN VFILE TO VOCFILE ELSE RETURN
      vocREC = "Q":@FM:ACCOUNT:@FM:remoteFILE
$IFDEF isRT
      WRITE vocREC ON VOCFILE, vocNAME ON ERROR RETURN
$ELSE
      WRITE vocREC ON VOCFILE, vocNAME ELSE RETURN
$ENDIF
      *
      OPEN vocNAME TO MY.IOFILE THEN
$IFDEF isRT
         FNAME = vocNAME
$ELSE
         FNAME = FILEINFO(MY.IOFILE, 1)
$ENDIF
         READ REC FROM MY.IOFILE, itemID THEN
            IF RECORDLOCKED(MY.IOFILE, itemID) < 0 THEN
               ANS = "@ID " : itemID : " in " : FNAME : " is currently locked by user ":STATUS()
            END ELSE
               ANS = "Found @ID " : itemID : " in " : FNAME
            END
         END ELSE
            ANS = "@ID " : itemID : " is not in " : FNAME
         END
      END ELSE
         ANS = "Cannot open " : vocNAME
      END
      DELETE VOCFILE, vocNAME
      RETURN
   END
