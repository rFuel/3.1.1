      SUBROUTINE SR.FILE.CLOSE (ERR, INFILE)
$INCLUDE I_Prologue
      
      LOG.KEY = MEMORY.VARS(1):@FM
      ERR = ""
      FPOS= 0
      TRIES=1
      USEFILE = INFILE
      IF USEFILE[1,4] = "upl_" AND DCOUNT(INFILE, "_") > 3 THEN
         CALL SR.GET.PROPERTY("dacct", DACCT)
         IF DACCT # "" THEN
            CONVERT "_" TO @FM IN USEFILE
            EOU = DCOUNT(USEFILE, @FM)
            FOR F = EOU TO 1 STEP -1
               TEMP = USEFILE<F>
               IF TEMP # DACCT THEN USEFILE<F>="" ELSE EXIT
            NEXT F
            NEWFILE = ""
            FOR F = 1 TO EOU
               IF USEFILE<F> = "" THEN CONTINUE
               NEWFILE := "_":USEFILE<F>
            NEXT F
            NEWFILE = NEWFILE[2, LEN(NEWFILE)]
            USEFILE = NEWFILE
            INFILE  = USEFILE
         END
      END
TRY..AGAIN:
      FFND = 0
$IFDEF isRT
      VFILE = "MD"
      LOOKIN  = FNAMES
      LOCATE USEFILE IN LOOKIN SETTING FPOS THEN FFND = 1
$ELSE
      VFILE = "VOC"
      LOOKIN  = FNAMES
      LOCATE(USEFILE, LOOKIN; FPOS) THEN FFND = 1
$ENDIF
      IF FFND THEN
         CLOSE FHANDLES(FPOS)
         LOG.MSG = "SR.FILE.CLOSE closed [":FNAMES<FPOS>:"]"
         IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
         FHANDLES(FPOS)  = ""
         FNAMES<FPOS>    = ""
         LAST.USED<FPOS> = ""
         OPEN VFILE TO VOC THEN 
            READ CHK FROM VOC, INFILE THEN
               IF CHK<1>[1,1] = "Q" AND INFILE[1,4] = "upl_" THEN 
                  DELETE VOC, INFILE
                  IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:INFILE:" removed from VOC")
               END
            END
         END
         CLOSE VOC
      END ELSE
         IF TRIES < 3 THEN
            IF INFILE[1,4] = "upl_" THEN
               usefile = FIELD(INFILE, "_", 2)
               useacct = FIELD(INFILE, "_", 3)
               USEFILE = usefile:"_":useacct
            END ELSE
               IDX = COUNT(USEFILE, "_")
               USEFILE = USEFILE[1,INDEX(USEFILE, "_", IDX)-1]
            END
            TRIES+=1
            GO TRY..AGAIN
         END
         ERR="Cannot find [":INFILE:"] to close it. Also tried reformatting the name to [":USEFILE:"]"
         LOG.MSG = "SR.FILE.CLOSE ":ERR
         IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      END
      
      RETURN
   END


