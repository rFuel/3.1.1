      SUBROUTINE SR.METABASIC (REPLY, REQUEST)
$INCLUDE I_Prologue
      *
      LOGGING.FLAG = INF.LOGGING
      IF INDEX(UPCASE(REQUEST), "{HUSH=TRUE}", 1) > 0 THEN INF.LOGGING=0
      IF MEMORY.VARS(1) = "" THEN MEMORY.VARS(1) = "uHARNESS"
      LOG.KEY = MEMORY.VARS(1) : @FM
      * need to strip last "}" so we don't have an empty mv at the end
      cmd.string = REQUEST[1,LEN(REQUEST)-1]
      *
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"[METABASIC Start] --------------------------------------------- ")
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"Processing: ":REQUEST[1,50])
      *
      cmd.string = EREPLACE(cmd.string, "{", "")
      cmd.string = EREPLACE(cmd.string, "}", @VM)
      cmd.string = EREPLACE(cmd.string, "<bo>", "{")
      cmd.string = EREPLACE(cmd.string, "<bc>", "}")
      cmd = cmd.string<1,1>
      cmd.string = DELETE(cmd.string,1,1,0)
      IF (UPCASE(cmd.string<1,1>[1,5])="CALL=") THEN
         list = cmd.string<1,1>[6, LEN(cmd.string<1,1>)]
         cmd.string<1,1,1> = "CALL"
         cmd.string<1,1,2> = list
      END
      IF (UPCASE(cmd.string<1,2>[1,5])="ARGS=") THEN
         list = cmd.string<1,2>[6, LEN(cmd.string<1,2>)]
         cmd.string<1,2,1> = "ARGS"
         cmd.string<1,2,2> = list
      END
      EOX = "{EOX}"
      REPLY = EOX
      DIM CALL.STRINGS(20)
      MAT CALL.STRINGS = "" 
      CALL.STRINGS(1) = cmd.string
      CALL.STRINGS(2) = LOG.KEY
      SRNAME = "SR.":UPCASE(cmd) ;* e.g. SR.EXE or SR.RDI
$IFDEF isRT
      VOCfile = "MD"
$ELSE
      VOCfile = "VOC"
$ENDIF
      CALL SR.FILE.OPEN (ERR, VOCfile, VOC)
      IF ERR # "" THEN REPLY = ERR; GO END..SUBR
      SUBR = SRNAME
      IF UPCASE(cmd) = "MSVC" THEN
         cmd = cmd.string<1,1>
         cmd.string = DELETE(cmd.string,1,1,0)
         CALL.STRINGS(1) = cmd.string
         SRNAME = UPCASE(cmd)
         IF SRNAME<1,1,1> = "CALL" THEN SRNAME = SRNAME<1,1,2>
         IF DCOUNT(SRNAME, "-") = 3 THEN
            MSTYPE = UPCASE(FIELD(SRNAME, "-", 1))
            MSTHRD = FIELD(SRNAME, "-", 2)
            MSITEM = FIELD(SRNAME, "-", 3)
            BEGIN CASE
               CASE MSTYPE = "SUBR"
                  SRNAME = MSITEM
                  IF UPCASE(CALL.STRINGS(1)<1,1,1>) = "ARGS" THEN
                     CALL.STRINGS(1) = CALL.STRINGS(1)<1,1,2>
                  END
                  READ CHK FROM VOC, SRNAME ELSE
                     REPLY = "Cannot find a catalog for ":SRNAME
                     GO END..SUBR
                 END
                  SUBR = SRNAME
                  GOSUB CALL.SUBR
                  LOG.MSG = "Called ":SUBR:" with ":NBR.ARGS:" args"
                  IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
               CASE MSTYPE = "EXEC"
                  SRNAME = "????"
                  LOG.MSG = "Must build microservice [":MSTYPE:"] for request [":REQUEST:"]"
                  IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
               CASE 1
                  LOG.MSG = "Invalid microservice [":MSTYPE:"] received from request [":REQUEST:"]"
                  IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                  GO END..SUBR
            END CASE
         END ELSE
            LOG.MSG = "Invalid MSVC-instruction [":cmd:"] received from request [":REQUEST:"]"
            IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            GO END..SUBR
         END
      END ELSE
         READ CHK FROM VOC, SRNAME ELSE
            REPLY = "Cannot find a catalog for ":SRNAME
            GO END..SUBR
         END
         IF SUBR = "SR.LBP" THEN CALL.STRINGS(1) = REQUEST
         CALL @SUBR (REPLY, MAT CALL.STRINGS)
      END
      LOG.MSG = "Called: ":SRNAME:" and got back [":REPLY:"]"
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"---------------------------------------------------------------")
END..SUBR:
      INF.LOGGING = LOGGING.FLAG
      RETURN
      *
CALL.SUBR:
      CS = CALL.STRINGS(1)          ; * CS = Call Strings
      CONVERT "<tm>" TO @FM IN CS
      B4 = CS
      DIM CSTR(50)
      MAT CSTR = ""
      MATPARSE CSTR FROM CS
      NBR.ARGS = DCOUNT(CS, @FM)
      BEGIN CASE
         CASE NBR.ARGS = 1
            CALL @SUBR (CSTR(1))
         CASE NBR.ARGS = 2
            CALL @SUBR (CSTR(1), CSTR(2))
         CASE NBR.ARGS = 3
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3))
         CASE NBR.ARGS = 4
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4))
         CASE NBR.ARGS = 5
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4), CSTR(5))
         CASE NBR.ARGS = 6
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4), CSTR(5), CSTR(6))
         CASE NBR.ARGS = 7
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4), CSTR(5), CSTR(6), CSTR(7))
         CASE NBR.ARGS = 8
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4), CSTR(5), CSTR(6), CSTR(7), CSTR(8))
         CASE NBR.ARGS = 9
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4), CSTR(5), CSTR(6), CSTR(7), CSTR(8), CSTR(9))
         CASE NBR.ARGS = 10
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4), CSTR(5), CSTR(6), CSTR(7), CSTR(8), CSTR(9), CSTR(10))
         CASE NBR.ARGS = 11
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4), CSTR(5), CSTR(6), CSTR(7), CSTR(8), CSTR(9), CSTR(10), CSTR(11))
         CASE NBR.ARGS = 12
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4), CSTR(5), CSTR(6), CSTR(7), CSTR(8), CSTR(9), CSTR(10), CSTR(11), CSTR(12))
         CASE NBR.ARGS = 13
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4), CSTR(5), CSTR(6), CSTR(7), CSTR(8), CSTR(9), CSTR(10), CSTR(11), CSTR(12), CSTR(13))
         CASE NBR.ARGS = 14
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4), CSTR(5), CSTR(6), CSTR(7), CSTR(8), CSTR(9), CSTR(10), CSTR(11), CSTR(12), CSTR(13), CSTR(14))
         CASE NBR.ARGS = 15
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4), CSTR(5), CSTR(6), CSTR(7), CSTR(8), CSTR(9), CSTR(10), CSTR(11), CSTR(12), CSTR(13), CSTR(14), CSTR(15))
         CASE NBR.ARGS = 16
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4), CSTR(5), CSTR(6), CSTR(7), CSTR(8), CSTR(9), CSTR(10), CSTR(11), CSTR(12), CSTR(13), CSTR(14), CSTR(15), CSTR(16))
         CASE NBR.ARGS = 17
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4), CSTR(5), CSTR(6), CSTR(7), CSTR(8), CSTR(9), CSTR(10), CSTR(11), CSTR(12), CSTR(13), CSTR(14), CSTR(15), CSTR(16), CSTR(17))
         CASE NBR.ARGS = 18
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4), CSTR(5), CSTR(6), CSTR(7), CSTR(8), CSTR(9), CSTR(10), CSTR(11), CSTR(12), CSTR(13), CSTR(14), CSTR(15), CSTR(16), CSTR(17), CSTR(18))
         CASE NBR.ARGS = 19
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4), CSTR(5), CSTR(6), CSTR(7), CSTR(8), CSTR(9), CSTR(10), CSTR(11), CSTR(12), CSTR(13), CSTR(14), CSTR(15), CSTR(16), CSTR(17), CSTR(18), CSTR(19))
         CASE NBR.ARGS = 20
            CALL @SUBR (CSTR(1), CSTR(2), CSTR(3), CSTR(4), CSTR(5), CSTR(6), CSTR(7), CSTR(8), CSTR(9), CSTR(10), CSTR(11), CSTR(12), CSTR(13), CSTR(14), CSTR(15), CSTR(16), CSTR(17), CSTR(18), CSTR(19), CSTR(20))
         CASE 1
            LOG.MSG = "Invalid number of Call String [":NBR.ARGS:"] received from request [":REQUEST:"]"
            IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            RETURN
      END CASE
      MATBUILD CS FROM CSTR USING @FM
      REPLY = ""
      FOR N = 1 TO NBR.ARGS
         IF B4<N> # CS<N> THEN 
            REPLY := CS<N>
         END ELSE
            REPLY := ""
         END
         IF N < NBR.ARGS THEN REPLY := "<tm>"
      NEXT N
      REPLY = "{Ans=":REPLY:"}"
      RETURN
   END
