      SUBROUTINE SR.PREPARE.MSERVICE (RTN.CODE, RTN.MSG, MSCAT, ORDER, SRTNS, PRGMS)
$INCLUDE I_Prologue
      * -------------------------------------------------------------------- *
      * Get the microservice catalog item                                     
      * Returning;                                                            
      *     Order array of executable objects in order of appearance          
      *     Srtns array of subroutines that can be called                     
      *     Prgms array of verbs (programs) that can be executed              
      * -------------------------------------------------------------------- *
      RTN.CODE = ""
      PRECISION 9
      STX = TIME()
      LOG.KEY = MEMORY.VARS(1):@FM
      LOG.MSG = "   SR.PREPARE.MSERVICE Started with [":MSCAT:"]"
      IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
      ERR = 0
      CALL SR.FILE.OPEN(ERR, "uCATALOG", uCATALOG)
      IF ERR # "" THEN
         RTN.CODE = 500
         RTN.MSG = "SR.PREPARE.MSERVICE cannot open uCATALOG"
         RETURN
      END
      READ MSCDETS FROM uCATALOG, MSCAT ELSE
         RTN.CODE = 500
         RTN.MSG = "uHARNESS:: No (U2) Micro-service Catalog item :: [":MSCAT:"]"
         RETURN
      END
      * -------------------------------------------------------------------- *
      * example catalog: MSCDETS                                             *
      *                                                                      *
      * NB:    : commas are OK for defining the subr argument list           *
      *        : subroutines MUST have 20 arguments !!!!!                    *
      *                                                                      *
      * ----------- [ eample 1 ] ------------------------------------------- *
      * 1> subr-t-SR123($message$, $var2$, SPARE3, SPARE4 ... SPARE20)       *
      * 2> subr-t-SR456($message$, $var4$, SPARE3, SPARE4 ... SPARE20)       *
      * 3> subr-t-SR789($message$, $var6$, SPARE3, SPARE4 ... SPARE20)       *
      * 4> subr-w-SRUPD($message$, $var8$, SPARE3, SPARE4 ... SPARE20)       *
      * 5> exec-t-PGM424 CLIENT=$var8$ BRANCH=$var3$                         *
      * 6> ucat-x-cPersonApplicant - link to another micro-service           *
      *                                                                      *
      * ----------- [ eample 2 ] ------------------------------------------- *
      * 1> xeio-x-PersonApplicant                     key to uEIO            *
      * 2> subr-t-SR.IMPORT-PAYLOAD($message$, $var2$, SPARE4 ... SPARE20)   *
      * -------------------------------------------------------------------- *
      *                                                                      *
      * where  : -t- (or nothing) says it is a threadable call.              *
      *        : -w- says wait for dependencies to complete.                 *
      *              So wait for previous calls to ALL return/complete.      *
      *        : subr - CALL SUBROUTINE                                      *
      *        : exec - Execute verb, substituting variables with data       *
      *                                                                      *
      * -------------------------------------------------------------------- *
      * Unpack the microservices :: create the integration pool              *
      * -------------------------------------------------------------------- *
      ORDER = ""
      NBR.OF.ITEMS = DCOUNT(MSCDETS, @FM)
      FOR C = 1 TO NBR.OF.ITEMS
         
         LINE = MSCDETS<C>
         
         MSTYPE = UPCASE(FIELD(LINE, "-", 1))
         IF (MSTYPE # "") THEN LINE = LINE[LEN(MSTYPE)+2, LEN(LINE)]
         
         MSTHRD = UPCASE(FIELD(LINE, "-", 1))
         IF (MSTHRD # "") THEN LINE = LINE[LEN(MSTHRD)+2, LEN(LINE)]
         
         MSITEM = LINE
         
         IF MSTHRD = "T" THEN THREADABLE = 1 ELSE  THREADABLE = 0
         
         BEGIN CASE
            CASE MSTYPE = "SUBR"
               SRNAME = FIELD(MSITEM, "(", 1)                      ;* srtn name
               SRARGS = FIELD(FIELD(MSITEM, "(", 2), ")", 1)       ;* call strings
               SRARGS = TRIM(SRARGS)
               CONVERT "," TO @SM IN SRARGS
               SRTNS<-1> = SRNAME:@VM:SRARGS
               ORDER<-1> = "S":@VM:DCOUNT(SRTNS, @FM):@VM:MSTHRD
               
            CASE MSTYPE = "EXEC"
               VERB   = FIELD(MSITEM, " ", 1)                      ;* prgm name
               KVPS   = MSITEM[LEN(VERB)+1, LEN(MSITEM)]           ;* key-value pairs
               KVPS   = TRIMF(KVPS)
               CONVERT " " TO @SM IN KVPS
               PRGMS<-1> = VERB:@VM:KVPS
               ORDER<-1> = "P":@VM:DCOUNT(PRGMS, @FM):@VM:MSTHRD
               
            CASE MSTYPE = "UCAT"
               * -------------------------------------------------------------- *
               * Linking multiple micro-services
               * To be developed
               * -------------------------------------------------------------- *
               
            CASE MSTYPE = "XEIO"
               * -------------------------------------------------------------- *
               * Linking to the xEIO : the base Enterprise Information Object
               * To be developed
               * -------------------------------------------------------------- *
            CASE 1
               * -------------------------------------------------------------- *
               * Error handling
               * To be developed
               * -------------------------------------------------------------- *
         END CASE
         
      NEXT C
      ETX = TIME()
      DIFF= ETX - STX
      LOG.MSG = "   SR.PREPARE.MSERVICE Finished with [":MSCAT:"] in ":DIFF:" seconds"
      IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
      RETURN
   END

