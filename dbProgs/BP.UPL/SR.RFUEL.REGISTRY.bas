      SUBROUTINE SR.RFUEL.REGISTRY(ACTION, STATUS, MESSAGE)
      * -------------------------------------------------------- *
      * NOT IN USE at the moment
      *
      * This is run from rFuel (java) when it does a task 910.
      * ------- Run this BEFORE compiling BP.UPL items ---------
      * ACTION:
      *        INSTALL
      *        CHECK
      *        SHOW
      * It is very important to KNOW if other RFUEL accounts
      * are installed on the same UV host. IF there are, then the
      * named common MUST NOT ever be the same !! Corrupts the VOC
      * To get around this, UniLibre MUST control the setup and
      * keep the knowledge itself.
      * -------------------------------------------------------- *
      * registry:
      *  1> UNIQUENESS    *    mv 1-n      e.g. rfuel.01*TEST*/u4/data/RFUEL
      *  2> INSTANCE.ID   *    mv 1-n      e.g. rfuel.01
      *  3> ENV.TIER      *    mv 1-n      e.g. TEST | UAT | PROD
      *  4> APP                mv 1-n      e.g. ultracs
      *  5> VERSION            mv 1-n      e.g. 3.1.1     {S3 bucket}
      *  6> CUSTOMER           mv 1-n      e.g. Kiwibank  {S3 bucket}
      *  7> HOME.DIR      *    mv 1-n      i.e. rfHOME
      *  8> DATE.CREATED       mv 1-n      e.g. 12-11-2025
      *  9> LAST.UPDATE        mv 1-n      e.g. 15-11-2025
      * 10> STATUS             mv 1-n      e.g. Running | Stopped
      * 11> NOTES              mv 1-n      e.g. installed patch for blah blah
      * -------------------------------------------------------- *
      INMSG    = MESSAGE ;* e.g. "Install patch for kerberos"
      IF INMSG = "" THEN INMSG = "Patch loaded on ":OCONV(DATE(), "MD4-")
      STATUS   = ""
      MESSAGE  = ""
      OPEN "BP.UPL" TO BPDATA ELSE
         PRINT "Run the 910 task FIRST !!"
         STOP
      END
      READ REGISTRY  FROM BPDATA, "registry"   ELSE REGISTRY = ""
      READ PARAMS    FROM BPDATA, "properties" ELSE PARAMS = ""
      * -------------------------------------------------------- *
      EOI = DCOUNT(PARAMS, @FM)
      rfENV  = ""
      FOR I = 1 TO EOI
         LINE = PARAMS<I>
         IF UPCASE(FIELD(LINE, "=", 1)) = "RFUEL.ENV" THEN
            rfENV = FIELD(LINE, "=", 2)
            EXIT
         END
      NEXT
      INSTANCE.ID = FIELD(rfENV, "*", 1)
      ENV.TIER    = FIELD(rfENV, "*", 2)
      APP         = FIELD(rfENV, "*", 3)
      VERSION     = FIELD(rfENV, "*", 4)
      CUSTOMER    = FIELD(rfENV, "*", 5)
      IF INSTANCE.ID = "" THEN INSTANCE.ID = "rfuel"
      BEGIN CASE
         CASE ACTION = "INSTALL"
            GOSUB INSTALL
         CASE ACTION = "CHECK"
            GOSUB CHECK
         CASE ACTION = "SHOW"
            GOSUB SHOW
         CASE 1
            MESSAGE = "Invalid action: INSTALL - CHECK - SHOW only"
            STATUS  = "FAIL"
      END CASE
      RETURN
      * -------------------------------------------------------- *
INSTALL:
      CALL !VOC.PATHNAME("DATA", "VOC", rfHOME, STAT)
      IF STAT # 0 THEN
         MESSAGE = "!VOC.PATHNAME failed with status ":STAT
         STATUS  = "FAIL"
         RETURN
      END
      *
      IF REGISTRY = "" THEN
         * The first time we build the register
         REGISTRY<1> = INSTANCE.ID:"*":ENV.TIER:"*":rfHOME
         REGISTRY<2> = INSTANCE.ID
         REGISTRY<3> = ENV.TIER
         REGISTRY<4> = APP
         REGISTRY<5> = VERSION
         REGISTRY<6> = CUSTOMER
         REGISTRY<7> = rfHOME
         REGISTRY<8> = DATE()       ;* Date Created
         REGISTRY<9> = DATE()       ;* Last Modified
         REGISTRY<10> = "Stopped"
         REGISTRY<11>= "Initial install"
      END ELSE
         LOCATE(INSTANCE.ID, REGISTRY, 2, 1; FND) THEN EXISTS=1 ELSE EXISTS=0
         THIS.COPY = INSTANCE.ID:"*":ENV.TIER:"*":rfHOME
         LOCATE(THIS.COPY, REGISTRY, 1, 1; FND) ELSE FND = -1
         *
         IF EXISTS AND FND = -1 THEN
            * Exists    : The instance.id has been used - STOP here        !!
            * FND = -1  : if exists but this.copy is new --> ERROR         !
            * use a different instance.id or named common will blow up     !!
            * Named Common MUST be unique
            MESSAGE = "The instance.id is in use : [":INSTANCE.ID:"] - choose another."
            STATUS = "FAIL"
            RETURN
         END
         *
         REGISTRY<1, FND> = INSTANCE.ID:"*":ENV.TIER:"*":rfHOME
         IF ENV.TIER # REGISTRY<3, FND>   THEN REGISTRY<3, FND> = ENV.TIER
         IF APP      # REGISTRY<4, FND>   THEN REGISTRY<4, FND> = APP
         IF VERSION  # REGISTRY<5, FND>   THEN REGISTRY<5, FND> = VERSION
         IF CUSTOMER # REGISTRY<6, FND>   THEN REGISTRY<6, FND> = CUSTOMER
         IF rfHOME   # REGISTRY<7, FND>   THEN REGISTRY<7, FND> = rfHOME
         REGISTRY<9, FND> = DATE()              ;* last modified
         REGISTRY<11>= INMSG                    ;* modification reason
         WRITE REGISTRY ON BPDATA, "registry"
         RELEASE
         RETURN
      END
      RETURN
      * -------------------------------------------------------------------------------
CHECK:
      RETURN
      * -------------------------------------------------------------------------------
SHOW:
      RETURN
      * -------------------------------------------------------------------------------
   END