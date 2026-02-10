      * Data push service for CDC events from uDELTA.LOG to rFuel
      * This WORKS with cdcReceiver - an https server / processor.
      * cdcReceiver is based on uDelta2kafka (for kafka batching)
      * Keeping it simple and SECURE - using headers to ensure security (identity)
      *     x-rfuel-api : Enc(purpose) : uv-cdc@{rfuel licence domain}
      *     x-rfuel-key : encSeed      : 
      * cdcReceiver will send data straight into kafka in batch mode.
      * THEREFORE, once an event has been sent successfully, DELETE it !!
      * -------------------------------------------------------------------------
$INCLUDE I_Prologue
      *
      PROGRAM = "uPUSH.SERVER"
      CALL SR.FILE.OPEN(ERR, "uDELTA.LOG", DELTAS)
      IF ERR # "" THEN STOP ERR
      CALL SR.FILE.OPEN(ERR, "UPL.CONTROL", CONTROL)
      IF ERR # "" THEN STOP ERR
      READ CTRL FROM CONTROL, "@PUSH" ELSE STOP "@PUSH is not setup!"
      *
      URL       = CTRL<1>     ;* https://{cdcReceiver IP}:{port}:/api/cdc
      VER       = CTRL<2>     ;* TLSv1
      CRT "Using;"
      CRT "     urL: ":URL
      CRT " version: ":VER
      TEMPLATE  = "$"
      METHOD    = "POST"
      CALL SR.DECRYPT(CTRL<3>, "", USER)
      CALL SR.DECRYPT(CTRL<4>, "", PASSWD)
      DBG = CTRL<10>+0        ;* 1 or 0
      GOSUB CONNECT..RFUEL
      *
      * -------------------------------------------------------------------------
      *
      TM = "<tm>"
      SEND.CTR= 0
      TOUT    = 5000
      LOOP
         SELECT DELTAS
         LOOP
            READNEXT ID ELSE EXIT
            READU EVENT FROM DELTAS, ID LOCKED
               * being handled by another uPUSH.SERVER
               CONTINUE
            END ELSE
               RELEASE DELTAS, ID
               CONTINUE
            END
            MESSAGE = ""
            RESPONSE= ""
            H.STATUS= ""
            PAYLOAD = ""
            CALL SR.ENCRYPT(PAYLOAD, ID:TM:EVENT)
            EVENT    = ""
            DELIM    = PAYLOAD[1,1]
            ELEMS    = EREPLACE(PAYLOAD, DELIM, @FM)
            PAYLOAD  = ELEMS<2>  ;* the encrypted event
            PASSPORT = ELEMS<3>  ;* the encryption seed
            *
            * --------------------------------------------------------------------------
            * Sending 1 event at a time is inefficient !! It may cause performance hits 
            * on the DB.  Need to think of batching with restart / recovery             
            * --------------------------------------------------------------------------
            *
            RTN.CD = setRequestHeader(H.HANDLE, "x-rfuel-api", PROGRAM)    ; IF RTN.CD # 0 THEN STOP
            RTN.CD = setRequestHeader(H.HANDLE, "x-rfuel-key", PASSPORT)   ; IF RTN.CD # 0 THEN STOP
            MESSAGE = EREPLACE(TEMPLATE, "$", PAYLOAD)
            *
            * Instead of sending to cdcReceiver, consider sending to an MQ queue / topic.
            * refer to curl command examples to see how MW receives the requests.        
            *
            RTN.CD = submitRequest(H.HANDLE, TOUT, MESSAGE, HEADERS, RESPONSE, H.STATUS)
            *
            BEGIN CASE
               CASE RTN.CD = 0
                  DELETE DELTAS, ID
                  SEND.CTR += 1
                  IF INT(SEND.CTR/100) = (SEND.CTR / 100) THEN
                     CRT OCONV(SEND.CTR, "MD0,") "R#10":" records sent"
                  END
               CASE RTN.CD = 1
                  CRT "submitRequest issue - invalid request handle. Restart uPUSH.SERVER required."
                  STOP
               CASE RTN.CD = 2
                  CRT "submitRequest issue - HTTP TIMEOUT - ignore and move on."
               CASE RTN.CD = 3
                  CRT "submitRequest issue - network error. Restart uPUSH.SERVER required."
                  STOP
               CASE RTN.CD = 4
                  CRT "submitRequest issue - Unknown problem - type 4. Restart uPUSH.SERVER required."
                  STOP
               CASE 1
                  CRT "submitRequest issue - unknown return code."
                  STOP
            END CASE
            *--------------------------*
            IF DBG THEN
               RELEASE DELTAS, ID
            END ELSE
***            DELETE DELTAS, ID
            END
            *--------------------------*
         REPEAT
         RQM
      REPEAT
      STOP
* ====================================================================================
* ====            Prepare TLS1.x secure connector to cdcReceiver                  ====
* ====================================================================================
CONNECT..RFUEL:
      CTX       = ""
      H.HANDLE  = ""
      BASE64    = ""
      *
      CRT "Creating Base64 authentication for username:password -------------"
      RTN.CD = ENCODE("Base64", 1, USER:":":PASSWD, 1, BASE64, 1)
      OKAY = 0
      BEGIN CASE
        CASE RTN.CD = 0
            CRT "ENCODE has succeeded."
            OKAY = 1
        CASE RTN.CD = 1
            CRT "ENCODE issue - unsupported algorithm."
        CASE RTN.CD = 2
            CRT "ENCODE issue - invalid parameters."
        CASE RTN.CD = 3
            CRT "ENCODE issue - data could not be read."
        CASE RTN.CD = 4
            CRT "ENCODE issue - data could not be encoded."
        CASE 1
            CRT "ENCODE issue - unknown return code."
      END CASE
      IF NOT(OKAY) THEN STOP
      *
      CRT "Creating security context packets --------------------------------"
      RTN.CD    = createSecurityContext(CTX, VER)
      OKAY = 0
      BEGIN CASE
        CASE RTN.CD = 0
            CRT "SecurityContext has succeeded."
            OKAY = 1
        CASE RTN.CD = 1
            CRT "SecurityContext could not be created."
        CASE RTN.CD = 2
            CRT "SecurityContext issue - invalid version."
        CASE 1
            CRT "SecurityContext issue - unknown return code."
      END CASE
      IF NOT(OKAY) THEN STOP
      *
      CRT "Creating secure request handle -----------------------------------"
      RTN.CD = createSecureRequest(URL, METHOD, H.HANDLE, CTX)
      OKAY = 0
      BEGIN CASE
        CASE RTN.CD = 0
            CRT "createSecureRequest has succeeded."
            OKAY = 1
        CASE RTN.CD = 1
            CRT "createSecureRequest issue - invalid URL"
        CASE RTN.CD = 2
            CRT "createSecureRequest issue - invalid method"
        CASE 1
            CRT "createSecureRequest issue - unknown return code."
      END CASE
      IF NOT(OKAY) THEN STOP
      *
      CRT "Setting default headers ] ----------------------------------------"
      STD.HEADERS  = "Authorization: Basic":@VM:BASE64:@FM
      STD.HEADERS := "Accept":@VM:"text/*":@FM
      STD.HEADERS := "Accept-Charset"@VM:"utf-8":@FM
      STD.HEADERS := "Access-Control-Request-Method":@VM:METHOD:@FM
      STD.HEADERS := "Content-Type":@VM:"text/plain":@FM
      STD.HEADERS := "Connection":@VM:"keep-alive":@FM
      *
      RTN.CD = setHTTPDefault("HEADERS", STD.HEADERS)
      OKAY = 0
      BEGIN CASE
        CASE RTN.CD = 0
            CRT "setHTTPDefault has succeeded."
            OKAY = 1
        CASE RTN.CD = 1
            CRT "setHTTPDefault issue - invalid option"
        CASE RTN.CD = 2
            CRT "setHTTPDefault issue - invalid value"
        CASE 1
            CRT "setHTTPDefault issue - unknown return code."
      END CASE
      IF NOT(OKAY) THEN STOP
      RETURN
   END
