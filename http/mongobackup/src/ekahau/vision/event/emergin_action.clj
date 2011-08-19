(ns ekahau.vision.event.emergin-action
  (:use
   [clojure.contrib.logging :only [debug error]]
   [ekahau.vision.properties :only [load-emergin-config!]])
  (:import [javax.xml.soap
            MessageFactory
            SOAPConnectionFactory
            SOAPElement
            SOAPMessage]))

;; ## Simple SOAP element hierarchy construction

(defmulti add-child-soap-element
  "Adds a child element to the given parent element."
  (fn [^SOAPElement parent k v] (class v)))

(defn add-child-elements
  "Adds elements described by data to the given parent element."
  [^SOAPElement parent data]
  (doseq [[k v] (partition 2 data)]
    (add-child-soap-element parent k v)))

(defmethod add-child-soap-element clojure.lang.PersistentVector
  [^SOAPElement parent ^String k v]
  (let [element (.addChildElement parent k)]
    (add-child-elements element v)))

(defmethod add-child-soap-element String
  [^SOAPElement parent ^String k v]
  (doto (.addChildElement parent k)
    (.addTextNode v)))

;; ## Emergin SOAP message headers and envelope

(defn add-headers
  "Adds headers to the SOAP message."
  [^SOAPMessage message host]
  (let [headers
        {"Host" host
         "Content-type" "text/xml"
         "SOAPAction" "http://emergin.com/webservices/SOAPToolkit/SubmitMessageSc"
         SOAPMessage/CHARACTER_SET_ENCODING "utf-8"}
        mime-headers (.getMimeHeaders message)]
    (doseq [[k v] headers]
      (.addHeader mime-headers k v))))

(defn add-namespace-declarations
  "Adds namespaces to the SOAP message."
  [^SOAPMessage message]
  (let [namespace-declarations
        {"xsd" "http://www.w3.org/2001/XMLSchema"
         "xsi" "http://www.w3.org/2001/XMLSchema-instance"
         "soap" "http://schemas.xmlsoap.org/soap/envelope/"}
        envelope (.. message getSOAPPart getEnvelope)]
    (doseq [[k v] namespace-declarations]
      (.addNamespaceDeclaration envelope k v))))

(defn create-body-element
  "Creates and returns the body SOAPElement of the message."
  [^SOAPMessage message]
  (.. message
      getSOAPBody
      (addChildElement
       (.createName (.. message getSOAPPart getEnvelope)
                    "SubmitMessageSc"
                    ""
                    "http://emergin.com/webservices/SOAPToolkit"))))

;; ## Vision Emergin message construction

(defn valid-message?
  "Checks that the message is a map and has all required values. "
  [message]
  (and
   (map? message)
   (every?
    (complement nil?)
    (map message [:host :source-id :sensitivity :text]))))

(defn create-message-data
  "Creates Vision Emergin message data structure that can be given to
  `add-child-elements`."
  [{:keys [source-id sensitivity text] :as message}]
  {:pre [(valid-message? message)]}
  ["oConnection"
   ["szApplicationID" ""
    "szUserName" ""
    "szPassword" ""]
   "szMessageID" ""
   "szCommand" "set"
   "szSourceID" source-id
   "szSensitivity" sensitivity
   "szMessage" text
   "szImage" ""])

(defn build-message-body
  "Builds the body of the SOAP message."
  [^SOAPMessage soap-message message]
  (add-child-elements
   (create-body-element soap-message)
   (create-message-data message)))

(defn create-message
  "Creates an Emergin SOAP message."
  [message]
  {:pre [(valid-message? message)]}
  (doto (.createMessage (MessageFactory/newInstance))
    (add-headers (:host message))
    (add-namespace-declarations)
    (build-message-body message)))

(defn str-from-soap-message
  [^SOAPMessage message]
  (let [bytes-out (java.io.ByteArrayOutputStream.)]
    (.writeTo message
              bytes-out)
    (.toString bytes-out "utf-8")))

(defn create-soap-connection
  []
  (.. (SOAPConnectionFactory/newInstance)
      (createConnection)))

(defn create-emergin-end-point-url
  [host service]
  (java.net.URL. (format "http://%s/%s" host service)))

(defn send-soap-message
  [emergin-host
   emergin-service
   ^SOAPMessage message]
  (debug (str "Sending SOAP message: " ))
  (let [connection (create-soap-connection)]
    (try
     (.call connection
            message
            (create-emergin-end-point-url emergin-host emergin-service))
     (catch Exception e
       (error "Failed to send SOAP message" e))
     (finally
      (.close connection)))))

(defn perform-emergin-action
  [m]
  (let [{:keys [host service]} (load-emergin-config!)]
    (when-not host
      (error "Missing Emergin 'host' configuration."))
    (when-not service
      (error "Missing Emergin 'service' configuration."))
    (when (and host service)
      (send-soap-message
       host
       service
       (create-message (assoc m :host host))))))

;; ## Example

(defn create-example-message
  "Creates an example message for testing purposes."
  []
  (str-from-soap-message
   (create-message {:host "localhost"
                    :source-id "some source id"
                    :sensitivity "some sensitivity"
                    :text "some text"})))
